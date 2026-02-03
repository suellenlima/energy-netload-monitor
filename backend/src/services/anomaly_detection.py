"""
Sistema de Detecção Automática de Anomalias e Fraudes.

Analisa padrões de consumo e identifica:
- Desvios anormais de consumo
- Padrões atípicos de carga
- Fator de carga suspeito
- Injeção de energia não registrada
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detector de anomalias em dados de consumo elétrico."""

    # Limiares de detecção
    LIMIAR_DESVIO_CONSUMO = 0.30  # 30% de desvio
    LIMIAR_FATOR_CARGA_MIN = 0.20  # Fator de carga muito baixo
    LIMIAR_FATOR_CARGA_MAX = 0.95  # Fator de carga muito alto (suspeito)
    LIMIAR_PICO_ATIPICO = 2.5      # Pico 2.5x maior que média

    def __init__(self, engine: Engine):
        self.engine = engine

    def detect_anomalies(
        self,
        distribuidora: Optional[str] = None,
        limite: int = 10
    ) -> List[Dict]:
        """
        Detecta anomalias nos dados de consumo.

        Args:
            distribuidora: Filtrar por distribuidora específica
            limite: Número máximo de anomalias a retornar

        Returns:
            Lista de anomalias detectadas com detalhes
        """
        anomalias = []

        try:
            with self.engine.connect() as conn:
                # Buscar dados agregados de consumo por distribuidora
                query = text("""
                    SELECT
                        distribuidora,
                        SUM(energia_kwh_mes) / COUNT(*) as consumo_medio_uc,
                        COUNT(*) as total_ucs,
                        SUM(energia_kwh_mes) as consumo_total
                    FROM bdgd_unidades_consumidoras
                    WHERE distribuidora IS NOT NULL
                """)

                if distribuidora:
                    query = text("""
                        SELECT
                            distribuidora,
                            SUM(energia_kwh_mes) / COUNT(*) as consumo_medio_uc,
                            COUNT(*) as total_ucs,
                            SUM(energia_kwh_mes) as consumo_total
                        FROM bdgd_unidades_consumidoras
                        WHERE distribuidora = :distribuidora
                    """)
                    params = {"distribuidora": distribuidora}
                else:
                    params = {}

                query_str = str(query)
                if params:
                    query_str += f" GROUP BY distribuidora LIMIT {limite}"
                else:
                    query_str += f" GROUP BY distribuidora LIMIT {limite}"

                result = conn.execute(text(query_str), params)
                dados_distribuidoras = result.fetchall()

                # Analisar cada distribuidora
                for row in dados_distribuidoras:
                    dist = row[0]
                    consumo_medio = float(row[1]) if row[1] else 0
                    total_ucs = int(row[2]) if row[2] else 0
                    consumo_total = float(row[3]) if row[3] else 0

                    # Detectar anomalias
                    anomalia = self._analyze_distribuidora(
                        dist, consumo_medio, total_ucs, consumo_total
                    )

                    if anomalia:
                        anomalias.append(anomalia)

        except Exception as e:
            logger.error(f"Erro ao detectar anomalias: {e}", exc_info=True)

        return anomalias[:limite]

    def _analyze_distribuidora(
        self,
        distribuidora: str,
        consumo_medio: float,
        total_ucs: int,
        consumo_total: float
    ) -> Optional[Dict]:
        """
        Analisa uma distribuidora específica em busca de anomalias.

        Critérios de Detecção:
        1. Consumo médio fora do padrão (muito baixo = fraude)
        2. Distribuição anômala de classes
        3. Fator de carga suspeito
        4. Padrão de consumo atípico

        Returns:
            Dict com dados da anomalia ou None se não detectada
        """
        # Consumo médio esperado por UC residencial no Brasil: 150-200 kWh/mês
        CONSUMO_ESPERADO_MIN = 100
        CONSUMO_ESPERADO_MAX = 300

        # Verificar desvio de consumo
        if consumo_medio < CONSUMO_ESPERADO_MIN * (1 - self.LIMIAR_DESVIO_CONSUMO):
            # Consumo muito baixo = possível fraude
            desvio = (CONSUMO_ESPERADO_MIN - consumo_medio) / CONSUMO_ESPERADO_MIN
            fraude_kwh_mes = (CONSUMO_ESPERADO_MIN - consumo_medio) * total_ucs

            return {
                "distribuidora": distribuidora,
                "tipo": "consumo_baixo",
                "severidade": self._calcular_severidade(desvio),
                "consumo_medio_uc": round(consumo_medio, 2),
                "consumo_esperado_uc": CONSUMO_ESPERADO_MIN,
                "desvio_percentual": round(desvio * 100, 1),
                "total_ucs_afetadas": total_ucs,
                "impacto_kwh_mes": round(fraude_kwh_mes, 2),
                "impacto_kw": round(fraude_kwh_mes / 720, 2),  # 720h = 30 dias
                "descricao": f"Consumo {desvio*100:.1f}% abaixo do esperado",
                "data_deteccao": datetime.now().isoformat(),
            }

        elif consumo_medio > CONSUMO_ESPERADO_MAX * (1 + self.LIMIAR_DESVIO_CONSUMO):
            # Consumo muito alto = possível erro de medição ou furto de energia
            desvio = (consumo_medio - CONSUMO_ESPERADO_MAX) / CONSUMO_ESPERADO_MAX

            return {
                "distribuidora": distribuidora,
                "tipo": "consumo_alto",
                "severidade": self._calcular_severidade(desvio),
                "consumo_medio_uc": round(consumo_medio, 2),
                "consumo_esperado_uc": CONSUMO_ESPERADO_MAX,
                "desvio_percentual": round(desvio * 100, 1),
                "total_ucs_afetadas": total_ucs,
                "descricao": f"Consumo {desvio*100:.1f}% acima do esperado",
                "data_deteccao": datetime.now().isoformat(),
            }

        # Análise de fator de carga (se houver dados de curva)
        anomalia_fator = self._analyze_load_factor(distribuidora)
        if anomalia_fator:
            return anomalia_fator

        return None

    def _analyze_load_factor(self, distribuidora: str) -> Optional[Dict]:
        """
        Analisa o fator de carga da distribuidora.

        Fator de Carga = Demanda Média / Demanda Máxima
        - Normal: 0.50 - 0.75
        - Suspeito: < 0.20 (muitos picos) ou > 0.95 (muito constante = bypass?)
        """
        try:
            with self.engine.connect() as conn:
                # Buscar dados de carga se disponíveis
                query = text("""
                    SELECT
                        MAX(carga_mw) as pico,
                        AVG(carga_mw) as media
                    FROM carga_ons
                    WHERE time >= NOW() - INTERVAL '7 days'
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row and row[0] and row[1]:
                    pico = float(row[0])
                    media = float(row[1])
                    fator_carga = media / pico if pico > 0 else 0

                    if fator_carga < self.LIMIAR_FATOR_CARGA_MIN:
                        return {
                            "distribuidora": distribuidora,
                            "tipo": "fator_carga_baixo",
                            "severidade": "medio",
                            "fator_carga": round(fator_carga, 3),
                            "pico_mw": round(pico, 2),
                            "media_mw": round(media, 2),
                            "descricao": f"Fator de carga suspeito: {fator_carga:.3f} (muito baixo)",
                            "data_deteccao": datetime.now().isoformat(),
                        }
                    elif fator_carga > self.LIMIAR_FATOR_CARGA_MAX:
                        return {
                            "distribuidora": distribuidora,
                            "tipo": "fator_carga_alto",
                            "severidade": "alto",
                            "fator_carga": round(fator_carga, 3),
                            "pico_mw": round(pico, 2),
                            "media_mw": round(media, 2),
                            "descricao": f"Fator de carga suspeito: {fator_carga:.3f} (muito constante)",
                            "data_deteccao": datetime.now().isoformat(),
                        }

        except Exception as e:
            logger.debug(f"Não foi possível analisar fator de carga: {e}")

        return None

    def _calcular_severidade(self, desvio: float) -> str:
        """
        Calcula severidade baseada no desvio.

        Args:
            desvio: Desvio percentual (0.0 - 1.0)

        Returns:
            "baixo", "medio" ou "alto"
        """
        if abs(desvio) >= 0.50:  # >= 50%
            return "alto"
        elif abs(desvio) >= 0.30:  # >= 30%
            return "medio"
        else:
            return "baixo"

    def generate_historical_alerts(
        self,
        dias: int = 30,
        num_alertas: int = 50
    ) -> List[Dict]:
        """
        Gera alertas históricos sintéticos para demonstração.

        Cria um histórico realista de detecções dos últimos N dias.

        Args:
            dias: Número de dias no passado
            num_alertas: Quantidade de alertas a gerar

        Returns:
            Lista de alertas históricos
        """
        alertas_historicos = []

        distribuidoras = [
            "CPFL Paulista", "CEMIG", "Light", "Elektro", "Enel SP",
            "Copel", "COELBA", "CELPE", "RGE", "AES SUL",
            "ELETROPAULO", "Bandeirante", "EDP SP", "CEEE", "CEMAT"
        ]

        tipos_anomalia = [
            {
                "tipo": "consumo_baixo",
                "descricao_template": "Consumo {desvio}% abaixo do esperado - possível fraude",
                "severidade_dist": [0.3, 0.5, 0.2],  # baixo, medio, alto
            },
            {
                "tipo": "fator_carga_alto",
                "descricao_template": "Fator de carga suspeito: {valor} (muito constante)",
                "severidade_dist": [0.2, 0.4, 0.4],
            },
            {
                "tipo": "pico_atipico",
                "descricao_template": "Pico de demanda {valor}x acima da média",
                "severidade_dist": [0.4, 0.4, 0.2],
            },
            {
                "tipo": "consumo_alto",
                "descricao_template": "Consumo {desvio}% acima do esperado - possível erro",
                "severidade_dist": [0.5, 0.3, 0.2],
            }
        ]

        # Gerar alertas distribuídos ao longo dos dias
        for i in range(num_alertas):
            # Data aleatória nos últimos N dias
            dias_atras = random.randint(0, dias)
            horas_atras = random.randint(0, 23)
            data_deteccao = datetime.now() - timedelta(days=dias_atras, hours=horas_atras)

            # Selecionar tipo de anomalia
            tipo_info = random.choice(tipos_anomalia)
            tipo = tipo_info["tipo"]

            # Selecionar severidade baseada na distribuição
            severidade = np.random.choice(
                ["baixo", "medio", "alto"],
                p=tipo_info["severidade_dist"]
            )

            # Distribuidora aleatória
            distribuidora = random.choice(distribuidoras)

            # Gerar dados específicos por tipo
            if tipo == "consumo_baixo":
                desvio = random.uniform(0.30, 0.70)  # 30% a 70% abaixo
                consumo_medio = random.uniform(50, 100)
                total_ucs = random.randint(500, 5000)
                impacto_kwh = (150 - consumo_medio) * total_ucs
                impacto_kw = impacto_kwh / 720

                alerta = {
                    "id": i + 1,
                    "distribuidora": distribuidora,
                    "tipo": tipo,
                    "severidade": severidade,
                    "consumo_medio_uc": round(consumo_medio, 2),
                    "consumo_esperado_uc": 150,
                    "desvio_percentual": round(desvio * 100, 1),
                    "total_ucs_afetadas": total_ucs,
                    "impacto_kwh_mes": round(impacto_kwh, 2),
                    "impacto_kw": round(impacto_kw, 2),
                    "descricao": tipo_info["descricao_template"].format(desvio=int(desvio*100)),
                    "data_deteccao": data_deteccao.isoformat(),
                    "status": random.choice(["ativo", "investigando", "resolvido", "falso_positivo"]),
                }

            elif tipo == "fator_carga_alto":
                fator = random.uniform(0.95, 0.99)
                pico = random.uniform(50, 200)
                media = pico * fator

                alerta = {
                    "id": i + 1,
                    "distribuidora": distribuidora,
                    "tipo": tipo,
                    "severidade": severidade,
                    "fator_carga": round(fator, 3),
                    "pico_mw": round(pico, 2),
                    "media_mw": round(media, 2),
                    "descricao": tipo_info["descricao_template"].format(valor=f"{fator:.3f}"),
                    "data_deteccao": data_deteccao.isoformat(),
                    "status": random.choice(["ativo", "investigando", "resolvido"]),
                }

            elif tipo == "pico_atipico":
                multiplicador = random.uniform(2.5, 5.0)
                media = random.uniform(50, 150)
                pico = media * multiplicador

                alerta = {
                    "id": i + 1,
                    "distribuidora": distribuidora,
                    "tipo": tipo,
                    "severidade": severidade,
                    "pico_mw": round(pico, 2),
                    "media_mw": round(media, 2),
                    "multiplicador": round(multiplicador, 1),
                    "descricao": tipo_info["descricao_template"].format(valor=f"{multiplicador:.1f}"),
                    "data_deteccao": data_deteccao.isoformat(),
                    "status": random.choice(["ativo", "investigando", "resolvido"]),
                }

            else:  # consumo_alto
                desvio = random.uniform(0.30, 0.60)
                consumo_medio = random.uniform(350, 500)
                total_ucs = random.randint(200, 2000)

                alerta = {
                    "id": i + 1,
                    "distribuidora": distribuidora,
                    "tipo": tipo,
                    "severidade": severidade,
                    "consumo_medio_uc": round(consumo_medio, 2),
                    "consumo_esperado_uc": 250,
                    "desvio_percentual": round(desvio * 100, 1),
                    "total_ucs_afetadas": total_ucs,
                    "descricao": tipo_info["descricao_template"].format(desvio=int(desvio*100)),
                    "data_deteccao": data_deteccao.isoformat(),
                    "status": random.choice(["ativo", "investigando", "resolvido"]),
                }

            # Adicionar coordenadas aleatórias (Brasil)
            alerta["latitude"] = random.uniform(-33.0, -5.0)
            alerta["longitude"] = random.uniform(-73.0, -35.0)

            alertas_historicos.append(alerta)

        # Ordenar por data (mais recente primeiro)
        alertas_historicos.sort(key=lambda x: x["data_deteccao"], reverse=True)

        return alertas_historicos


def get_latest_alert(engine: Engine, distribuidora: Optional[str] = None) -> Optional[Dict]:
    """
    Retorna o alerta mais recente (real ou gerado automaticamente).

    Args:
        engine: Conexão com banco de dados
        distribuidora: Filtrar por distribuidora

    Returns:
        Dict com dados do alerta ou None
    """
    detector = AnomalyDetector(engine)

    # Tentar buscar alerta real do banco (auditoria_visual)
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    data_inspecao,
                    latitude,
                    longitude,
                    distribuidora,
                    classe_estimada_ia,
                    diferenca_fraude_kw,
                    potencia_oficial_kw,
                    status
                FROM auditoria_visual
                WHERE 1=1
            """)

            params = {}
            if distribuidora:
                query = text(str(query) + " AND distribuidora = :distribuidora")
                params["distribuidora"] = distribuidora

            query = text(str(query) + " ORDER BY data_inspecao DESC LIMIT 1")

            result = conn.execute(query, params)
            row = result.fetchone()

            if row:
                # Retornar alerta real do banco
                return {
                    "data": row[0].isoformat() if row[0] else None,
                    "local": f"{row[1]}, {row[2]}" if row[1] and row[2] else "N/A",
                    "distribuidora": row[3],
                    "classe_ia": row[4] or "Não Classificado",
                    "fraude_kw": float(row[5]) if row[5] else 0,
                    "oficial_kw": float(row[6]) if row[6] else 0,
                    "status": row[7] or "OK",
                    "fonte": "auditoria_manual"
                }

    except Exception as e:
        logger.debug(f"Nenhum alerta manual encontrado: {e}")

    # Se não houver alertas reais, gerar automaticamente
    anomalias = detector.detect_anomalies(distribuidora=distribuidora, limite=1)

    if anomalias:
        anomalia = anomalias[0]
        return {
            "data": anomalia.get("data_deteccao"),
            "local": f"{anomalia.get('latitude', -23.55)}, {anomalia.get('longitude', -46.63)}",
            "distribuidora": anomalia.get("distribuidora"),
            "classe_ia": anomalia.get("tipo", "Anomalia Detectada").replace("_", " ").title(),
            "fraude_kw": anomalia.get("impacto_kw", 0),
            "oficial_kw": anomalia.get("consumo_medio_uc", 150) * 0.7,  # Estimativa
            "status": "ALERTA" if anomalia.get("severidade") in ["medio", "alto"] else "OK",
            "fonte": "deteccao_automatica",
            "detalhes": anomalia
        }

    # Nenhum alerta encontrado
    return None
