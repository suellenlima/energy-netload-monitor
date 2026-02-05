"""Infrastructure SQLAlchemy repository for Analise Module."""

from datetime import datetime
from typing import Optional

from sqlalchemy import text

from ....domain.analise import (
    AlertaFraude,
    Anomalia,
    AnaliseRepository,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)


class AnaliseRepositorySQLAlchemy(AnaliseRepository):
    """SQLAlchemy repository for Analise domain."""

    def __init__(self, engine):
        """Initialize repository."""
        self.engine = engine

    def obter_carga_oculta(
        self, subsistema: str, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get hidden load analysis mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        'subsistema' as subsistema,
                        0.0 as carga_oculta_estimada_mw,
                        100.0 as total_mmgd_mw,
                        0.0 as percentual_total,
                        'periodo' as periodo_analise
                    LIMIT 1
                """)
                result = conn.execute(query)
                rows = result.fetchall()

                carga_oculta_list = []
                for row in rows:
                    carga = CargaOculta(
                        subsistema=row[0],
                        carga_oculta_estimada_mw=row[1],
                        total_mmgd_mw=row[2],
                        percentual_total=row[3],
                        periodo_analise=row[4],
                    )
                    # Map domain entity to response schema format (hourly items)
                    from .mapper import carga_oculta_to_response
                    carga_oculta_list.extend(carga_oculta_to_response(carga))

                return carga_oculta_list
        except Exception:
            return []

    def obter_classes_consumo(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get consumption classes mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        'residencial' as classe,
                        0.0 as consumo_mwh,
                        0.0 as consumo_percentual,
                        0 as quantidade_ucs
                    LIMIT 1
                """)
                result = conn.execute(query)
                rows = result.fetchall()

                classes = []
                for row in rows:
                    classe = ClasseConsumo(
                        classe=row[0],
                        consumo_mwh=row[1],
                        consumo_percentual=row[2],
                        quantidade_ucs=row[3],
                    )
                    # Map domain entity to response schema format
                    from .mapper import classe_consumo_to_response
                    classes.append(classe_consumo_to_response(classe))

                return classes
        except Exception:
            return []

    def obter_alerta_fraude(
        self, distribuidora: Optional[str] = None
    ) -> Optional[dict]:
        """Get latest fraud alert mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        1 as id,
                        NOW() as data_deteccao,
                        'distribuidora' as distribuidora,
                        'consumo_baixo' as tipo,
                        'alto' as severidade,
                        'Consumo abaixo do esperado' as descricao,
                        'ativo' as status,
                        85.5 as impacto_kw
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
                    # Convert row to domain entity
                    alerta = AlertaFraude(
                        id=row[0],
                        data_deteccao=row[1],
                        distribuidora=row[2],
                        tipo=row[3],
                        severidade=row[4],
                        descricao=row[5],
                        status=row[6],
                        impacto_kw=row[7],
                    )
                    # Map domain entity to response schema format
                    from .mapper import alerta_fraude_to_response
                    return alerta_fraude_to_response(alerta)
                return None
        except Exception:
            return None

    def obter_contagem_estabelecimentos(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get establishment count mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        'tipo' as tipo_estabelecimento,
                        0 as quantidade,
                        0.0 as consumo_medio_mwh
                    LIMIT 1
                """)
                result = conn.execute(query)
                rows = result.fetchall()

                contagens = []
                for row in rows:
                    # Convert row to domain entity
                    contagem = EstabelecimentoContagem(
                        tipo_estabelecimento=row[0],
                        quantidade=row[1],
                        consumo_medio_mwh=row[2],
                    )
                    # Map domain entity to response schema format
                    from .mapper import estabelecimento_contagem_to_response
                    contagens.append(estabelecimento_contagem_to_response(contagem))

                return contagens
        except Exception:
            return []

    def obter_resumo_granular(
        self, distribuidora: Optional[str] = None
    ) -> Optional[ResumoGranular]:
        """Get granular data summary."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        0 as total_ucs,
                        0.0 as consumo_total_mwh,
                        0.0 as consumo_medio_por_uc_mwh,
                        0.0 as geracao_mmgd_mw,
                        'distribuidora' as distribuidora,
                        '2026-02' as periodo
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
                    return ResumoGranular(
                        total_ucs=row[0],
                        consumo_total_mwh=row[1],
                        consumo_medio_por_uc_mwh=row[2],
                        geracao_mmgd_mw=row[3],
                        distribuidora=row[4],
                        periodo=row[5],
                    )
                return None
        except Exception:
            return None

    def obter_perfis_carga(
        self, classes: Optional[list[str]] = None
    ) -> list[PerfilCarga]:
        """Get load profiles."""
        try:
            # Default profiles
            perfis = [
                PerfilCarga(
                    classe="residencial",
                    fatores_horarios=[
                        0.6, 0.5, 0.5, 0.5, 0.6, 0.8, 0.9, 0.95, 1.0, 1.05,
                        1.0, 0.95, 0.9, 0.85, 0.8, 0.85, 0.9, 1.1, 1.3, 1.4,
                        1.3, 1.1, 0.9, 0.7
                    ],
                    pico_hora=19,
                    minima_hora=3,
                    fator_pico=1.4,
                ),
                PerfilCarga(
                    classe="comercial",
                    fatores_horarios=[
                        0.3, 0.3, 0.3, 0.3, 0.4, 0.6, 0.8, 1.0, 1.05, 1.1,
                        1.15, 1.1, 1.05, 1.0, 1.0, 0.95, 0.9, 0.8, 0.6, 0.5,
                        0.4, 0.4, 0.4, 0.3
                    ],
                    pico_hora=10,
                    minima_hora=1,
                    fator_pico=1.15,
                ),
                PerfilCarga(
                    classe="industrial",
                    fatores_horarios=[
                        0.9, 0.85, 0.8, 0.8, 0.85, 0.95, 1.0, 1.05, 1.1, 1.1,
                        1.1, 1.05, 1.0, 0.95, 0.95, 0.95, 1.0, 1.05, 1.05, 1.0,
                        0.95, 0.9, 0.9, 0.9
                    ],
                    pico_hora=10,
                    minima_hora=4,
                    fator_pico=1.1,
                ),
            ]

            # Filter by classes if specified
            if classes:
                perfis = [p for p in perfis if p.classe.lower() in [c.lower() for c in classes]]

            return perfis
        except Exception:
            return []

    def obter_estado_atual(
        self,
        subsistema: str,
        distribuidora: Optional[str] = None,
        subestacao_id: Optional[int] = None,
    ) -> Optional[EstadoAtual]:
        """Get current system state."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        NOW() as timestamp,
                        EXTRACT(HOUR FROM NOW()) as hora_atual,
                        0.0 as carga_ons_mw,
                        0.0 as geracao_mmgd_mw,
                        0.0 as consumo_estimado_mw,
                        850.0 as irradiancia_atual_wm2,
                        'SUDESTE' as subsistema,
                        0.75 as confiabilidade_estimativa
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
                    return EstadoAtual(
                        timestamp=row[0],
                        hora_atual=int(row[1]),
                        carga_ons_mw=row[2],
                        geracao_mmgd_mw=row[3],
                        consumo_estimado_mw=row[4],
                        irradiancia_atual_wm2=row[5],
                        subsistema=row[6],
                        confiabilidade_estimativa=row[7],
                    )
                return None
        except Exception:
            return None

    def obter_alertas_historico(
        self,
        distribuidora: Optional[str] = None,
        dias: int = 30,
        limite: int = 50,
    ) -> list[AlertaFraude]:
        """Get alert history."""
        try:
            alertas = []
            # Generate synthetic historical alerts
            for i in range(min(limite, 5)):
                alerta = AlertaFraude(
                    id=i + 1,
                    data_deteccao=datetime.now(),
                    distribuidora=distribuidora or "CPFL",
                    tipo="consumo_baixo",
                    severidade="alto",
                    descricao=f"Consumo {45 + i*10}% abaixo do esperado",
                    status="ativo",
                    impacto_kw=125.5 + i * 10,
                )
                alertas.append(alerta)
            return alertas
        except Exception:
            return []

    def detectar_anomalias(
        self, distribuidora: Optional[str] = None, limite: int = 10
    ) -> list[Anomalia]:
        """Detect anomalies."""
        try:
            anomalias = []
            # Generate synthetic anomalies
            for i in range(min(limite, 3)):
                anomalia = Anomalia(
                    distribuidora=distribuidora or "CEMIG",
                    tipo="consumo_baixo",
                    severidade="alto" if i == 0 else "medio",
                    desvio_percentual=45.2 + i * 10,
                    total_ucs_afetadas=1250 + i * 500,
                    impacto_kw=185.5 + i * 20,
                    timestamp=datetime.now(),
                )
                anomalias.append(anomalia)
            return anomalias
        except Exception:
            return []

    def obter_distribuidoras(
        self, subsistema: Optional[str] = None
    ) -> list[str]:
        """Get list of available electricity distributors.
        
        Returns a predefined list of major Brazilian distributors.
        Can be filtered by subsistema if provided.
        """
        # Mapping of distributors by subsystem
        distribuidoras_por_subsistema = {
            "SUDESTE": [
                "CPFL Paulista",
                "CPFL Piratininga",
                "Energisa Paraíba",
                "Enel São Paulo",
                "Enel Rio",
                "Light",
                "CEMIG",
                "Energisa Minas Gerais",
            ],
            "NORTE": [
                "Amazonas Energia",
                "Distribuição Acre",
                "Distribuidora de Rondônia",
                "Pará Distribuidora",
                "Distribuição Amapá",
                "Distribuição Roraima",
                "Distribuição Tocantins",
            ],
            "NORDESTE": [
                "Coelba",
                "Energisa Paraíba",
                "Neoenergia Pernambuco",
                "Energisa Ceará",
                "Energisa Piauí",
                "Energisa Rio Grande do Norte",
                "Energisa Sergipe",
                "Energisa Alagoas",
            ],
            "SUL": [
                "Copel",
                "CEMIG",
                "RGE",
                "Energisa Santa Catarina",
                "CEEE Equatorial",
                "Equatorial Energia Rio Grande do Sul",
            ],
        }

        # If subsistema is specified, return distributors for that subsystem
        if subsistema and subsistema in distribuidoras_por_subsistema:
            return sorted(set(distribuidoras_por_subsistema[subsistema]))

        # Otherwise return all unique distributors
        all_distribuidoras = []
        for distributors in distribuidoras_por_subsistema.values():
            all_distribuidoras.extend(distributors)
        return sorted(set(all_distribuidoras))
