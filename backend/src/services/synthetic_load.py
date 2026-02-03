"""
Serviço de Cálculo de Carga Sintética por Subestação.

Combina mix de consumidores com perfis típicos para gerar curva horária local.
"""

import logging
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.database import get_engine
from ..data import load_profiles

logger = logging.getLogger(__name__)


def calculate_synthetic_load_by_subestacao(
    engine: Engine,
    subestacao_id: int,
    consumo_medio_por_classe: Optional[Dict[str, float]] = None
) -> Dict[str, any]:
    """
    Calcula curva de carga sintética horária para uma subestação.

    Formula:
        Carga_hora(h) = Σ (Qtd_UCs_classe × Consumo_medio_UC_classe × Perfil_classe(h))

    Args:
        engine: Engine do SQLAlchemy
        subestacao_id: ID da subestação
        consumo_medio_por_classe: Dict opcional com consumo médio por classe (kWh/mês)
                                   Se None, usa valores padrão da ANEEL

    Returns:
        Dict com curva horária e metadados
    """
    logger.info(f"Calculando carga sintética para subestação {subestacao_id}")

    # Consumo médio padrão por classe (kWh/mês) - ANEEL 2023
    if consumo_medio_por_classe is None:
        consumo_medio_por_classe = {
            "Residencial": 150.0,      # 150 kWh/mês
            "Comercial": 800.0,        # 800 kWh/mês
            "Industrial": 15000.0,     # 15 MWh/mês
            "Rural": 300.0,            # 300 kWh/mês
            "Poder Público": 2000.0    # 2 MWh/mês
        }

    # Buscar mix de consumidores da subestação
    query = text("""
        SELECT
            classe_consumo,
            SUM(qtd_unidades) as qtd_unidades_consumidoras,
            SUM(potencia_kw) / 1000.0 as potencia_total_mw
        FROM gd_granular
        WHERE subestacao_id = :subestacao_id
        GROUP BY classe_consumo
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"subestacao_id": subestacao_id})
            rows = result.fetchall()

        if not rows:
            logger.warning(f"Nenhuma UC associada à subestação {subestacao_id}")
            return {
                "subestacao_id": subestacao_id,
                "curva_horaria_kw": [0.0] * 24,
                "curva_horaria_mw": [0.0] * 24,
                "erro": "Sem UCs associadas"
            }

        # Inicializar curva horária (24 horas)
        curva_total_kw = [0.0] * 24

        # Metadados por classe
        contribuicao_por_classe = {}

        for row in rows:
            classe = row.classe_consumo
            qtd_ucs = int(row.qtd_unidades_consumidoras or 0)

            if qtd_ucs == 0:
                continue

            # Buscar consumo médio para a classe
            consumo_medio_kwh_mes = consumo_medio_por_classe.get(classe, 150.0)

            # Consumo médio mensal → potência média horária
            # kWh/mês / 720h (30 dias × 24h) = kW médio por UC
            potencia_media_por_uc_kw = consumo_medio_kwh_mes / 720

            # Buscar perfil típico da classe
            try:
                perfil = load_profiles.get_profile(classe.lower().replace(" ", "_"))
            except KeyError:
                logger.warning(f"Perfil não encontrado para classe '{classe}', usando residencial")
                perfil = load_profiles.get_profile("residencial")

            # Calcular curva horária para esta classe
            curva_classe_kw = [
                qtd_ucs * potencia_media_por_uc_kw * fator
                for fator in perfil
            ]

            # Acumular na curva total
            curva_total_kw = [
                curva_total_kw[h] + curva_classe_kw[h]
                for h in range(24)
            ]

            # Guardar contribuição da classe
            contribuicao_por_classe[classe] = {
                "qtd_ucs": qtd_ucs,
                "consumo_medio_kwh_mes": consumo_medio_kwh_mes,
                "potencia_media_uc_kw": round(potencia_media_por_uc_kw, 3),
                "curva_horaria_kw": [round(v, 2) for v in curva_classe_kw],
                "pico_kw": round(max(curva_classe_kw), 2),
                "media_kw": round(sum(curva_classe_kw) / 24, 2)
            }

        # Converter para MW
        curva_total_mw = [kw / 1000 for kw in curva_total_kw]

        # Calcular estatísticas
        pico_kw = max(curva_total_kw)
        hora_pico = curva_total_kw.index(pico_kw)
        vale_kw = min(curva_total_kw)
        hora_vale = curva_total_kw.index(vale_kw)
        media_kw = sum(curva_total_kw) / 24

        resultado = {
            "subestacao_id": subestacao_id,
            "curva_horaria_kw": [round(v, 2) for v in curva_total_kw],
            "curva_horaria_mw": [round(v, 3) for v in curva_total_mw],
            "estatisticas": {
                "pico_kw": round(pico_kw, 2),
                "hora_pico": hora_pico,
                "vale_kw": round(vale_kw, 2),
                "hora_vale": hora_vale,
                "media_kw": round(media_kw, 2),
                "amplitude_kw": round(pico_kw - vale_kw, 2),
                "fator_carga": round(media_kw / pico_kw, 3) if pico_kw > 0 else 0
            },
            "contribuicao_por_classe": contribuicao_por_classe,
            "total_ucs": sum(c["qtd_ucs"] for c in contribuicao_por_classe.values())
        }

        logger.info(
            f"Carga sintética calculada: pico {pico_kw:.1f} kW às {hora_pico}h, "
            f"média {media_kw:.1f} kW ({len(contribuicao_por_classe)} classes)"
        )

        return resultado

    except Exception as exc:
        logger.error(f"Erro ao calcular carga sintética: {exc}", exc_info=True)
        return {
            "subestacao_id": subestacao_id,
            "erro": str(exc)
        }


def calculate_load_at_hour(
    engine: Engine,
    subestacao_id: int,
    hora: int,
    consumo_medio_por_classe: Optional[Dict[str, float]] = None
) -> Dict[str, any]:
    """
    Calcula carga estimada para uma hora específica.

    Args:
        engine: Engine do SQLAlchemy
        subestacao_id: ID da subestação
        hora: Hora do dia (0-23)
        consumo_medio_por_classe: Dict opcional com consumo por classe

    Returns:
        Dict com carga estimada e detalhes
    """
    if not 0 <= hora <= 23:
        raise ValueError(f"Hora deve estar entre 0 e 23, recebido: {hora}")

    # Calcular curva completa
    resultado = calculate_synthetic_load_by_subestacao(
        engine,
        subestacao_id,
        consumo_medio_por_classe
    )

    if "erro" in resultado:
        return resultado

    # Extrair valores para a hora específica
    carga_kw = resultado["curva_horaria_kw"][hora]
    carga_mw = resultado["curva_horaria_mw"][hora]

    # Contribuição por classe naquela hora
    contrib_hora = {}
    for classe, dados in resultado["contribuicao_por_classe"].items():
        contrib_hora[classe] = {
            "carga_kw": dados["curva_horaria_kw"][hora],
            "qtd_ucs": dados["qtd_ucs"],
            "percentual": round(
                (dados["curva_horaria_kw"][hora] / carga_kw * 100) if carga_kw > 0 else 0,
                2
            )
        }

    return {
        "subestacao_id": subestacao_id,
        "hora": hora,
        "carga_kw": round(carga_kw, 2),
        "carga_mw": round(carga_mw, 3),
        "contribuicao_por_classe": contrib_hora,
        "total_ucs": resultado["total_ucs"]
    }


def compare_with_ons_load(
    engine: Engine,
    subestacao_id: int,
    subsistema: str = "SUDESTE"
) -> Dict[str, any]:
    """
    Compara carga sintética da subestação com carga agregada do ONS.

    Args:
        engine: Engine do SQLAlchemy
        subestacao_id: ID da subestação
        subsistema: Subsistema elétrico

    Returns:
        Dict com comparação
    """
    logger.info(f"Comparando carga sintética com ONS para subestação {subestacao_id}")

    # Calcular sintética
    sintetica = calculate_synthetic_load_by_subestacao(engine, subestacao_id)

    if "erro" in sintetica:
        return sintetica

    # Buscar carga ONS (última disponível)
    query_ons = text("""
        SELECT
            time,
            carga_mw,
            EXTRACT(HOUR FROM time) as hora
        FROM carga_ons
        WHERE UPPER(subsistema) LIKE :subsistema
        ORDER BY time DESC
        LIMIT 24
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query_ons, {"subsistema": f"%{subsistema.upper()}%"})
            rows = result.fetchall()

        if not rows:
            logger.warning(f"Sem dados ONS para {subsistema}")
            return {
                **sintetica,
                "ons": None,
                "comparacao": "Sem dados ONS disponíveis"
            }

        # Organizar dados ONS por hora
        ons_por_hora = {int(row.hora): float(row.carga_mw) for row in rows}

        # Calcular erro médio
        erros = []
        for h in range(24):
            if h in ons_por_hora:
                sintetica_mw = sintetica["curva_horaria_mw"][h]
                ons_mw = ons_por_hora[h]
                erro_percentual = abs(sintetica_mw - ons_mw) / ons_mw * 100 if ons_mw > 0 else 0
                erros.append(erro_percentual)

        erro_medio = sum(erros) / len(erros) if erros else 0

        return {
            **sintetica,
            "comparacao_ons": {
                "erro_medio_percentual": round(erro_medio, 2),
                "subsistema": subsistema,
                "nota": "Comparação com carga agregada regional (não é 1:1 com subestação)"
            }
        }

    except Exception as exc:
        logger.error(f"Erro ao comparar com ONS: {exc}", exc_info=True)
        return {
            **sintetica,
            "comparacao_ons": {"erro": str(exc)}
        }


if __name__ == "__main__":
    # Teste
    logging.basicConfig(level=logging.INFO)

    engine = get_engine()

    print("\n=== Teste de Carga Sintética ===")
    resultado = calculate_synthetic_load_by_subestacao(engine, subestacao_id=1)

    if "erro" not in resultado:
        print(f"\nSubestação: {resultado['subestacao_id']}")
        print(f"Total UCs: {resultado['total_ucs']}")
        print(f"Pico: {resultado['estatisticas']['pico_kw']:.1f} kW às {resultado['estatisticas']['hora_pico']}h")
        print(f"Média: {resultado['estatisticas']['media_kw']:.1f} kW")
        print(f"Fator de carga: {resultado['estatisticas']['fator_carga']:.2f}")
    else:
        print(f"Erro: {resultado['erro']}")
