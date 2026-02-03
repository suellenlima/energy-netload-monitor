"""
Serviço de calibração de perfis de carga usando dados da BDGD.

Este módulo combina perfis típicos com consumo real da BDGD para gerar
curvas horárias mais precisas por região.
"""

import logging
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.database import get_engine
from ..data import load_profiles

logger = logging.getLogger(__name__)


def get_bdgd_consumo_medio(
    engine: Engine,
    distribuidora: Optional[str] = None,
    municipio: Optional[str] = None,
    classe_consumo: Optional[str] = None
) -> Dict[str, float]:
    """
    Busca consumo médio por classe da BDGD.

    Args:
        engine: Engine do SQLAlchemy
        distribuidora: Filtrar por distribuidora
        municipio: Filtrar por município
        classe_consumo: Filtrar por classe específica

    Returns:
        Dict com {classe: consumo_medio_kwh_mes}
    """
    logger.info("Buscando consumo médio da BDGD")

    # Construir query dinamicamente
    where_clauses = []
    params = {}

    if distribuidora:
        where_clauses.append("distribuidora = :distribuidora")
        params["distribuidora"] = distribuidora

    if municipio:
        where_clauses.append("municipio = :municipio")
        params["municipio"] = municipio

    if classe_consumo:
        where_clauses.append("classe_consumo = :classe_consumo")
        params["classe_consumo"] = classe_consumo

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = text(f"""
        SELECT
            classe_consumo,
            COUNT(*) as qtd_ucs,
            AVG(energia_kwh_mes) as consumo_medio_kwh,
            STDDEV(energia_kwh_mes) as desvio_padrao
        FROM bdgd_unidades_consumidoras
        {where_sql}
        GROUP BY classe_consumo
        ORDER BY qtd_ucs DESC
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, params)
            consumo_por_classe = {}

            for row in result:
                classe = row.classe_consumo
                consumo_medio = float(row.consumo_medio_kwh or 0)
                qtd_ucs = int(row.qtd_ucs or 0)

                if consumo_medio > 0:
                    consumo_por_classe[classe.lower()] = consumo_medio
                    logger.info(
                        f"  {classe}: {consumo_medio:.1f} kWh/mês "
                        f"(n={qtd_ucs} UCs)"
                    )

            return consumo_por_classe

    except Exception as exc:
        logger.error(f"Erro ao buscar BDGD: {exc}", exc_info=True)
        return {}


def calibrate_profiles_with_bdgd(
    distribuidora: Optional[str] = None,
    municipio: Optional[str] = None,
    use_fallback: bool = True
) -> Dict[str, List[float]]:
    """
    Calibra perfis de carga usando dados reais da BDGD.

    Args:
        distribuidora: Distribuidora para calibração
        municipio: Município para calibração
        use_fallback: Se True, usa valores padrão quando BDGD não disponível

    Returns:
        Dict com {classe: curva_horaria_kw}
    """
    logger.info("Calibrando perfis com dados da BDGD")

    engine = get_engine()
    perfis_calibrados = {}

    # Buscar consumo médio da BDGD
    consumo_bdgd = get_bdgd_consumo_medio(
        engine,
        distribuidora=distribuidora,
        municipio=municipio
    )

    # Se não houver dados da BDGD, usar valores padrão
    if not consumo_bdgd and use_fallback:
        logger.warning("BDGD vazia, usando valores de consumo padrão")
        consumo_bdgd = {
            "residencial": 150.0,      # 150 kWh/mês (ANEEL 2023)
            "comercial": 800.0,        # 800 kWh/mês
            "industrial": 15000.0,     # 15 MWh/mês
            "rural": 300.0,            # 300 kWh/mês
            "poder_publico": 2000.0    # 2 MWh/mês
        }

    # Calibrar cada classe
    for classe, consumo_kwh_mes in consumo_bdgd.items():
        try:
            curva_kw = load_profiles.apply_profile_to_load(
                classe=classe,
                consumo_medio_mensal_kwh=consumo_kwh_mes
            )
            perfis_calibrados[classe] = curva_kw

            pico_kw = max(curva_kw)
            media_kw = sum(curva_kw) / 24
            logger.info(
                f"  {classe.title()}: {consumo_kwh_mes:.1f} kWh/mês "
                f"→ pico {pico_kw:.2f} kW, média {media_kw:.2f} kW"
            )

        except Exception as exc:
            logger.error(f"Erro ao calibrar {classe}: {exc}")
            continue

    return perfis_calibrados


def get_profile_for_location(
    classe: str,
    lat: float,
    lon: float,
    raio_km: float = 5.0
) -> List[float]:
    """
    Retorna perfil calibrado para uma localização específica.

    Busca UCs da BDGD próximas ao ponto e calcula consumo médio local.

    Args:
        classe: Classe de consumo
        lat: Latitude do ponto
        lon: Longitude do ponto
        raio_km: Raio de busca em km

    Returns:
        Curva horária calibrada em kW
    """
    logger.info(f"Buscando perfil para ({lat:.4f}, {lon:.4f}) r={raio_km}km")

    engine = get_engine()
    query = text("""
        SELECT
            AVG(energia_kwh_mes) as consumo_medio
        FROM bdgd_unidades_consumidoras
        WHERE classe_consumo = :classe
          AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :raio_metros
              )
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {
                "classe": classe.title(),
                "lat": lat,
                "lon": lon,
                "raio_metros": raio_km * 1000
            }).fetchone()

            if result and result.consumo_medio:
                consumo_local = float(result.consumo_medio)
                logger.info(f"Consumo local {classe}: {consumo_local:.1f} kWh/mês")

                return load_profiles.apply_profile_to_load(
                    classe=classe,
                    consumo_medio_mensal_kwh=consumo_local
                )
            else:
                logger.warning(
                    f"Sem dados BDGD próximos, usando perfil padrão"
                )
                # Fallback para perfil genérico
                consumo_padrao = {
                    "residencial": 150.0,
                    "comercial": 800.0,
                    "industrial": 15000.0,
                    "rural": 300.0,
                    "poder_publico": 2000.0
                }
                return load_profiles.apply_profile_to_load(
                    classe=classe,
                    consumo_medio_mensal_kwh=consumo_padrao.get(classe, 150.0)
                )

    except Exception as exc:
        logger.error(f"Erro ao buscar perfil local: {exc}", exc_info=True)
        raise


def get_calibration_summary(
    distribuidora: Optional[str] = None
) -> Dict[str, any]:
    """
    Retorna sumário estatístico da calibração com BDGD.

    Args:
        distribuidora: Filtrar por distribuidora

    Returns:
        Dict com estatísticas de calibração
    """
    engine = get_engine()
    consumo_bdgd = get_bdgd_consumo_medio(engine, distribuidora=distribuidora)

    if not consumo_bdgd:
        return {
            "status": "sem_dados_bdgd",
            "classes": [],
            "total_ucs": 0
        }

    # Obter total de UCs
    where_clause = "WHERE distribuidora = :dist" if distribuidora else ""
    params = {"dist": distribuidora} if distribuidora else {}

    query = text(f"""
        SELECT
            COUNT(*) as total_ucs,
            COUNT(DISTINCT classe_consumo) as total_classes,
            COUNT(DISTINCT municipio) as total_municipios
        FROM bdgd_unidades_consumidoras
        {where_clause}
    """)

    with engine.connect() as conn:
        stats = conn.execute(query, params).fetchone()

        return {
            "status": "calibrado",
            "distribuidora": distribuidora,
            "total_ucs": int(stats.total_ucs or 0),
            "total_classes": int(stats.total_classes or 0),
            "total_municipios": int(stats.total_municipios or 0),
            "consumo_medio_por_classe": {
                classe: round(kwh, 2)
                for classe, kwh in consumo_bdgd.items()
            }
        }


if __name__ == "__main__":
    # Teste
    logging.basicConfig(level=logging.INFO)

    print("\n=== Teste de Calibração ===")
    perfis = calibrate_profiles_with_bdgd(use_fallback=True)

    for classe, curva in perfis.items():
        print(f"\n{classe.title()}:")
        print(f"  Pico: {max(curva):.2f} kW")
        print(f"  Vale: {min(curva):.2f} kW")
        print(f"  Média: {sum(curva)/24:.2f} kW")
