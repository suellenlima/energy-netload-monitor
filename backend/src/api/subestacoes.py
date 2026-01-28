"""
Endpoints para gerenciar e consultar dados de subestações.
Fornece API para subestações públicas (ONS) e detectadas (clustering).
"""

from fastapi import APIRouter, HTTPException

from ..core.database import get_engine
from ..services.subestacoes_clustering import detect_subestacoes_by_clustering, load_detected_subestacoes

router = APIRouter(prefix="/subestacoes")


@router.get("/ons")
def get_subestacoes_ons(distribuidora: str | None = None, limite: int = 100):
    """
    Lista subestações do ONS (dados públicos oficiais).
    
    Parâmetros:
    - distribuidora: Filtrar por distribuidora (opcional)
    - limite: Máximo de registros (default 100)
    """
    try:
        import pandas as pd
        from sqlalchemy import text
        import logging
        
        logger = logging.getLogger("api.subestacoes")
        engine = get_engine()
        
        where_clause = ""
        params = {"limite": limite}
        
        if distribuidora:
            where_clause = "WHERE distribuidora ILIKE :dist"
            params["dist"] = f"%{distribuidora}%"
        
        query_str = f"""
            SELECT id_estacao as id, nome, sigla_se, tensao_kv, subsistema, distribuidora,
                   latitude, longitude, fonte_dados
            FROM subestacoes_ons
            {where_clause}
            ORDER BY tensao_kv DESC, nome
            LIMIT :limite
        """
        
        logger.debug(f"Query: {query_str}, Params: {params}")
        
        df = pd.read_sql(text(query_str), engine, params=params)
        
        logger.info(f"Retornadas {len(df)} subestações ONS")
        return df.to_dict(orient="records")
    
    except Exception as exc:
        import logging
        logger = logging.getLogger("api.subestacoes")
        logger.error(f"Erro ao buscar subestações ONS: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao buscar subestações ONS: {str(exc)}")


@router.get("/detectadas")
def get_subestacoes_detectadas(distribuidora: str | None = None, limite: int = 100):
    """
    Lista subestações detectadas automaticamente via clustering de GD.

    Parâmetros:
    - distribuidora: Filtrar por distribuidora (opcional)
    - limite: Máximo de registros (default 100)
    """
    try:
        import logging

        import pandas as pd
        from sqlalchemy import text

        from ..core.database import table_exists

        logger = logging.getLogger("api.subestacoes")
        engine = get_engine()

        # Verificar se tabela existe (usa cache)
        if not table_exists("subestacoes_detectadas", engine):
            logger.info("Tabela subestacoes_detectadas não existe ainda")
            return []
        
        where_clause = ""
        params = {"limite": limite}
        
        if distribuidora:
            where_clause = "WHERE distribuidora ILIKE :dist"
            params["dist"] = f"%{distribuidora}%"
        
        query_str = f"""
            SELECT id, cluster_id, nome, latitude, longitude, distribuidora,
                   subsistema, quantidade_gd, potencia_total_mw, raio_deteccao_km,
                   data_deteccao
            FROM subestacoes_detectadas
            {where_clause}
            ORDER BY potencia_total_mw DESC
            LIMIT :limite
        """
        
        df = pd.read_sql(text(query_str), engine, params=params)
        
        logger.info(f"Retornadas {len(df)} subestações detectadas")
        return df.to_dict(orient="records") if not df.empty else []
    
    except Exception as exc:
        import logging
        logger = logging.getLogger("api.subestacoes")
        logger.error(f"Erro ao buscar subestações detectadas: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao buscar subestações detectadas: {str(exc)}")


@router.post("/detectadas/atualizar")
def atualizar_subestacoes_detectadas(distribuidora: str | None = None, eps_km: float = 5.0):
    """
    Executa detecção de subestações via clustering e armazena resultados.
    
    Parâmetros:
    - distribuidora: Processar apenas uma distribuidora (opcional)
    - eps_km: Raio de busca em km para clustering (default 5 km)
    """
    try:
        import logging
        
        engine = get_engine()
        logger = logging.getLogger("api.subestacoes")
        
        # Executar clustering
        df_detectadas = detect_subestacoes_by_clustering(
            engine,
            distribuidora=distribuidora,
            eps_km=eps_km,
            logger=logger
        )
        
        if df_detectadas.empty:
            return {
                "status": "sucesso",
                "mensagem": "Nenhuma subestação detectada",
                "quantidade": 0
            }
        
        # Carregar no banco
        quantidade = load_detected_subestacoes(df_detectadas, engine, logger)
        
        return {
            "status": "sucesso",
            "mensagem": f"Detectadas e armazenadas {quantidade} subestações",
            "quantidade": quantidade,
            "raio_km": eps_km
        }
    
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar subestações: {exc}")


def _df_to_geojson_features(df, property_mapping: dict, tipo: str) -> list[dict]:
    """
    Converte DataFrame em features GeoJSON de forma vetorizada.
    Muito mais eficiente que iterrows().
    """
    if df.empty:
        return []

    features = []
    records = df.to_dict(orient="records")

    for row in records:
        properties = {
            prop_name: row.get(col_name)
            for prop_name, col_name in property_mapping.items()
        }
        properties["tipo"] = tipo

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": properties
        })

    return features


@router.get("/geo")
def get_subestacoes_geojson(origem: str = "ambas", limite: int = 100):
    """
    Retorna subestações em formato GeoJSON.

    Parâmetros:
    - origem: "ons", "detectadas" ou "ambas" (default)
    - limite: Máximo de registros por origem
    """
    try:
        import pandas as pd
        from sqlalchemy import text

        from ..core.database import table_exists

        engine = get_engine()
        features = []

        # Buscar subestações ONS
        if origem in ["ons", "ambas"]:
            query_ons = text("""
                SELECT id_estacao as id, nome, sigla_se, tensao_kv, subsistema, distribuidora,
                       latitude, longitude, 'ONS' as origem
                FROM subestacoes_ons
                LIMIT :limite
            """)
            df_ons = pd.read_sql(query_ons, engine, params={"limite": limite})

            ons_mapping = {
                "id": "id",
                "nome": "nome",
                "sigla": "sigla_se",
                "tensao_kv": "tensao_kv",
                "subsistema": "subsistema",
                "distribuidora": "distribuidora",
                "origem": "origem",
            }
            features.extend(_df_to_geojson_features(df_ons, ons_mapping, "subestacao_ons"))

        # Buscar subestações detectadas (usa cache de table_exists)
        if origem in ["detectadas", "ambas"]:
            if table_exists("subestacoes_detectadas", engine):
                query_det = text("""
                    SELECT id, cluster_id, nome, latitude, longitude, distribuidora,
                           subsistema, quantidade_gd, potencia_total_mw,
                           'DETECTADA' as origem
                    FROM subestacoes_detectadas
                    LIMIT :limite
                """)
                df_det = pd.read_sql(query_det, engine, params={"limite": limite})

                det_mapping = {
                    "id": "id",
                    "nome": "nome",
                    "cluster_id": "cluster_id",
                    "distribuidora": "distribuidora",
                    "subsistema": "subsistema",
                    "quantidade_gd": "quantidade_gd",
                    "potencia_total_mw": "potencia_total_mw",
                    "origem": "origem",
                }
                features.extend(_df_to_geojson_features(df_det, det_mapping, "subestacao_detectada"))

        return {
            "type": "FeatureCollection",
            "features": features
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar GeoJSON: {exc}")


@router.get("/resumo")
def get_subestacoes_resumo():
    """
    Retorna resumo de subestações por distribuidora e origem.
    """
    try:
        import pandas as pd
        from sqlalchemy import text

        from ..core.database import table_exists

        engine = get_engine()

        # Se tabela detectadas não existe, retornar só ONS (usa cache)
        if not table_exists("subestacoes_detectadas", engine):
            query = text("""
                SELECT 
                    distribuidora,
                    COUNT(*) as total_ons,
                    0 as total_detectadas,
                    COUNT(*) as total
                FROM subestacoes_ons
                GROUP BY distribuidora
                ORDER BY total DESC
            """)
        else:
            query = text("""
                SELECT 
                    distribuidora,
                    COUNT(CASE WHEN origem = 'ons' THEN 1 END) as total_ons,
                    COUNT(CASE WHEN origem = 'detectada' THEN 1 END) as total_detectadas,
                    COUNT(*) as total
                FROM (
                    SELECT distribuidora, 'ons' as origem FROM subestacoes_ons
                    UNION ALL
                    SELECT distribuidora, 'detectada' as origem FROM subestacoes_detectadas
                ) t
                GROUP BY distribuidora
                ORDER BY total DESC
            """)
        
        df = pd.read_sql(query, engine)
        
        return df.to_dict(orient="records") if not df.empty else []
    
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar resumo: {exc}")
