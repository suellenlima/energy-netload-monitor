"""
Endpoints para gerenciar e consultar dados de subestações.
Fornece API para subestações públicas (ONS) e detectadas (clustering).
"""

import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, Query

from ..core import DatabaseError, get_engine
from ..schemas import (
    AtualizarDetectadasResponse,
    SubestacaoDetectadaResponse,
    SubestacaoONSResponse,
    SubestacaoResumo,
    TaskAsyncResponse,
)
from ..services.subestacoes_clustering import (
    detect_subestacoes_by_clustering,
    load_detected_subestacoes,
)
from .deps import (
    DistribuidoraQuery,
    EngineDepends,
    LimiteQuery,
    SubestacaoRepoDepends,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/subestacoes", tags=["Subestações"])


@router.get("/ons", response_model=list[SubestacaoONSResponse])
def get_subestacoes_ons(
    repo: SubestacaoRepoDepends,
    distribuidora: DistribuidoraQuery = None,
    limite: LimiteQuery = 100,
):
    """
    Lista subestações do ONS (dados públicos oficiais).

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **limite**: Máximo de registros (default 100)
    """
    try:
        return repo.get_ons(distribuidora, limite)
    except Exception as exc:
        logger.error(f"Erro ao buscar subestações ONS: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar subestações ONS") from exc


@router.get("/detectadas", response_model=list[SubestacaoDetectadaResponse])
def get_subestacoes_detectadas(
    repo: SubestacaoRepoDepends,
    distribuidora: DistribuidoraQuery = None,
    limite: LimiteQuery = 100,
):
    """
    Lista subestações detectadas automaticamente via clustering de GD.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **limite**: Máximo de registros (default 100)
    """
    try:
        return repo.get_detectadas(distribuidora, limite)
    except Exception as exc:
        logger.error(f"Erro ao buscar subestações detectadas: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar subestações detectadas") from exc


@router.post("/detectadas/atualizar", response_model=AtualizarDetectadasResponse)
def atualizar_subestacoes_detectadas(
    engine: EngineDepends,
    distribuidora: DistribuidoraQuery = None,
    eps_km: float = Query(default=5.0, ge=0.5, le=50.0, description="Raio de busca em km"),
):
    """
    Executa detecção de subestações via clustering e armazena resultados (síncrono).

    - **distribuidora**: Processar apenas uma distribuidora (opcional)
    - **eps_km**: Raio de busca em km para clustering (default 5 km)
    """
    try:
        df_detectadas = detect_subestacoes_by_clustering(
            engine,
            distribuidora=distribuidora,
            eps_km=eps_km,
            logger=logger,
        )

        if df_detectadas.empty:
            return AtualizarDetectadasResponse(
                status="sucesso",
                mensagem="Nenhuma subestação detectada",
                quantidade=0,
            )

        quantidade = load_detected_subestacoes(df_detectadas, engine, logger)

        return AtualizarDetectadasResponse(
            status="sucesso",
            mensagem=f"Detectadas e armazenadas {quantidade} subestações",
            quantidade=quantidade,
            raio_km=eps_km,
        )
    except Exception as exc:
        logger.error(f"Erro ao atualizar subestações: {exc}", exc_info=True)
        raise DatabaseError("Falha ao executar clustering") from exc


def _run_clustering_background(
    task_id: str,
    distribuidora: str | None,
    eps_km: float,
) -> None:
    """
    Executa clustering em background.

    Args:
        task_id: ID da tarefa para logging
        distribuidora: Filtrar por distribuidora (opcional)
        eps_km: Raio de busca em km
    """
    logger.info(f"[Task {task_id}] Iniciando clustering em background")
    try:
        engine = get_engine()
        df_detectadas = detect_subestacoes_by_clustering(
            engine,
            distribuidora=distribuidora,
            eps_km=eps_km,
            logger=logger,
        )

        if df_detectadas.empty:
            logger.info(f"[Task {task_id}] Nenhuma subestação detectada")
            return

        quantidade = load_detected_subestacoes(df_detectadas, engine, logger)
        logger.info(f"[Task {task_id}] Concluído: {quantidade} subestações detectadas")

    except Exception as exc:
        logger.error(f"[Task {task_id}] Erro no clustering: {exc}", exc_info=True)


@router.post("/detectadas/atualizar-async", response_model=TaskAsyncResponse)
def atualizar_subestacoes_async(
    background_tasks: BackgroundTasks,
    distribuidora: DistribuidoraQuery = None,
    eps_km: float = Query(default=5.0, ge=0.5, le=50.0, description="Raio de busca em km"),
):
    """
    Inicia detecção de subestações em background (assíncrono).

    Retorna imediatamente com um task_id para acompanhamento.
    O processamento continua em background.

    - **distribuidora**: Processar apenas uma distribuidora (opcional)
    - **eps_km**: Raio de busca em km para clustering (default 5 km)
    """
    task_id = str(uuid.uuid4())[:8]

    background_tasks.add_task(
        _run_clustering_background,
        task_id=task_id,
        distribuidora=distribuidora,
        eps_km=eps_km,
    )

    logger.info(f"[Task {task_id}] Tarefa de clustering agendada")

    return TaskAsyncResponse(
        status="iniciado",
        task_id=task_id,
        mensagem="Processamento iniciado em background. Consulte /detectadas para ver os resultados.",
    )


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
def get_subestacoes_geojson(
    repo: SubestacaoRepoDepends,
    origem: str = Query(default="ambas", description="ons, detectadas ou ambas"),
    limite: LimiteQuery = 100,
):
    """
    Retorna subestações em formato GeoJSON.

    - **origem**: "ons", "detectadas" ou "ambas" (default)
    - **limite**: Máximo de registros por origem
    """
    try:
        features = []

        if origem in ["ons", "ambas"]:
            df_ons = repo.get_ons_for_geojson(limite)
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

        if origem in ["detectadas", "ambas"]:
            df_det = repo.get_detectadas_for_geojson(limite)
            if not df_det.empty:
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
        logger.error(f"Erro ao gerar GeoJSON: {exc}", exc_info=True)
        raise DatabaseError("Falha ao gerar GeoJSON") from exc


@router.get("/resumo", response_model=list[SubestacaoResumo])
def get_subestacoes_resumo(repo: SubestacaoRepoDepends):
    """Retorna resumo de subestações por distribuidora e origem."""
    try:
        return repo.get_resumo()
    except Exception as exc:
        logger.error(f"Erro ao gerar resumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao gerar resumo") from exc


# ============================================================================
# ÁREA DE COBERTURA - INTEGRAÇÃO COM TRANSFORMADORES
# ============================================================================

from ..services.area_service import AreaService


@router.get("/{id}/area")
def get_subestacao_area(
    id: int,
    engine: EngineDepends,
    formato: str = Query("geojson", regex="^(geojson|wkt|json)$")
):
    """
    Obtém a área de cobertura de uma subestação.
    
    - **formato**: geojson | wkt | json (default: geojson)
    """
    try:
        service = AreaService(engine)
        area_data = service.obter_area_subestacao(id)
        
        if not area_data:
            raise DatabaseError(f"Subestação {id} não encontrada")
        
        if formato == "wkt":
            return {"id": id, "wkt": area_data['wkt_area']}
        elif formato == "geojson":
            import json
            return {
                "id": id,
                "type": "Feature",
                "geometry": json.loads(area_data['geojson_area']) if area_data['geojson_area'] else None,
                "properties": {
                    "nome": area_data['nome'],
                    "area_km2": area_data['area_km2'],
                    "total_transformadores": area_data['total_transformadores']
                }
            }
        else:  # json
            return area_data
            
    except Exception as exc:
        logger.error(f"Erro ao buscar área da subestação {id}: {exc}", exc_info=True)
        raise DatabaseError(str(exc))


@router.get("/{id}/transformadores")
def get_subestacao_transformadores(
    id: int,
    engine: EngineDepends
):
    """
    Lista todos os transformadores de uma subestação com suas áreas de cobertura.
    """
    try:
        service = AreaService(engine)
        df = service.listar_transformadores_subestacao(id)
        
        if df.empty:
            raise DatabaseError(f"Subestação {id} não encontrada ou sem transformadores")
        
        return {
            "subestacao_id": id,
            "total": len(df),
            "transformadores": df.to_dict(orient='records')
        }
        
    except Exception as exc:
        logger.error(f"Erro ao buscar transformadores da subestação {id}: {exc}", exc_info=True)
        raise DatabaseError(str(exc))


@router.get("/areas/stats")
def get_areas_statistics(engine: EngineDepends):
    """
    Obtém estatísticas gerais de áreas de cobertura.
    """
    try:
        service = AreaService(engine)
        stats = service.obter_estatisticas_areas()
        return stats
        
    except Exception as exc:
        logger.error(f"Erro ao buscar estatísticas de áreas: {exc}", exc_info=True)
        raise DatabaseError(str(exc))
