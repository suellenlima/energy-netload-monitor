"""
Endpoints para gerenciar e consultar dados de subestações.
Fornece API para subestações públicas (ONS) e detectadas (clustering).
"""

import logging

from fastapi import APIRouter, Query

from ..core import DatabaseError
from ..schemas import (
    AtualizarDetectadasResponse,
    SubestacaoDetectadaResponse,
    SubestacaoONSResponse,
    SubestacaoResumo,
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
    Executa detecção de subestações via clustering e armazena resultados.

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
