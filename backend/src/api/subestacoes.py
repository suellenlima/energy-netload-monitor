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


@router.post("/associar-ucs")
def associar_ucs_a_subestacoes(
    engine: EngineDepends,
    raio_km: float = Query(10.0, description="Raio de busca em km"),
    origem: str = Query("detectadas", description="Origem: detectadas, ons ou ambas")
):
    """
    Associa unidades consumidoras (GD) à subestação mais próxima.

    Este endpoint realiza associação espacial entre UCs e subestações,
    permitindo análise local por subestação (FASE 2 do plano).

    - **raio_km**: Raio máximo de busca em km (padrão: 10km)
    - **origem**: Usar subestações 'detectadas', 'ons' ou 'ambas'

    Retorna estatísticas da associação realizada.
    """
    from ..services.subestacoes_clustering import associate_ucs_to_nearest_subestacao

    try:
        resultado = associate_ucs_to_nearest_subestacao(
            engine=engine,
            raio_km=raio_km,
            origem=origem,
            logger=logger
        )
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao associar UCs: {exc}", exc_info=True)
        raise DatabaseError("Falha ao associar UCs a subestações") from exc


@router.get("/{subestacao_id}/mix-consumidores")
def get_mix_consumidores_subestacao(
    subestacao_id: int,
    engine: EngineDepends
):
    """
    Retorna o mix de consumidores por subestação.

    Mostra a quantidade de UCs por classe de consumo e tipo de estabelecimento
    associadas a uma subestação específica.

    - **subestacao_id**: ID da subestação

    **Response**:
    ```json
    {
      "subestacao_id": 123,
      "mix": {
        "Residencial": {
          "qtd_instalacoes": 450,
          "qtd_unidades_consumidoras": 5420,
          "potencia_total_mw": 8.13,
          "por_tipo": {
            "residencia": {...},
            "predio_residencial": {...}
          }
        },
        "Comercial": {...}
      },
      "totais": {
        "qtd_instalacoes": 500,
        "qtd_unidades_consumidoras": 6000,
        "potencia_total_mw": 12.5
      }
    }
    ```
    """
    from ..services.subestacoes_clustering import get_uc_mix_by_subestacao

    try:
        resultado = get_uc_mix_by_subestacao(
            engine=engine,
            subestacao_id=subestacao_id,
            logger=logger
        )
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao buscar mix de consumidores: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar mix de consumidores") from exc


@router.get("/{subestacao_id}/carga-sintetica")
def get_carga_sintetica_subestacao(
    subestacao_id: int,
    engine: EngineDepends
):
    """
    Calcula curva de carga sintética horária para uma subestação.

    Combina mix de consumidores com perfis típicos para gerar curva de 24 horas.

    **Formula**:
    ```
    Carga_hora(h) = Σ (Qtd_UCs_classe × Consumo_medio_UC × Perfil_classe(h))
    ```

    - **subestacao_id**: ID da subestação

    **Response**:
    ```json
    {
      "subestacao_id": 123,
      "curva_horaria_kw": [120.5, 110.2, ..., 245.8],  // 24 valores
      "curva_horaria_mw": [0.121, 0.110, ..., 0.246],
      "estatisticas": {
        "pico_kw": 245.8,
        "hora_pico": 19,
        "vale_kw": 95.3,
        "hora_vale": 3,
        "media_kw": 156.7,
        "fator_carga": 0.638
      },
      "contribuicao_por_classe": {
        "Residencial": {
          "qtd_ucs": 5420,
          "curva_horaria_kw": [...],
          "pico_kw": 180.5
        }
      },
      "total_ucs": 6000
    }
    ```
    """
    from ..services.synthetic_load import calculate_synthetic_load_by_subestacao

    try:
        resultado = calculate_synthetic_load_by_subestacao(
            engine=engine,
            subestacao_id=subestacao_id
        )
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao calcular carga sintética: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular carga sintética") from exc
