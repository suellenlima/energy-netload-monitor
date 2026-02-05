"""
API Layer - Subestacao FastAPI Endpoints (DDD Pattern)
Substitui endpoints legados com nova arquitetura DDD
"""

from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends
import logging

# Legacy imports (mantém compatibilidade com endpoints existentes)
from ..core import DatabaseError, get_engine
from ..schemas import (
    AtualizarDetectadasResponse,
    SubestacaoDetectadaResponse,
    SubestacaoONSResponse,
    SubestacaoResumo,
    TaskAsyncResponse,
)
from .deps import DistribuidoraQuery, LimiteQuery, EngineDepends

# DDD imports
from src.infrastructure.persistence.subestacao import (
    SQLAlchemySubestacaoRepository,
    SubestacaoMapper,
)
from src.application.subestacao import (
    ObtenerSubestacaoUseCase,
    ListarSubestacioesUseCase,
    ListarPorDistribuidoraUseCase,
    ListarPorTensaoUseCase,
    ObtenerEstatisticasUseCase,
    AtivarSubestacaoUseCase,
    DesativarSubestacaoUseCase,
    ObtenerTipoTensaoUseCase,
    ObtenerONSSubestacioesUseCase,
    ObtenerGeoJSONSubestacioesUseCase,
    ObtenerResumoSubestacioesUseCase,
    ObtenerDetalhesSubestacaoUseCase,
    AssociarUCsUseCase,
    ObtenerMixConsumidoresUseCase,
    ObtenerCargaSinteticaUseCase,
    # TIER 2: Clustering e Area use cases
    DetectarSubestacioesClusteringUseCase,
    AtualizarSubestacioesDetectadasUseCase,
    ExecutarClusteringBackgroundUseCase,
    ObtenerAreaSubestacaoUseCase,
    ObtenerTransformadoresUseCase,
    ObtenerEstatisticasAreasUseCase,
)
from src.domain.subestacao import SubestacaoError, SubestacaoNotFoundError

logger = logging.getLogger(__name__)

from fastapi import BackgroundTasks
import uuid

# DDD router
router = APIRouter(prefix="/subestacoes", tags=["Subestações"])

# DDD router (novos endpoints)
router_ddd = APIRouter(prefix="/api/v1/subestacoes", tags=["Subestações (DDD)"])

# Repository singleton
_repository = None

def get_repository():
    """Dependency injection do repository"""
    global _repository
    if _repository is None:
        _repository = SQLAlchemySubestacaoRepository()
    return _repository


@router.get("/ons", response_model=list[SubestacaoONSResponse])
def get_subestacoes_ons(
    distribuidora: DistribuidoraQuery = None,
    limite: LimiteQuery = 100,
    repository = Depends(get_repository)
):
    """
    Lista subestações do ONS (dados públicos oficiais) - Migrado para DDD.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **limite**: Máximo de registros (default 100)
    """
    try:
        use_case = ObtenerONSSubestacioesUseCase(repository=repository)
        resultado = use_case.executar(
            distribuidora_codigo=distribuidora,
            limite=limite
        )
        return resultado['items']
    except Exception as exc:
        logger.error(f"Erro ao buscar subestações ONS: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar subestações ONS") from exc


@router.get("/detectadas", response_model=list[SubestacaoDetectadaResponse])
def get_subestacoes_detectadas(
    distribuidora: DistribuidoraQuery = None,
    eps_km: float = Query(default=5.0, ge=0.5, le=50.0),
    repository = Depends(get_repository)
):
    """
    Detecta subestações automaticamente via clustering de GD - 100% DDD.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **eps_km**: Raio de busca em km para clustering (default 5 km)
    """
    try:
        use_case = DetectarSubestacioesClusteringUseCase(repository=repository)
        resultado = use_case.executar(distribuidora_codigo=distribuidora, eps_km=eps_km)
        return resultado['dados']
    except Exception as exc:
        logger.error(f"Erro ao detectar subestações por clustering: {exc}", exc_info=True)
        raise DatabaseError("Falha ao detectar subestações por clustering") from exc


@router.post("/detectadas/atualizar", response_model=AtualizarDetectadasResponse)
def atualizar_subestacoes_detectadas(
    distribuidora: DistribuidoraQuery = None,
    eps_km: float = Query(default=5.0, ge=0.5, le=50.0, description="Raio de busca em km"),
    limpar_anterior: bool = Query(default=False, description="Limpar subestações anteriormente detectadas"),
    repository = Depends(get_repository)
):
    """
    Executa detecção de subestações via clustering e armazena resultados - 100% DDD.

    - **distribuidora**: Processar apenas uma distribuidora (opcional)
    - **eps_km**: Raio de busca em km para clustering (default 5 km)
    - **limpar_anterior**: Se True, limpa subestações anteriormente detectadas (default False)
    """
    try:
        use_case = AtualizarSubestacioesDetectadasUseCase(repository=repository)
        resultado = use_case.executar(
            distribuidora_codigo=distribuidora,
            eps_km=eps_km,
            limpar_anterior=limpar_anterior
        )

        return AtualizarDetectadasResponse(
            status="sucesso",
            mensagem=resultado['mensagem'],
            quantidade=resultado.get('quantidade', 0),
            raio_km=eps_km,
        )
    except Exception as exc:
        logger.error(f"Erro ao atualizar subestações: {exc}", exc_info=True)
        raise DatabaseError("Falha ao executar clustering") from exc


def _run_clustering_background(
    task_id: str,
    distribuidora: str | None,
    eps_km: float,
    repository = None,
) -> None:
    """
    Executa clustering em background usando DDD use case.

    Args:
        task_id: ID da tarefa para logging
        distribuidora: Filtrar por distribuidora (opcional)
        eps_km: Raio de busca em km
        repository: Repositório de subestações
    """
    if repository is None:
        repository = get_repository()
    
    logger.info(f"[Task {task_id}] Iniciando clustering em background")
    try:
        use_case = ExecutarClusteringBackgroundUseCase(repository=repository)
        resultado = use_case.executar(
            task_id=task_id,
            distribuidora_codigo=distribuidora,
            eps_km=eps_km
        )
        
        if resultado['sucesso']:
            logger.info(f"[Task {task_id}] Concluído: {resultado['quantidade']} subestações processadas")
        else:
            logger.error(f"[Task {task_id}] Falha: {resultado['mensagem']}")
    except Exception as exc:
        logger.error(f"[Task {task_id}] Erro no clustering: {exc}", exc_info=True)


@router.post("/detectadas/atualizar-async", response_model=TaskAsyncResponse)
def atualizar_subestacoes_async(
    background_tasks: BackgroundTasks,
    distribuidora: DistribuidoraQuery = None,
    eps_km: float = Query(default=5.0, ge=0.5, le=50.0, description="Raio de busca em km"),
    repository = Depends(get_repository)
):
    """
    Inicia detecção de subestações em background (assíncrono) - 100% DDD.

    Retorna imediatamente com um task_id para acompanhamento.
    O processamento continua em background usando ExecutarClusteringBackgroundUseCase.

    - **distribuidora**: Processar apenas uma distribuidora (opcional)
    - **eps_km**: Raio de busca em km para clustering (default 5 km)
    """
    task_id = str(uuid.uuid4())[:8]

    background_tasks.add_task(
        _run_clustering_background,
        task_id=task_id,
        distribuidora=distribuidora,
        eps_km=eps_km,
        repository=repository,
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
    origem: str = Query(default="ambas", description="ons, detectadas ou ambas"),
    limite: LimiteQuery = 100,
    repository = Depends(get_repository)
):
    """
    Retorna subestações em formato GeoJSON - Migrado para DDD.

    - **origem**: "ons", "detectadas" ou "ambas" (default)
    - **limite**: Máximo de registros por origem
    """
    try:
        use_case = ObtenerGeoJSONSubestacioesUseCase(repository=repository)
        resultado = use_case.executar(limite=limite)
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao gerar GeoJSON: {exc}", exc_info=True)
        raise DatabaseError("Falha ao gerar GeoJSON") from exc


@router.get("/resumo", response_model=list[SubestacaoResumo])
def get_subestacoes_resumo(
    repository = Depends(get_repository)
):
    """Retorna resumo de subestações por distribuidora e origem - Migrado para DDD."""
    try:
        use_case = ObtenerResumoSubestacioesUseCase(repository=repository)
        resultado = use_case.executar()
        return resultado['resumo']
    except Exception as exc:
        logger.error(f"Erro ao gerar resumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao gerar resumo") from exc




# ============================================================================
# ÁREA DE COBERTURA - INTEGRAÇÃO COM TRANSFORMADORES
# ============================================================================


@router.get("/{id}/area")
def get_subestacao_area(
    id: int,
    formato: str = Query("geojson", regex="^(geojson|wkt|json)$"),
    repository = Depends(get_repository)
):
    """
    Obtém a área de cobertura de uma subestação - 100% DDD.
    
    - **formato**: geojson | wkt | json (default: geojson)
    """
    try:
        use_case = ObtenerAreaSubestacaoUseCase(repository=repository)
        area_data = use_case.executar(subestacao_id=id)
        
        if not area_data.get('dados'):
            raise DatabaseError(f"Subestação {id} não encontrada")
        
        dados = area_data['dados']
        
        if formato == "wkt":
            return {"id": id, "wkt": dados.get('wkt_area')}
        elif formato == "geojson":
            import json
            return {
                "id": id,
                "type": "Feature",
                "geometry": json.loads(dados.get('geojson_area')) if dados.get('geojson_area') else None,
                "properties": {
                    "nome": dados.get('nome'),
                    "area_km2": dados.get('area_km2'),
                    "total_transformadores": dados.get('total_transformadores')
                }
            }
        else:  # json
            return dados
            
    except Exception as exc:
        logger.error(f"Erro ao buscar área da subestação {id}: {exc}", exc_info=True)
        raise DatabaseError(str(exc))


@router.get("/{id}/transformadores")
def get_subestacao_transformadores(
    id: int,
    repository = Depends(get_repository)
):
    """
    Lista todos os transformadores de uma subestação com suas áreas de cobertura - 100% DDD.
    """
    try:
        use_case = ObtenerTransformadoresUseCase(repository=repository)
        resultado = use_case.executar(subestacao_id=id)
        
        if not resultado.get('dados'):
            raise DatabaseError(f"Subestação {id} não encontrada ou sem transformadores")
        
        return {
            "subestacao_id": id,
            "total": len(resultado['dados']),
            "transformadores": resultado['dados']
        }
        
    except Exception as exc:
        logger.error(f"Erro ao buscar transformadores da subestação {id}: {exc}", exc_info=True)
        raise DatabaseError(str(exc))


@router.get("/areas/stats")
def get_areas_statistics(
    repository = Depends(get_repository)
):
    """
    Obtém estatísticas gerais de áreas de cobertura - 100% DDD.
    """
    try:
        use_case = ObtenerEstatisticasAreasUseCase(repository=repository)
        stats = use_case.executar()
        return stats.get('dados', {})
        
    except Exception as exc:
        logger.error(f"Erro ao buscar estatísticas de áreas: {exc}", exc_info=True)
        raise DatabaseError(str(exc))
@router.post("/associar-ucs")
def associar_ucs_a_subestacoes(
    raio_km: float = Query(10.0, description="Raio de busca em km"),
    origem: str = Query("detectadas", description="Origem: detectadas, ons ou ambas"),
    repository = Depends(get_repository)
):
    """
    Associa unidades consumidoras (GD) à subestação mais próxima - Migrado para DDD.

    Este endpoint realiza associação espacial entre UCs e subestações,
    permitindo análise local por subestação (FASE 2 do plano).

    - **raio_km**: Raio máximo de busca em km (padrão: 10km)
    - **origem**: Usar subestações 'detectadas', 'ons' ou 'ambas'

    Retorna estatísticas da associação realizada.
    """
    try:
        use_case = AssociarUCsUseCase(repository=repository)
        resultado = use_case.executar(raio_km=raio_km, origem=origem)
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao associar UCs: {exc}", exc_info=True)
        raise DatabaseError("Falha ao associar UCs a subestações") from exc


@router.get("/{subestacao_id}/mix-consumidores")
def get_mix_consumidores_subestacao(
    subestacao_id: int,
    repository = Depends(get_repository)
):
    """
    Retorna o mix de consumidores por subestação - Migrado para DDD.

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
    try:
        use_case = ObtenerMixConsumidoresUseCase(repository=repository)
        resultado = use_case.executar(subestacao_id=subestacao_id)
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao buscar mix de consumidores: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar mix de consumidores") from exc


@router.get("/{subestacao_id}/carga-sintetica")
def get_carga_sintetica_subestacao(
    subestacao_id: int,
    repository = Depends(get_repository)
):
    """
    Calcula curva de carga sintética horária para uma subestação - Migrado para DDD.

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
    try:
        use_case = ObtenerCargaSinteticaUseCase(repository=repository)
        resultado = use_case.executar(subestacao_id=subestacao_id)
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao calcular carga sintética: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular carga sintética") from exc


# ============================================================================
# DDD Layer Endpoints - Novos endpoints usando arquitetura DDD
# ============================================================================

@router_ddd.get("/{codigo}", response_model=Dict[str, Any])
async def obtener_subestacao_ddd(
    codigo: str,
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Obtém detalhes de uma subestação por código"""
    try:
        use_case = ObtenerSubestacaoUseCase(repository=repository)
        resultado = use_case.executar(codigo=codigo)
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados']
        }
    except SubestacaoNotFoundError as e:
        logger.error(f"Subestação {codigo} não encontrada: {e}")
        raise HTTPException(status_code=404, detail=f"Subestação {codigo} não encontrada")
    except SubestacaoError as e:
        logger.error(f"Erro ao obter subestação {codigo}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter subestação: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao obter subestação: {str(e)}")


@router_ddd.get("", response_model=Dict[str, Any])
async def listar_subestacoes_ddd(
    offset: int = Query(0, ge=0),
    limite: int = Query(20, ge=1, le=100),
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Lista todas as subestações com paginação"""
    try:
        use_case = ListarSubestacioesUseCase(repository=repository)
        resultado = use_case.executar(offset=offset, limite=limite)
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados'],
            'paginacao': resultado.get('paginacao', {})
        }
    except SubestacaoError as e:
        logger.error(f"Erro ao listar subestações: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao listar subestações: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao listar subestações: {str(e)}")


@router_ddd.get("/stats", response_model=Dict[str, Any])
async def obter_estatisticas_ddd(
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Obtém estatísticas gerais das subestações"""
    try:
        use_case = ObtenerEstatisticasUseCase(repository=repository)
        resultado = use_case.executar()
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados']
        }
    except SubestacaoError as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao obter estatísticas: {str(e)}")


@router_ddd.get("/tensao/{tensao_kv}", response_model=Dict[str, Any])
async def listar_por_tensao_ddd(
    tensao_kv: float,
    offset: int = Query(0, ge=0),
    limite: int = Query(20, ge=1, le=100),
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Lista subestações filtradas por tensão nominal (kV)"""
    try:
        use_case = ListarPorTensaoUseCase(repository=repository)
        resultado = use_case.executar(
            tensao_nominal_kv=tensao_kv,
            offset=offset,
            limite=limite
        )
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados'],
            'paginacao': resultado.get('paginacao', {})
        }
    except SubestacaoError as e:
        logger.error(f"Erro ao filtrar por tensão: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao filtrar por tensão: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao filtrar por tensão: {str(e)}")


@router_ddd.get("/distribuidora/{codigo}", response_model=Dict[str, Any])
async def listar_por_distribuidora_ddd(
    codigo: str,
    offset: int = Query(0, ge=0),
    limite: int = Query(20, ge=1, le=100),
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Lista subestações filtradas por código da distribuidora"""
    try:
        use_case = ListarPorDistribuidoraUseCase(repository=repository)
        resultado = use_case.executar(
            distribuidora_codigo=codigo,
            offset=offset,
            limite=limite
        )
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados'],
            'paginacao': resultado.get('paginacao', {})
        }
    except SubestacaoError as e:
        logger.error(f"Erro ao filtrar por distribuidora: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao filtrar por distribuidora: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao filtrar por distribuidora: {str(e)}")


@router_ddd.get("/{codigo}/tipo-tensao", response_model=Dict[str, Any])
async def obter_tipo_tensao_ddd(
    codigo: str,
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Obtém classificação de tipo de tensão (AT/MT/BT) para uma subestação"""
    try:
        use_case = ObtenerTipoTensaoUseCase(repository=repository)
        resultado = use_case.executar(codigo=codigo)
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'dados': resultado['dados']
        }
    except SubestacaoNotFoundError as e:
        logger.error(f"Subestação {codigo} não encontrada: {e}")
        raise HTTPException(status_code=404, detail=f"Subestação {codigo} não encontrada")
    except SubestacaoError as e:
        logger.error(f"Erro ao obter tipo de tensão: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao obter tipo de tensão: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao obter tipo de tensão: {str(e)}")


@router_ddd.post("/{codigo}/ativar", response_model=Dict[str, Any])
async def ativar_subestacao_ddd(
    codigo: str,
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Ativa uma subestação"""
    try:
        use_case = AtivarSubestacaoUseCase(repository=repository)
        resultado = use_case.executar(codigo=codigo)
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'mensagem': f"Subestação {codigo} ativada com sucesso",
            'dados': resultado['dados']
        }
    except SubestacaoNotFoundError as e:
        logger.error(f"Subestação {codigo} não encontrada: {e}")
        raise HTTPException(status_code=404, detail=f"Subestação {codigo} não encontrada")
    except SubestacaoError as e:
        logger.error(f"Erro ao ativar subestação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao ativar subestação: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao ativar subestação: {str(e)}")


@router_ddd.post("/{codigo}/desativar", response_model=Dict[str, Any])
async def desativar_subestacao_ddd(
    codigo: str,
    repository = Depends(get_repository)
) -> Dict[str, Any]:
    """Desativa uma subestação"""
    try:
        use_case = DesativarSubestacaoUseCase(repository=repository)
        resultado = use_case.executar(codigo=codigo)
        
        return {
            'status': 'sucesso',
            'codigo': 200,
            'mensagem': f"Subestação {codigo} desativada com sucesso",
            'dados': resultado['dados']
        }
    except SubestacaoNotFoundError as e:
        logger.error(f"Subestação {codigo} não encontrada: {e}")
        raise HTTPException(status_code=404, detail=f"Subestação {codigo} não encontrada")
    except SubestacaoError as e:
        logger.error(f"Erro ao desativar subestação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao desativar subestação: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao desativar subestação: {str(e)}")
