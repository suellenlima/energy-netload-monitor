"""Endpoints para análise de carga e fraude."""

import logging

from fastapi import APIRouter

from ..core import DatabaseError
from ..schemas import (
    AlertaFraude,
    CargaOcultaItem,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    PerfilCarga,
    PerfisResponse,
    ResumoGranular,
)
from .deps import AnaliseRepoDepends, DistribuidoraQuery, SubsistemaQuery

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analise", tags=["Análise"])


@router.get("/carga-oculta", response_model=list[CargaOcultaItem])
def calcular_carga_oculta(
    repo: AnaliseRepoDepends,
    subsistema: SubsistemaQuery = "SUDESTE",
    distribuidora: DistribuidoraQuery = None,
):
    """
    Calcula carga oculta estimada (geração solar não medida).

    - **subsistema**: Subsistema elétrico (SUDESTE, NORTE, NORDESTE, SUL)
    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_carga_oculta(subsistema, distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao calcular carga oculta: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular carga oculta") from exc


@router.get("/classes-consumo", response_model=list[ClasseConsumoItem])
def get_classes_consumo(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna consumo por classe de consumidor.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_classes_consumo(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar classes de consumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar classes de consumo") from exc


@router.get("/alertas-fraude", response_model=AlertaFraude | dict)
def get_alertas_fraude(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna último alerta de fraude detectado.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_alerta_fraude(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar alertas de fraude: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar alertas de fraude") from exc


@router.get("/estabelecimentos/contagem", response_model=list[EstabelecimentoContagem])
def get_contagem_estabelecimentos(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna contagem de estabelecimentos por tipo.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_contagem_estabelecimentos(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar contagem: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar contagem de estabelecimentos") from exc


@router.get("/estabelecimentos/resumo", response_model=ResumoGranular | dict)
def get_resumo_estabelecimentos(
    repo: AnaliseRepoDepends,
    distribuidora: DistribuidoraQuery = None,
):
    """
    Retorna resumo geral dos dados granulares.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        return repo.get_resumo_granular(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar resumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar resumo de estabelecimentos") from exc


@router.get("/perfis-carga", response_model=PerfisResponse)
def obter_perfis_carga(
    classes: str | None = None,
):
    """
    Retorna perfis de carga típicos por classe de consumo.

    Os perfis são curvas horárias (24 pontos) com fatores normalizados (média=1.0).
    Para obter a carga em MW/kW, multiplique pelo consumo médio da classe.

    - **classes**: Classes separadas por vírgula (ex: "residencial,comercial").
      Se omitido, retorna todos os perfis disponíveis.

    **Classes disponíveis:**
    - residencial: Pico noturno (18h-22h)
    - comercial: Pico diurno (9h-18h)
    - industrial: Perfil mais plano, operação contínua
    - rural: Picos matinal (5h-7h) e vespertino (17h-19h)
    - poder_publico: Similar ao comercial com iluminação pública noturna

    **Exemplo de uso:**
    ```
    GET /analise/perfis-carga?classes=residencial,comercial
    ```
    """
    from ..services.load_calc import get_load_profiles

    # Parse classes
    classes_list = None
    if classes:
        classes_list = [c.strip() for c in classes.split(",")]

    try:
        resultado = get_load_profiles(classes_list)
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao buscar perfis de carga: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar perfis de carga") from exc


@router.get("/estado-atual")
def obter_estado_atual(
    subsistema: SubsistemaQuery = "SUDESTE",
    distribuidora: DistribuidoraQuery = None,
    subestacao_id: int | None = None,
):
    """
    Retorna o estado atual do sistema (estimado em tempo real).

    Combina:
    - Última carga medida do ONS
    - Irradiância solar atual (API meteorológica)
    - Geração MMGD estimada
    - Consumo real estimado (carga + MMGD)

    **Nota**: Esta é uma ESTIMATIVA INFORMADA, não medição real.
    Smart meters não estão disponíveis publicamente no Brasil.

    - **subsistema**: Subsistema elétrico
    - **distribuidora**: Distribuidora (opcional)
    - **subestacao_id**: ID da subestação (opcional)

    **Resposta**:
    ```json
    {
      "timestamp": "2024-01-15T14:30:00",
      "hora_atual": 14,
      "estimativas": {
        "carga_ons_mw": 45.2,
        "geracao_mmgd_mw": 12.8,
        "consumo_estimado_mw": 58.0,
        "irradiancia_atual_wm2": 850
      }
    }
    ```
    """
    from ..services.realtime_estimation import get_realtime_state

    try:
        estado = get_realtime_state(
            subsistema=subsistema,
            distribuidora=distribuidora,
            subestacao_id=subestacao_id
        )
        return estado
    except Exception as exc:
        logger.error(f"Erro ao calcular estado atual: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular estado atual") from exc
