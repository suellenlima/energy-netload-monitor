"""Endpoints para análise de carga e fraude."""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends

from ..application.analise import (
    DetectarAnomalasUseCase,
    ObtenerAlertaFraudeUseCase,
    ObtenerAlertasHistoricoUseCase,
    ObtenerCargaOcultaUseCase,
    ObtenerClassesConsumoUseCase,
    ObtenerContagemEstabelecimentosUseCase,
    ObtenerEstadoAtualUseCase,
    ObtenerPerfisCargaUseCase,
    ObtenerResumoGranularUseCase,
)
from ..core import DatabaseError
from ..infrastructure.persistence.analise import AnaliseRepositorySQLAlchemy
from ..schemas import (
    AlertaFraude,
    CargaDistribuidoraAtual,
    CargaOcultaItem,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    PerfilCarga,
    PerfisResponse,
    ResumoGranular,
)
from .deps import DistribuidoraQuery, SubsistemaQuery
from ..core.database import get_engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analise", tags=["Análise"])


def get_repository(engine=Depends(get_engine)):
    """Get Analise repository."""
    return AnaliseRepositorySQLAlchemy(engine)


@router.get("/carga-oculta", response_model=list[CargaOcultaItem])
def calcular_carga_oculta(
    subsistema: SubsistemaQuery = "SUDESTE",
    distribuidora: DistribuidoraQuery = None,
    repo=Depends(get_repository),
):
    """
    Calcula carga oculta estimada (geração solar não medida).

    - **subsistema**: Subsistema elétrico (SUDESTE, NORTE, NORDESTE, SUL)
    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        use_case = ObtenerCargaOcultaUseCase(repository=repo)
        return use_case.executar(subsistema, distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao calcular carga oculta: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular carga oculta") from exc


@router.get("/carga-atual-distribuidora")
def obter_carga_atual_distribuidora(
    distribuidora: DistribuidoraQuery,
):
    """
    Obtém a carga ATUAL (tempo real) da distribuidora com dados medidos.

    Retorna a última carga medida para a distribuidora especificada,
    com timestamp da medição.

    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, IENERGIA, etc)

    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "carga_ons_mw": 177.82,
      "data_medicao": "2026-02-05T22:59:48.735958",
      "subsistema": "Sudeste/Centro-Oeste",
      "status": "ok"
    }
    ```
    """
    try:
        from ..core.database import get_engine
        import pandas as pd
        from sqlalchemy import text
        
        engine = get_engine()
        
        # Query para obter última carga da distribuidora
        query = text(f"""
            SELECT DISTINCT ON (distribuidora)
                distribuidora,
                carga_mw,
                data_medicao,
                subsistema
            FROM carga_distribuidoras
            WHERE distribuidora = :dist
            ORDER BY distribuidora, data_medicao DESC
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            resultado = conn.execute(query, {"dist": distribuidora})
            row = resultado.fetchone()
        
        if not row:
            return {
                "distribuidora": distribuidora,
                "carga_ons_mw": 0.0,
                "data_medicao": None,
                "subsistema": None,
                "status": "Sem dados"
            }
        
        return {
            "distribuidora": row[0],
            "carga_ons_mw": float(row[1]),
            "data_medicao": row[2],
            "subsistema": row[3],
            "status": "ok"
        }
        
    except Exception as exc:
        logger.error(f"Erro ao obter carga atual distribuidora: {exc}", exc_info=True)
        return {
            "distribuidora": distribuidora,
            "carga_ons_mw": 0.0,
            "data_medicao": None,
            "subsistema": None,
            "status": f"Erro: {str(exc)}"
        }


@router.get("/carga-distribuidor-tempo-real", response_model=list[CargaDistribuidoraAtual])
def obter_carga_distribuidora_tempo_real(
    distribuidora: DistribuidoraQuery = None,
):
    """
    Obtém a carga em tempo real das distribuidoras (dados atualizados a cada hora).

    Esta é a verdadeira carga da distribuidora baseada em medições reais,
    não em estimativas. Os dados são atualizados via ETL a cada hora.

    **Parâmetros:**
    - **distribuidora**: Filtrar por distribuidora (opcional - retorna todas se vazio)

    **Resposta:**
    ```json
    [
      {
        "distribuidora": "LIGHT",
        "carga_mw": 450.5,
        "data_medicao": "2026-02-05T22:00:00",
        "subsistema": "Sudeste/Centro-Oeste"
      }
    ]
    ```
    """
    try:
        engine = get_engine()
        
        # Query para obter última carga de cada distribuidora
        if distribuidora:
            query = f"""
                SELECT DISTINCT ON (distribuidora)
                    distribuidora,
                    carga_mw,
                    data_medicao,
                    subsistema
                FROM carga_distribuidoras
                WHERE distribuidora = '{distribuidora}'
                ORDER BY distribuidora, data_medicao DESC
                LIMIT 1
            """
        else:
            query = """
                SELECT DISTINCT ON (distribuidora)
                    distribuidora,
                    carga_mw,
                    data_medicao,
                    subsistema
                FROM carga_distribuidoras
                ORDER BY distribuidora, data_medicao DESC
            """
        
        import pandas as pd
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return []
        
        # Converter para list de dicts com tipos corretos
        resultado = [
            {
                "distribuidora": row['distribuidora'],
                "carga_mw": float(row['carga_mw']),
                "data_medicao": row['data_medicao'],
                "subsistema": row['subsistema'],
            }
            for _, row in df.iterrows()
        ]
        
        return resultado

    except Exception as exc:
        logger.error(f"Erro ao obter carga distribuidor tempo real: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter carga em tempo real") from exc


@router.get("/classes-consumo", response_model=list[ClasseConsumoItem])
def get_classes_consumo(
    distribuidora: DistribuidoraQuery = None,
    repo=Depends(get_repository),
):
    """
    Retorna consumo por classe de consumidor.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        use_case = ObtenerClassesConsumoUseCase(repository=repo)
        return use_case.executar(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar classes de consumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar classes de consumo") from exc


@router.get("/alertas-fraude", response_model=AlertaFraude | dict)
def get_alertas_fraude(
    distribuidora: DistribuidoraQuery = None,
    repo=Depends(get_repository),
):
    """
    Retorna último alerta de fraude detectado.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        use_case = ObtenerAlertaFraudeUseCase(repository=repo)
        return use_case.executar(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar alertas de fraude: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar alertas de fraude") from exc


@router.get("/estabelecimentos/contagem", response_model=list[EstabelecimentoContagem])
def get_contagem_estabelecimentos(
    distribuidora: DistribuidoraQuery = None,
    repo=Depends(get_repository),
):
    """
    Retorna contagem de estabelecimentos por tipo.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        use_case = ObtenerContagemEstabelecimentosUseCase(repository=repo)
        return use_case.executar(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar contagem: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar contagem de estabelecimentos") from exc


@router.get("/estabelecimentos/resumo", response_model=ResumoGranular | dict)
def get_resumo_estabelecimentos(
    distribuidora: DistribuidoraQuery = None,
    repo=Depends(get_repository),
):
    """
    Retorna resumo geral dos dados granulares.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    """
    try:
        use_case = ObtenerResumoGranularUseCase(repository=repo)
        return use_case.executar(distribuidora)
    except Exception as exc:
        logger.error(f"Erro ao buscar resumo: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar resumo de estabelecimentos") from exc


@router.get("/perfis-carga", response_model=PerfisResponse)
def obter_perfis_carga(
    classes: str | None = None,
    repo=Depends(get_repository),
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
    # Parse classes
    classes_list = None
    if classes:
        classes_list = [c.strip() for c in classes.split(",")]

    try:
        use_case = ObtenerPerfisCargaUseCase(repository=repo)
        resultado = use_case.executar(classes_list)
        
        # Format response as list matching PerfisResponse schema
        perfis_list = []
        classes_disponiveis = []
        
        for perfil in resultado:
            classes_disponiveis.append(perfil.classe)
            perfis_list.append({
                "classe": perfil.classe,
                "curva": perfil.fatores_horarios,
                "hora_pico": perfil.pico_hora,
                "fator_pico": perfil.fator_pico,
                "hora_vale": perfil.minima_hora,
                "fator_vale": min(perfil.fatores_horarios) if perfil.fatores_horarios else 0.5,
                "amplitude": (max(perfil.fatores_horarios) - min(perfil.fatores_horarios)) if perfil.fatores_horarios else 0.5,
            })
        
        return {
            "perfis": perfis_list,
            "classes_disponiveis": classes_disponiveis
        }
    except Exception as exc:
        logger.error(f"Erro ao buscar perfis de carga: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar perfis de carga") from exc


@router.get("/estado-atual")
def obter_estado_atual(
    subsistema: SubsistemaQuery = "SUDESTE",
    distribuidora: DistribuidoraQuery = None,
    subestacao_id: int | None = None,
    repo=Depends(get_repository),
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
    try:
        use_case = ObtenerEstadoAtualUseCase(repository=repo)
        estado = use_case.executar(subsistema, distribuidora, subestacao_id)
        
        if estado:
            return {
                "timestamp": estado.timestamp.isoformat(),
                "hora_atual": estado.hora_atual,
                "estimativas": {
                    "carga_ons_mw": estado.carga_ons_mw,
                    "geracao_mmgd_mw": estado.geracao_mmgd_mw,
                    "consumo_estimado_mw": estado.consumo_estimado_mw,
                    "irradiancia_atual_wm2": estado.irradiancia_atual_wm2,
                }
            }
        return {}
    except Exception as exc:
        logger.error(f"Erro ao calcular estado atual: {exc}", exc_info=True)
        raise DatabaseError("Falha ao calcular estado atual") from exc


@router.get("/alertas-historico")
def obter_alertas_historico(
    distribuidora: str | None = None,
    dias: int = 30,
    limite: int = 50,
    repo=Depends(get_repository),
):
    """
    Retorna histórico de alertas de anomalias/fraudes.

    Combina:
    - Alertas manuais da auditoria_visual
    - Alertas gerados automaticamente por detecção de anomalias

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **dias**: Número de dias no histórico (padrão: 30)
    - **limite**: Máximo de alertas a retornar (padrão: 50)

    **Resposta**:
    ```json
    {
      "total": 45,
      "alertas": [
        {
          "id": 1,
          "data_deteccao": "2024-01-15T14:30:00",
          "distribuidora": "CPFL Paulista",
          "tipo": "consumo_baixo",
          "severidade": "alto",
          "descricao": "Consumo 45% abaixo do esperado",
          "status": "ativo",
          "impacto_kw": 125.5
        }
      ]
    }
    ```
    """
    try:
        use_case = ObtenerAlertasHistoricoUseCase(repository=repo)
        alertas = use_case.executar(distribuidora, dias, limite)

        return {
            "total": len(alertas),
            "periodo_dias": dias,
            "distribuidora": distribuidora or "Todas",
            "alertas": [
                {
                    "id": a.id,
                    "data_deteccao": a.data_deteccao.isoformat(),
                    "distribuidora": a.distribuidora,
                    "tipo": a.tipo,
                    "severidade": a.severidade,
                    "descricao": a.descricao,
                    "status": a.status,
                    "impacto_kw": a.impacto_kw,
                }
                for a in alertas
            ]
        }

    except Exception as exc:
        logger.error(f"Erro ao buscar histórico de alertas: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar histórico de alertas") from exc


@router.post("/detectar-anomalias")
def executar_deteccao_anomalias(
    distribuidora: str | None = None,
    limite: int = 10,
    repo=Depends(get_repository),
):
    """
    Executa detecção de anomalias em tempo real.

    Analisa dados atuais de consumo e identifica:
    - Desvios de consumo anormais
    - Padrões atípicos de carga
    - Fatores de carga suspeitos

    - **distribuidora**: Analisar distribuidora específica (opcional)
    - **limite**: Máximo de anomalias a retornar (padrão: 10)

    **Resposta**:
    ```json
    {
      "total_anomalias": 3,
      "anomalias": [
        {
          "distribuidora": "CEMIG",
          "tipo": "consumo_baixo",
          "severidade": "alto",
          "desvio_percentual": 45.2,
          "total_ucs_afetadas": 1250,
          "impacto_kw": 185.5
        }
      ]
    }
    ```
    """
    try:
        use_case = DetectarAnomalasUseCase(repository=repo)
        anomalias = use_case.executar(distribuidora, limite)

        return {
            "total_anomalias": len(anomalias),
            "distribuidora": distribuidora or "Todas",
            "timestamp": datetime.now().isoformat(),
            "anomalias": [
                {
                    "distribuidora": a.distribuidora,
                    "tipo": a.tipo,
                    "severidade": a.severidade,
                    "desvio_percentual": a.desvio_percentual,
                    "total_ucs_afetadas": a.total_ucs_afetadas,
                    "impacto_kw": a.impacto_kw,
                }
                for a in anomalias
            ]
        }

    except Exception as exc:
        logger.error(f"Erro ao detectar anomalias: {exc}", exc_info=True)
        raise DatabaseError("Falha ao detectar anomalias") from exc
