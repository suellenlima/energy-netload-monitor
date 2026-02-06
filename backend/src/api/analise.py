"""Endpoints para análise de carga e fraude."""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, Query

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
    CargaLiquidaONS,
    CargaTotalDistribuidora,
    ClasseConsumoItem,
    EstabelecimentoContagem,
    EstadoAtual,
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


@router.get("/distribuidoras", response_model=list[str])
def listar_distribuidoras_disponiveis(
    repo=Depends(get_repository),
):
    """
    Lista todas as distribuidoras com dados REAIS de MMGD na base.
    
    Útil para validação e autocomplete no frontend.
    Retorna nomes normalizados em UPPERCASE.
    """
    try:
        distribuidoras = repo.obter_distribuidoras_disponiveis()
        return distribuidoras
    except Exception as exc:
        logger.error(f"Erro ao listar distribuidoras: {exc}", exc_info=True)
        return []


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


@router.get("/carga-liquida-ons", response_model=CargaLiquidaONS)
def obter_carga_liquida_ons(
    distribuidora: DistribuidoraQuery,
):
    """
    Obtém a CARGA LÍQUIDA (ONS) de uma distribuidora.

    A carga líquida é o que o ONS "enxerga" nos pontos de entrega da transmissão.
    É a energia que vem da rede de transmissão (usinas hidro, térmicas, eólicas, solares centralizadas).

    **NÃO inclui**:
    - Geração distribuída (MMGD) consumida localmente
    - Painéis solares em residências/comércios

    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, IENERGIA, etc)

    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "carga_liquida_mw": 177.82,
      "data_medicao": "2026-02-05T22:59:48",
      "subsistema": "Sudeste/Centro-Oeste"
    }
    ```
    """
    try:
        from ..core.database import get_engine
        from sqlalchemy import text
        
        engine = get_engine()
        
        # Query para obter última carga líquida da distribuidora
        query = text("""
            SELECT DISTINCT ON (distribuidora)
                distribuidora,
                carga_liquida_mw,
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
            raise DatabaseError(f"Sem dados de carga líquida para {distribuidora}")
        
        return CargaLiquidaONS(
            distribuidora=row[0],
            carga_liquida_mw=float(row[1]),
            data_medicao=row[2],
            subsistema=row[3],
        )
        
    except Exception as exc:
        logger.error(f"Erro ao obter carga líquida ONS: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter carga líquida ONS") from exc


@router.get("/carga-total", response_model=CargaTotalDistribuidora)
def obter_carga_total(
    distribuidora: DistribuidoraQuery,
):
    """
    Obtém a CARGA TOTAL (Real) de uma distribuidora.

    A carga total é o consumo REAL de energia pelos consumidores finais.
    
    **Fórmula:**
    ```
    Carga Distribuidora = Carga Estimada (consumo granular) + Carga Líquida ONS
    ```

    Combina:
    - **Carga Estimada**: Consumo agregado por classe de consumidor (dados ANEEL)
    - **Carga Líquida ONS**: Energia que vem da rede de transmissão (dados ONS)
    - **Geração MMGD**: Painéis solares gerando localmente (dados estimados)

    **Exemplo:**
    - Carga Estimada (consumo granular): 250 MW (residencial + comercial + industrial)
    - Carga Líquida ONS: 177.82 MW (o que vem da rede agora)
    - Geração MMGD: 35.5 MW (painéis solares locais)
    - **Carga Distribuidora: 427.82 MW** (250 + 177.82)

    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, IENERGIA, etc)

    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "carga_liquida": 177.82,
      "geracao_mmgd": 35.5,
      "carga_distribuidora": 427.82,
      "percentual_mmgd": 19.97,
      "data_medicao": "2026-02-05T22:59:48",
      "subsistema": "Sudeste/Centro-Oeste"
    }
    ```
    """
    try:
        import re
        from ..core.database import get_engine
        from sqlalchemy import text
        
        engine = get_engine()
        
        # Normalizar nome da distribuidora (remover espaços, pontuação etc.)
        dist_pattern = f"%{re.sub('[^A-Z0-9]', '', distribuidora.upper())}%"
        
        # 1. Obter carga líquida (ONS) da distribuidora
        query_liquida = text("""
            SELECT DISTINCT ON (distribuidora)
                distribuidora,
                carga_liquida_mw,
                data_medicao,
                subsistema
            FROM carga_distribuidoras
            WHERE regexp_replace(UPPER(distribuidora), '[^A-Z0-9]', '', 'g') ILIKE :dist_pattern
            ORDER BY distribuidora, data_medicao DESC
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            resultado_liquida = conn.execute(query_liquida, {"dist_pattern": dist_pattern})
            row_liquida = resultado_liquida.fetchone()
        
        if not row_liquida:
            raise DatabaseError(f"Sem dados de carga líquida para {distribuidora}")
        
        carga_liquida = float(row_liquida[1])
        data_medicao = row_liquida[2]
        subsistema = row_liquida[3]
        
        # 2. Obter carga estimada da distribuidora (consumo granular por classe)
        # Somando consumo de todas as classes para essa distribuidora
        query_consumo = text("""
            SELECT COALESCE(SUM(consumo_kwh), 0) as consumo_total_kwh
            FROM consumo_granular_classe
            WHERE regexp_replace(UPPER(distribuidora), '[^A-Z0-9]', '', 'g') ILIKE :dist_pattern
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            resultado_consumo = conn.execute(query_consumo, {"dist_pattern": dist_pattern})
            row_consumo = resultado_consumo.fetchone()
        
        # Converter kWh/dia → MW (considerando 24 horas: kWh / 24000 = MW)
        carga_estimada_mw = (float(row_consumo[0]) / 24000.0) if row_consumo and row_consumo[0] else 0.0
        
        # 3. Obter a geração MMGD para o mesmo momento
        query_mmgd = text("""
            SELECT COALESCE(SUM(estimativa_solar_mw), 0) as geracao_mmgd
            FROM carga_oculta
            WHERE regexp_replace(UPPER(distribuidora), '[^A-Z0-9]', '', 'g') ILIKE :dist_pattern
            AND DATE_TRUNC('hour', hora) = DATE_TRUNC('hour', :data_medicao)
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            resultado_mmgd = conn.execute(query_mmgd, {"dist_pattern": dist_pattern, "data_medicao": data_medicao})
            row_mmgd = resultado_mmgd.fetchone()
        
        geracao_mmgd = float(row_mmgd[0]) if row_mmgd and row_mmgd[0] else 0.0
        
        # 4. Calcular carga total: carga estimada + carga líquida
        # (A carga estimada ANEEL é o que a distribuidora pode consumir)
        # (A carga líquida ONS é o que ela está consumindo da rede naquele momento)
        # Interpretação: carga_distribuidora = max(carga_estimada, carga_liquida + geracao_mmgd)
        carga_distribuidora = carga_estimada_mw + carga_liquida
        
        # Calcular percentual de MMGD em relação à carga líquida
        percentual_mmgd = (geracao_mmgd / carga_liquida * 100) if carga_liquida > 0 else 0.0
        
        return CargaTotalDistribuidora(
            distribuidora=row_liquida[0],
            carga_liquida=carga_liquida,
            geracao_mmgd=geracao_mmgd,
            carga_distribuidora=round(carga_distribuidora, 2),
            percentual_mmgd=round(percentual_mmgd, 2),
            data_medicao=data_medicao,
            subsistema=subsistema,
        )
        
    except Exception as exc:
        logger.error(f"Erro ao obter carga total: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter carga total") from exc


@router.get("/carga-atual-distribuidora", response_model=CargaDistribuidoraAtual)
def obter_carga_atual_distribuidora(
    distribuidora: DistribuidoraQuery,
):
    """
    Obtém a carga LÍQUIDA (ONS) ATUAL (tempo real) da distribuidora com dados medidos.

    Retorna a última carga líquida medida para a distribuidora especificada,
    com timestamp da medição.

    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, IENERGIA, etc)

    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "carga_mw": 177.82,
      "data_medicao": "2026-02-05T22:59:48.735958",
      "subsistema": "Sudeste/Centro-Oeste"
    }
    ```
    """
    try:
        from ..core.database import get_engine
        from sqlalchemy import text
        from datetime import datetime
        
        engine = get_engine()
        
        # Query para obter última carga da distribuidora (priorizar registros com carga_estimada_total_mw)
        query = text("""
            SELECT DISTINCT ON (distribuidora)
                distribuidora,
                COALESCE(carga_estimada_total_mw, carga_liquida_mw) as carga_total,
                carga_liquida_mw,
                data_medicao,
                subsistema
            FROM carga_distribuidoras
            WHERE distribuidora = :dist
            ORDER BY distribuidora, 
                     (carga_estimada_total_mw IS NOT NULL) DESC,  -- Priorizar registros com este campo
                     data_medicao DESC
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            resultado = conn.execute(query, {"dist": distribuidora})
            row = resultado.fetchone()
        
        if not row:
            # Retornar valor padrão válido se não houver dados
            return CargaDistribuidoraAtual(
                distribuidora=distribuidora,
                carga_granular_mw=0.0,
                carga_liquida_mw=0.0,
                carga_total_mw=0.0,
                data_medicao=datetime.now(),
                subsistema=None
            )
        
        # Calcular carga granular como: total - líquida
        carga_total = float(row[1]) if row[1] else 0.0
        carga_liquida = float(row[2]) if row[2] else 0.0
        carga_granular = carga_total - carga_liquida
        
        return CargaDistribuidoraAtual(
            distribuidora=row[0],
            carga_granular_mw=max(0.0, carga_granular),  # Garantir >= 0
            carga_liquida_mw=carga_liquida,
            carga_total_mw=carga_total,
            data_medicao=row[3] if row[3] else datetime.now(),
            subsistema=row[4]
        )
        
    except Exception as exc:
        logger.error(f"Erro ao obter carga atual distribuidora: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter carga atual distribuidora") from exc


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
                    carga_liquida_mw,
                    carga_estimada_total_mw,
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
                    carga_liquida_mw,
                    carga_estimada_total_mw,
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
                "carga_mw": float(row['carga_liquida_mw']),
                "carga_total_mw": float(row['carga_estimada_total_mw']) if row['carga_estimada_total_mw'] else float(row['carga_liquida_mw']),
                "data_medicao": row['data_medicao'],
                "subsistema": row['subsistema'],
            }
            for _, row in df.iterrows()
        ]
        
        return resultado

    except Exception as exc:
        logger.error(f"Erro ao obter carga distribuidor tempo real: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter carga em tempo real") from exc


@router.get("/carga-distribuidor-historico")
def obter_carga_distribuidor_historico(
    distribuidora: DistribuidoraQuery,
    subsistema: SubsistemaQuery = None,
    dias: int = 7,
    repo=Depends(get_repository),
):
    """
    Obtém o histórico de carga por distribuidora.

    Retorna os dados de carga armazenados na tabela carga_distribuidoras
    para análise de tendências históricas.

    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (obrigatório)
    - **subsistema**: Filtrar por subsistema (opcional)
    - **dias**: Número de dias de histórico a retornar (padrão: 7)

    **Resposta:**
    ```json
    [
      {
        "hora": "2026-02-05T22:00:00",
        "carga_ons": 177.82,
        "distribuidora": "LIGHT",
        "subsistema": "Sudeste/Centro-Oeste"
      }
    ]
    ```
    """
    try:
        if not distribuidora:
            raise ValueError("Distribuidora é obrigatória")
        
        return repo.obter_carga_distribuidor_historico(distribuidora, subsistema, dias)

    except Exception as exc:
        logger.error(f"Erro ao obter histórico de carga: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter histórico de carga") from exc


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


@router.get("/carga-por-classe", response_model=list)
def get_carga_por_classe(
    distribuidora: DistribuidoraQuery = None,
    limite: int = 288,
    repo=Depends(get_repository),
):
    """
    Retorna série temporal de carga por classe de consumo.
    
    Útil para exibir no gráfico comparativo como linhas separadas por classe.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **limite**: Número máximo de registros (padrão: 288 = 12 dias em frequência horária)
    """
    try:
        return repo.obter_carga_por_classe(distribuidora, limite)
    except Exception as exc:
        logger.error(f"Erro ao buscar carga por classe: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar carga por classe") from exc


@router.get("/carga-distribuidora-horaria", response_model=list)
def get_carga_distribuidora_horaria(
    distribuidora: DistribuidoraQuery = None,
    limite: int = 288,
    repo=Depends(get_repository),
):
    """
    Retorna série temporal de carga real por distribuidora (dados de carga_distribuidoras).
    
    Útil para comparar carga líquida (medida real) vs consumo estimado.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **limite**: Número máximo de registros (padrão: 288 = 12 dias horários)
    """
    try:
        return repo.obter_carga_distribuidora_horaria(distribuidora, limite)
    except Exception as exc:
        logger.error(f"Erro ao buscar carga distribuidora horária: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar carga distribuidora horária") from exc


@router.get("/curva-pato", response_model=list)
def get_curva_pato(
    distribuidora: DistribuidoraQuery = None,
    dias: int = 30,
    repo=Depends(get_repository),
):
    """
    Retorna a "curva de pato" - padrão de carga por hora do dia.
    
    Mostra como a demanda varia ao longo de um dia típico, agregando dados de múltiplos dias.
    Útil para identificar picos de demanda e efeitos da geração solar.

    - **distribuidora**: Filtrar por distribuidora (opcional)
    - **dias**: Número de dias para agregar (padrão: 30)
    """
    try:
        return repo.obter_curva_pato(distribuidora, dias)
    except Exception as exc:
        logger.error(f"Erro ao buscar curva de pato: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar curva de pato") from exc


@router.get("/carga-ons-realtime", response_model=list)
def get_carga_ons_realtime(
    subsistema: str = None,
    distribuidora: str = None,
    limite: int = 288,
    repo=Depends(get_repository),
):
    """
    Retorna dados de carga do ONS em tempo real.
    
    Dados reais do Sistema Interligado Nacional (SIN).

    - **subsistema**: Filtrar por subsistema (SUDESTE, SUL, NORDESTE, NORTE) - opcional
    - **distribuidora**: Filtrar por distribuidora (LIGHT, ENEL, etc.) - opcional
    - **limite**: Número máximo de registros (padrão: 288 = 12 dias horários)
    """
    try:
        return repo.obter_carga_ons_realtime(subsistema, distribuidora, limite)
    except Exception as exc:
        logger.error(f"Erro ao buscar carga ONS realtime: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar carga ONS realtime") from exc


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
        resumo = use_case.executar(distribuidora)
        # Garantir que a resposta seja serializável pelo Pydantic
        if resumo is None:
            # Retornar dicionário vazio conforme response_model permite `dict`
            return {}
        # Se for um objeto ResumoGranular, retorná-lo diretamente
        return resumo
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


@router.get("/estado-atual", response_model=EstadoAtual)
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
            return EstadoAtual(
                timestamp=estado.timestamp,
                hora_atual=estado.hora_atual,
                estimativas={
                    "carga_ons_mw": estado.carga_ons_mw,
                    "geracao_mmgd_mw": estado.geracao_mmgd_mw,
                    "consumo_estimado_mw": estado.consumo_estimado_mw,
                    "irradiancia_atual_wm2": estado.irradiancia_atual_wm2,
                }
            )
        
        # Se não houver dados, retornar estimativa padrão
        return EstadoAtual(
            timestamp=datetime.now(),
            hora_atual=datetime.now().hour,
            estimativas={
                "carga_ons_mw": 0.0,
                "geracao_mmgd_mw": 0.0,
                "consumo_estimado_mw": 0.0,
                "irradiancia_atual_wm2": 0.0,
            }
        )
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

@router.get("/gd-granular", response_model=dict)
def get_gd_granular(
    distribuidora: str = None,
    tipo_estabelecimento: str = None,
    engine=Depends(get_engine),
):
    """
    Retorna dados GRANULARES de geração distribuída diretamente de gd_granular.
    
    Filtra por distribuidora (com fuzzy matching robusto) e opcionalmente por tipo de estabelecimento.
    
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL-SP, etc) - fuzzy matching
    - **tipo_estabelecimento**: residencia, comercio, industria, etc (opcional)
    """
    try:
        import re
        from sqlalchemy import text
        
        with engine.connect() as conn:
            # Normalizar nome da distribuidora para fuzzy matching
            if distribuidora:
                dist_pattern = f"%{re.sub('[^A-Z0-9]', '', distribuidora.upper())}%"
                base_query = """
                    SELECT 
                        distribuidora,
                        tipo_estabelecimento,
                        COUNT(*) as quantidade,
                        SUM(potencia_kw) as potencia_total_kw
                    FROM gd_granular
                    WHERE regexp_replace(UPPER(distribuidora), '[^A-Z0-9]', '', 'g') ILIKE :dist_pattern
                """
                params = {"dist_pattern": dist_pattern}
            else:
                base_query = """
                    SELECT 
                        distribuidora,
                        tipo_estabelecimento,
                        COUNT(*) as quantidade,
                        SUM(potencia_kw) as potencia_total_kw
                    FROM gd_granular
                """
                params = {}
            
            # Adicionar filtro por tipo de estabelecimento se fornecido
            if tipo_estabelecimento:
                base_query += " AND tipo_estabelecimento = :tipo"
                params["tipo"] = tipo_estabelecimento
            
            base_query += " GROUP BY distribuidora, tipo_estabelecimento ORDER BY quantidade DESC LIMIT 1000"
            
            result = conn.execute(text(base_query), params)
            rows = result.fetchall()
            
            # Retornar dados em formato estruturado
            dados = []
            for row in rows:
                dados.append({
                    "distribuidora": row[0],
                    "tipo_estabelecimento": row[1],
                    "quantidade": row[2],
                    "potencia_total_kw": float(row[3]) if row[3] else 0,
                    "potencia_total_mw": float(row[3] / 1000) if row[3] else 0,
                })
            
            # Resumo geral
            if distribuidora:
                resumo_query = text("""
                    SELECT 
                        COUNT(*) as total_ucs,
                        SUM(potencia_kw) as potencia_total_kw
                    FROM gd_granular
                    WHERE regexp_replace(UPPER(distribuidora), '[^A-Z0-9]', '', 'g') ILIKE :dist_pattern
                """)
                resumo_result = conn.execute(resumo_query, {"dist_pattern": dist_pattern})
            else:
                resumo_query = text("""
                    SELECT 
                        COUNT(*) as total_ucs,
                        SUM(potencia_kw) as potencia_total_kw
                    FROM gd_granular
                """)
                resumo_result = conn.execute(resumo_query)
            
            resumo_row = resumo_result.fetchone()
            
            return {
                "distribuidora": distribuidora or "Todas",
                "resumo": {
                    "total_ucs": resumo_row[0] if resumo_row else 0,
                    "potencia_total_kw": float(resumo_row[1]) if resumo_row and resumo_row[1] else 0,
                    "potencia_total_mw": float(resumo_row[1] / 1000) if resumo_row and resumo_row[1] else 0,
                },
                "dados": dados,
                "total_registros": len(dados)
            }
        
    except Exception as exc:
        logger.error(f"Erro ao buscar GD granular: {exc}", exc_info=True)
        raise DatabaseError("Falha ao buscar dados granulares") from exc


@router.get("/mmgd-detectada")
def obter_mmgd_detectada(
    distribuidora: DistribuidoraQuery,
):
    """
    Obtém potência MMGD detectada na área da distribuidora.
    
    Retorna a potência instalada de painéis solares detectados/estimados
    com base em:
    1. Painéis detectados (tabela paineis_solares_detectados)
    2. GD granular da ANEEL (tabela gd_granular) 
    3. Estimativa por transformadores (densidade típica)
    
    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, etc)
    
    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "potencia_detectada_mw": 156.5,
      "paineis_detectados": 1234,
      "fonte_dados": "gd_granular",
      "data_calculo": "2026-02-06T23:00:00"
    }
    ```
    """
    try:
        from ..core.database import get_engine
        from sqlalchemy import text
        from datetime import datetime
        
        engine = get_engine()
        
        with engine.connect() as conn:
            # 1. Tentar buscar painéis detectados primeiro
            query_paineis = text("""
                SELECT 
                    COUNT(*) as total_paineis,
                    COALESCE(SUM(potencia_w), 0) / 1000000.0 as potencia_mw
                FROM paineis_solares_detectados psd
                INNER JOIN transformadores_aneel ta ON psd.transformador_id = ta.id
                WHERE UPPER(ta.distribuidora) = UPPER(:dist)
                AND psd.potencia_w > 0
            """)
            
            result_paineis = conn.execute(query_paineis, {"dist": distribuidora})
            row_paineis = result_paineis.fetchone()
            
            if row_paineis and row_paineis[0] > 0:
                # Dados de detecção visual disponíveis
                return {
                    "distribuidora": distribuidora,
                    "potencia_detectada_mw": float(row_paineis[1]),
                    "paineis_detectados": int(row_paineis[0]),
                    "fonte_dados": "deteccao_visual",
                    "data_calculo": datetime.now()
                }
            
            # 2. Fallback: buscar GD granular da ANEEL
            query_gd = text("""
                SELECT 
                    COUNT(*) as total_ucs,
                    COALESCE(SUM(potencia_kw), 0) / 1000.0 as potencia_mw
                FROM gd_granular
                WHERE UPPER(distribuidora) ILIKE UPPER(:dist || '%')
            """)
            
            result_gd = conn.execute(query_gd, {"dist": distribuidora})
            row_gd = result_gd.fetchone()
            
            if row_gd and row_gd[0] > 0:
                # Dados GD granular ANEEL disponíveis
                return {
                    "distribuidora": distribuidora,
                    "potencia_detectada_mw": float(row_gd[1]),
                    "paineis_detectados": int(row_gd[0]),
                    "fonte_dados": "gd_granular_aneel",
                    "data_calculo": datetime.now()
                }
            
            # 3. Fallback final: estimativa por transformadores
            # Densidade típica: 5 kW por transformador (média residencial/comercial)
            query_trafo = text("""
                SELECT COUNT(*) as total_trafos
                FROM transformadores_aneel
                WHERE UPPER(distribuidora) = UPPER(:dist)
            """)
            
            result_trafo = conn.execute(query_trafo, {"dist": distribuidora})
            row_trafo = result_trafo.fetchone()
            
            num_trafos = row_trafo[0] if row_trafo else 0
            potencia_estimada_mw = (num_trafos * 5) / 1000.0  # 5 kW/trafo
            
            return {
                "distribuidora": distribuidora,
                "potencia_detectada_mw": float(potencia_estimada_mw),
                "paineis_detectados": num_trafos,  # Estimativa: 1 painel por trafo
                "fonte_dados": "estimativa_transformadores",
                "data_calculo": datetime.now()
            }
            
    except Exception as exc:
        logger.error(f"Erro ao obter MMGD detectada: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter MMGD detectada") from exc


@router.get("/mmgd-detectada-paineis")
def obter_mmgd_detectada_paineis(
    distribuidora: str = Query(..., description="Nome da distribuidora")
):
    """
    Obtém a potência total de painéis solares detectados na área da distribuidora.
    
    Retorna dados reais da tabela paineis_solares_detectados agregados por distribuidora.
    
    **Parâmetros:**
    - **distribuidora**: Nome da distribuidora (LIGHT, ENEL, IENERGIA, etc)
    
    **Resposta:**
    ```json
    {
      "distribuidora": "LIGHT",
      "potencia_detectada_mw": 45.8,
      "paineis_detectados": 1234,
      "area_total_m2": 98500.5,
      "confianca_media": 0.87,
      "fonte_dados": "paineis_solares_detectados",
      "data_calculo": "2026-02-06T22:50:00"
    }
    ```
    """
    try:
        from ..core.database import get_engine
        from sqlalchemy import text
        from datetime import datetime
        
        engine = get_engine()
        
        # Query para buscar painéis detectados da distribuidora (usando ILIKE para busca parcial)
        query = text("""
            SELECT 
                COUNT(psd.id) as total_paineis,
                COALESCE(SUM(psd.potencia_w), 0) / 1000000.0 as potencia_mw,
                COALESCE(SUM(psd.area_m2), 0) as area_total_m2,
                COALESCE(AVG(psd.confianca), 0) as confianca_media
            FROM paineis_solares_detectados psd
            INNER JOIN transformadores_aneel ta ON psd.transformador_id = ta.id
            WHERE UPPER(ta.distribuidora) LIKE UPPER(:dist)
            AND psd.potencia_w > 0
        """)
        
        # Adiciona % ao final para busca parcial (ex: "LIGHT" encontra "LIGHT SERVICOS...")
        dist_param = f"{distribuidora}%"
        
        with engine.connect() as conn:
            resultado = conn.execute(query, {"dist": dist_param})
            row = resultado.fetchone()
        
        if not row or row[0] == 0:
            # Retornar zeros se não houver dados
            return {
                "distribuidora": distribuidora,
                "potencia_detectada_mw": 0.0,
                "paineis_detectados": 0,
                "area_total_m2": 0.0,
                "confianca_media": 0.0,
                "fonte_dados": "paineis_solares_detectados",
                "mensagem": "Nenhum painel detectado ainda",
                "data_calculo": datetime.now()
            }
        
        return {
            "distribuidora": distribuidora,
            "potencia_detectada_mw": float(row[1]),
            "paineis_detectados": int(row[0]),
            "area_total_m2": float(row[2]),
            "confianca_media": round(float(row[3]), 2) if row[3] else 0.0,
            "fonte_dados": "paineis_solares_detectados",
            "data_calculo": datetime.now()
        }
            
    except Exception as exc:
        logger.error(f"Erro ao obter MMGD detectada de painéis: {exc}", exc_info=True)
        raise DatabaseError("Falha ao obter MMGD detectada de painéis") from exc