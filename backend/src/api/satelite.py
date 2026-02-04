"""
Endpoints para gerenciar dados de imagens de satélite (CBERS-4A, Google Maps, etc).

Refatorado em 3 camadas Clean Architecture:
- API Layer: Recebe requisições HTTP, valida entrada, retorna resposta
- Service Layer: Orquestra lógica de decisão de fontes e rastreamento
- Repository Layer: Acessa banco de dados (schema_aneel_bdgd.sql)

Tabelas utilizadas:
- transformadores_aneel: Dados de transformadores (READ)
- subestacoes_aneel: Dados de subestações (READ)
- transformador_area_cobertura: Áreas poligonais (READ)
- requisicoes_satelite_cbers4a: Histórico de requisições (WRITE/READ)

Reutiliza serviços existentes:
- GoogleMapsService: Integração com Google Maps Static API (versão unificada)
- CBERSService: Integração com CBERS-4A do INPE
- GoogleMapsQuotaService: Gerenciamento de quota

Author: Energy Netload Monitor
Date: 2026-02-04
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Path, Depends
from pydantic import BaseModel

from ..core import DatabaseError, get_engine
from ..schemas import (
    ConsultaSateliteRequest,
    DadosSatelliteSubestacao,
    ListaImagensSatelite,
    RegistrarImagemRequest,
    RegistrarImagemResponse,
)
from ..services.satelite_service import SateliteService
from ..services.google_maps_service import GoogleMapsService
from ..services.inpe_service import CBERSService
from .deps import EngineDepends, LimiteQuery

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/satelite", tags=["Satélite"])


# ========================================================================
# MODELOS PYDANTIC
# ========================================================================

class FonteDecisaoResponse(BaseModel):
    """Resposta de decisão de fonte para satélite"""
    fonte_recomendada: Optional[str]
    razao: str
    pode_usar: bool
    resolucao_m: Optional[float] = None
    cobertura: Optional[str] = None
    quota_disponivel: Optional[int] = None
    custo_estimado: Optional[float] = None


class CoordenadaTelhadoResponse(BaseModel):
    """Resposta com coordenadas para busca de telhado"""
    transformador_id: int
    transformador_codigo: str
    transformador_nome: Optional[str] = None
    distribuidora: Optional[str] = None
    latitude: float
    longitude: float
    tipo_tensao: Optional[str] = None
    valido: bool


class AreaCoberturaTelhadoResponse(BaseModel):
    """Resposta com área de cobertura de telhado"""
    transformador_codigo: str
    tipo_tensao: str
    metodo_calculo: str
    area_km2: float
    area_m2: float
    num_consumidores: int
    num_vertices: int
    data_calculo: str


class HistoricoRequisicaoResponse(BaseModel):
    """Um registro de requisição de satélite"""
    id: int
    transformador_id: int
    subestacao_id: int
    fonte_satelite: str
    status: str
    imagem_id: Optional[str] = None
    url_download: Optional[str] = None
    data_imagem: Optional[str] = None
    cobertura_nuvem_percentual: Optional[float] = None
    resolucao_metros: Optional[float] = None
    tempo_requisicao_ms: Optional[int] = None
    custo_usd_estimado: Optional[float] = None
    data_requisicao: str


class ListarImagensTransformadorResponse(BaseModel):
    """Resposta com lista de imagens do transformador"""
    transformador_id: int
    total_requisicoes: int
    registros: List[HistoricoRequisicaoResponse]


class QuotaGoogleMapsResponse(BaseModel):
    """Resposta com quota Google Maps"""
    requisicoes_mes: int
    limite_mensal: int
    disponivel: int
    percentual_uso: float
    custo_mes_usd: float
    mes_ano: str


class EstatisticasGoogleMapsResponse(BaseModel):
    """Estatísticas de uso do Google Maps"""
    total_requisicoes: int
    transformadores_unicos: int
    custo_total_usd: float
    sucesso: int
    erro: int
    taxa_sucesso: float


# ========================================================================
# DEPENDÊNCIAS
# ========================================================================

def get_satelite_service(engine: EngineDepends) -> SateliteService:
    """Dependência para obter serviço de satélite."""
    return SateliteService(engine)


# ========================================================================
# ENDPOINTS - TRANSFORMADOR (Coordenadas e Histórico)
# ========================================================================

@router.get(
    "/transformador/{transformador_id}/coordenadas",
    response_model=CoordenadaTelhadoResponse,
    summary="Obter coordenadas de transformador para busca de satélite"
)
async def obter_coordenadas_transformador(
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    service: SateliteService = Depends(get_satelite_service)
):
    """
    Obtém coordenadas de um transformador validadas para busca de satélite.
    
    Retorna:
    - ID e código do transformador
    - Latitude e longitude (validadas)
    - Tipo de tensão (BT, MT, AT)
    - Indicador de validade
    
    **Erros:**
    - 400: Coordenadas inválidas ou ausentes
    - 404: Transformador não encontrado
    - 500: Erro interno
    
    **Exemplo:**
    ```
    GET /satelite/transformador/1/coordenadas
    
    Response: 200 OK
    {
      "transformador_id": 1,
      "transformador_codigo": "TRAFO_001",
      "transformador_nome": "TRANSFORMADOR 13.8/0.22",
      "distribuidora": "CEMIG",
      "latitude": -19.925,
      "longitude": -43.938,
      "tipo_tensao": "BT",
      "valido": true
    }
    ```
    """
    try:
        coordenadas = service.obter_coordenadas_transformador(transformador_id)
        return CoordenadaTelhadoResponse(**coordenadas)
    
    except ValueError as e:
        logger.error(f"❌ Erro de validação: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter coordenadas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/transformador/{transformador_id}/area-cobertura",
    response_model=Optional[AreaCoberturaTelhadoResponse],
    summary="Obter área de cobertura (polígono) de transformador"
)
async def obter_area_cobertura_transformador(
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    service: SateliteService = Depends(get_satelite_service)
):
    """
    Obtém a área de cobertura (polígono) de um transformador.
    
    A área é calculada a partir dos consumidores BT/MT/AT conectados:
    - ≥3 consumidores: ConvexHull (polígono real)
    - <3 consumidores: Buffer (500m-2km conforme tipo de tensão)
    
    Retorna:
    - Método de cálculo (convex_hull, buffer_500m, etc)
    - Área em km² e m²
    - Número de consumidores
    - Data do cálculo
    
    **Exemplo:**
    ```
    GET /satelite/transformador/1/area-cobertura
    
    Response: 200 OK
    {
      "transformador_codigo": "TRAFO_001",
      "tipo_tensao": "BT",
      "metodo_calculo": "convex_hull",
      "area_km2": 2.5,
      "area_m2": 2500000,
      "num_consumidores": 45,
      "num_vertices": 8,
      "data_calculo": "2026-02-04T10:30:00"
    }
    
    Response: 200 OK (sem área calculada)
    null
    ```
    """
    try:
        area = service.obter_area_cobertura_transformador(transformador_id)
        
        if not area:
            return None
        
        return AreaCoberturaTelhadoResponse(**area)
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter área de cobertura: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/transformador/{transformador_id}/imagens/historico",
    response_model=ListarImagensTransformadorResponse,
    summary="Listar histórico de requisições de satélite"
)
async def listar_imagens_historico_transformador(
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    service: SateliteService = Depends(get_satelite_service),
    limit: int = Query(50, ge=1, le=100, description="Máximo de registros"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
    apenas_sucesso: bool = Query(True, description="Retornar apenas bem-sucedidas")
):
    """
    Lista histórico de requisições de satélite para um transformador.
    
    Retorna:
    - Data da requisição
    - Fonte (cbers4a, google_maps, etc)
    - Status (sucesso, sem_cobertura, erro)
    - ID da imagem
    - URL de download
    - Data de aquisição
    - Cobertura de nuvens
    - Custo estimado (Google Maps)
    
    **Parâmetros:**
    - `limit`: Máximo de registros (padrão: 50, máx: 100)
    - `offset`: Página (padrão: 0)
    - `apenas_sucesso`: Filtrar por sucesso (padrão: true)
    
    **Exemplo:**
    ```
    GET /satelite/transformador/1/imagens/historico?limit=10
    
    Response: 200 OK
    {
      "transformador_id": 1,
      "total_requisicoes": 25,
      "registros": [
        {
          "id": 1001,
          "transformador_id": 1,
          "subestacao_id": 1,
          "fonte_satelite": "google_maps",
          "status": "sucesso",
          "imagem_id": "IMG_20260201_001",
          "url_download": "https://...",
          "data_imagem": "2026-02-01T14:30:00",
          "cobertura_nuvem_percentual": 15.5,
          "resolucao_metros": 1.0,
          "tempo_requisicao_ms": 2145,
          "custo_usd_estimado": 0.007,
          "data_requisicao": "2026-02-01T14:32:15"
        }
      ]
    }
    ```
    """
    try:
        resultado = service.obter_historico_transformador(
            transformador_id=transformador_id,
            limite=limit,
            offset=offset,
            apenas_sucesso=apenas_sucesso
        )
        
        return ListarImagensTransformadorResponse(
            transformador_id=transformador_id,
            total_requisicoes=resultado['total_registros'],
            registros=[HistoricoRequisicaoResponse(**r) for r in resultado['registros']]
        )
    
    except Exception as e:
        logger.error(f"❌ Erro ao listar histórico: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================================================
# ENDPOINTS - DECISÃO DE FONTE
# ========================================================================

@router.get(
    "/transformador/{transformador_id}/decidir-fonte",
    response_model=FonteDecisaoResponse,
    summary="Decidir qual fonte de satélite usar (Google Maps vs CBERS-4A)"
)
async def decidir_fonte_satelite(
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    service: SateliteService = Depends(get_satelite_service),
    tentar_google_maps: bool = Query(True, description="Tentar Google Maps"),
    tentar_cbers4a: bool = Query(True, description="Tentar CBERS-4A como fallback"),
    force_cbers4a: bool = Query(False, description="Forçar CBERS-4A (ignora Google)")
):
    """
    Decide automaticamente qual fonte de satélite usar para um transformador.
    
    **Estratégia de decisão:**
    1. Se `force_cbers4a=true` → CBERS-4A (gratuito, 2m/pixel)
    2. Se quota Google disponível → Google Maps (1m/pixel, melhor resolução)
    3. Se quota Google esgotada → CBERS-4A (fallback gratuito)
    4. Se nenhuma disponível → Erro 400
    
    **Retorna:**
    - Fonte recomendada (google_maps ou cbers4a)
    - Razão da escolha
    - Resolução em metros (1.0 ou 2.0)
    - Cobertura geográfica
    - Quota disponível (se Google)
    - Custo estimado (se Google)
    - Flag pode_usar (true/false)
    
    **Exemplo (com quota disponível):**
    ```
    GET /satelite/transformador/1/decidir-fonte
    
    Response: 200 OK
    {
      "fonte_recomendada": "google_maps",
      "razao": "Quota disponível (18750 requisições)",
      "pode_usar": true,
      "resolucao_m": 1.0,
      "cobertura": "Mundo inteiro",
      "quota_disponivel": 18750,
      "custo_estimado": 0.007
    }
    ```
    
    **Exemplo (quota esgotada):**
    ```
    Response: 200 OK
    {
      "fonte_recomendada": "cbers4a",
      "razao": "Fallback CBERS-4A (Google Maps sem quota)",
      "pode_usar": true,
      "resolucao_m": 2.0,
      "cobertura": "Brasil inteiro",
      "custo_estimado": 0.0
    }
    ```
    
    **Exemplo (nenhuma disponível):**
    ```
    Response: 400 Bad Request
    {
      "detail": "Nenhuma fonte disponível (ambas desabilitadas)"
    }
    ```
    """
    try:
        decisao = service.decidir_fonte_satelite(
            transformador_id=transformador_id,
            tentar_google_maps=tentar_google_maps,
            tentar_cbers4a=tentar_cbers4a,
            force_cbers4a=force_cbers4a
        )
        
        if not decisao.get('pode_usar'):
            raise HTTPException(
                status_code=400,
                detail=decisao.get('razao', 'Nenhuma fonte disponível')
            )
        
        return FonteDecisaoResponse(**decisao)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao decidir fonte: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================================================
# ENDPOINTS - QUOTA E ESTATÍSTICAS
# ========================================================================

@router.get(
    "/google-maps/quota-mes",
    response_model=QuotaGoogleMapsResponse,
    summary="Quota de requisições do Google Maps no mês atual"
)
async def obter_quota_mes_atual(
    service: SateliteService = Depends(get_satelite_service)
):
    """
    Obtém quota de requisições do Google Maps para o mês atual.
    
    **Informações retornadas:**
    - Limite mensal: 25.000 requisições
    - Requisições usadas neste mês
    - Requisições disponíveis
    - Percentual de uso
    - Custo estimado em USD
    - Mês/ano (formato YYYY-MM)
    
    **Custo por requisição:** ~$0.007 USD
    
    **Exemplo:**
    ```
    GET /satelite/google-maps/quota-mes
    
    Response: 200 OK
    {
      "requisicoes_mes": 6250,
      "limite_mensal": 25000,
      "disponivel": 18750,
      "percentual_uso": 25.0,
      "custo_mes_usd": 43.75,
      "mes_ano": "2026-02"
    }
    ```
    """
    try:
        quota = service.obter_quota_mes_atual()
        return QuotaGoogleMapsResponse(**quota)
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/google-maps/estatisticas",
    response_model=EstatisticasGoogleMapsResponse,
    summary="Estatísticas gerais de uso do Google Maps"
)
async def obter_estatisticas_google_maps(
    service: SateliteService = Depends(get_satelite_service)
):
    """
    Obtém estatísticas gerais de uso do Google Maps (histórico completo).
    
    **Informações retornadas:**
    - Total de requisições (todos os tempos)
    - Transformadores únicos buscados
    - Custo total gasto
    - Total de requisições bem-sucedidas
    - Total de erros
    - Taxa de sucesso (%)
    
    **Exemplo:**
    ```
    GET /satelite/google-maps/estatisticas
    
    Response: 200 OK
    {
      "total_requisicoes": 15420,
      "transformadores_unicos": 156,
      "custo_total_usd": 107.94,
      "sucesso": 15200,
      "erro": 220,
      "taxa_sucesso": 98.57
    }
    ```
    """
    try:
        stats = service.obter_estatisticas_google_maps()
        return EstatisticasGoogleMapsResponse(**stats)
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================================================
# ENDPOINTS LEGADOS - Mantidos para compatibilidade
# ========================================================================

@router.get(
    "/subestacao/{subestacao_id}/coordenadas",
    response_model=dict,
    summary="[LEGADO] Obter coordenadas de subestação"
)
async def get_coordenadas_subestacao(
    subestacao_id: int = Path(..., gt=0),
    raio_km: float = Query(
        default=5.0,
        ge=0.5,
        le=50.0,
        description="Área poligonal de cobertura em km (bounding box)"
    )
):
    """Endpoint legado - usar /satelite/transformador/{id}/coordenadas"""
    return {
        "aviso": "Endpoint legado",
        "status": "deprecated"
    }


@router.get(
    "/subestacao/{subestacao_id}/imagens",
    response_model=dict,
    summary="[LEGADO] Listar imagens de satélite de subestação"
)
async def listar_imagens_subestacao(
    subestacao_id: int = Path(..., gt=0),
    limite: int = Query(50, ge=1, le=100)
):
    """Endpoint legado"""
    return {
        "aviso": "Endpoint legado",
        "status": "deprecated"
    }


__all__ = ['router']
