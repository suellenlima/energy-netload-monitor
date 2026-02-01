"""
Endpoints para Satélites V2 - Subestação e Transformador
Integra SatelliteServiceV2 e INPEServiceV2

Novos endpoints:
- GET /satelite/v2/transformador/{id}/imagens
- GET /satelite/v2/transformador/{id}/fonte
- GET /satelite/v2/transformador/multiplos/imagens
- GET /satelite/v2/quota/google-maps
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime

from fastapi import APIRouter, Query, Depends, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy import text

from ..core import get_engine
from ..services.satellite_service_v2 import SatelliteServiceV2
from ..services.inpe_service_v2 import INPEServiceV2
from ..services.google_maps_service_v2 import GoogleMapsServiceV2
from .deps import EngineDepends

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/satelite/v2", tags=["Satélite V2"])


# ============================================================================
# SCHEMAS
# ============================================================================

class FonteDecisaoResponse(BaseModel):
    """Resposta de decisão de fonte para satélite"""
    fonte: str
    pode_usar: bool
    motivo: str
    resolucao_metros: float
    cobertura: str
    quota_disponivel: Optional[int] = None
    percentual_uso: Optional[float] = None


class ImagemSateliteResponse(BaseModel):
    """Resposta com imagem de satélite"""
    id: str
    data: str
    cobertura_nuvem_percent: float
    resolucao_metros: float
    sensor: str
    fonte: str = "CBERS-4A"  # CBERS-4A ou GOOGLE_MAPS
    tipo: Optional[str] = None  # Para Google Maps: satellite, hybrid
    url: Optional[str] = None  # URL direta da imagem (Google Maps)
    banda_pan: Optional[str] = None
    banda_red: Optional[str] = None
    banda_green: Optional[str] = None
    banda_blue: Optional[str] = None


class BuscaTransformadorResponse(BaseModel):
    """Resposta de busca por transformador"""
    fonte: str
    transformador_id: int
    imagens_encontradas: int
    imagens: List[ImagemSateliteResponse]
    imagens_google_maps: Optional[List[ImagemSateliteResponse]] = []  # Imagens do Google Maps
    bbox: tuple
    area_poligonal_km: float
    periodo: str
    resolucao_metros: float
    status: str


class ImagemSalvaResponse(BaseModel):
    """Resposta com imagem salva no banco"""
    id: int
    imagem_id: str
    url_download: Optional[str] = None
    data_imagem: Optional[str] = None
    cobertura_nuvem_percentual: Optional[float] = None
    resolucao_metros: Optional[float] = None
    status: str
    data_requisicao: str


class ListarImagensTransformadorResponse(BaseModel):
    """Resposta com lista de imagens do transformador"""
    transformador_id: int
    total_requisicoes: int
    total_sucesso: int
    imagens: List[ImagemSalvaResponse]


class QuotaGoogleMapsResponse(BaseModel):
    """Resposta com quota Google Maps"""
    pode_usar: bool
    usada: int
    disponivel: int
    percentual_uso: float
    limite: int
    mes: str


class RegistrarImagemTransformadorRequest(BaseModel):
    """Request para registrar imagem de transformador"""
    transformador_id: int
    subestacao_id: int
    data_aquisicao: str
    resolucao_m: float = 2.0
    cobertura_nuvem_pct: float
    urls_bandas: Optional[Dict[str, str]] = None  # {blue, green, red, nir, swir} - opcional para CBERS-4A
    url_google_maps_satellite: Optional[str] = None  # URL da imagem satellite do Google Maps
    url_google_maps_hybrid: Optional[str] = None  # URL da imagem hybrid do Google Maps


class RegistrarImagemTransformadorResponse(BaseModel):
    """Response ao registrar imagem de transformador"""
    sucesso: bool
    imagem_id: Optional[int] = None
    bandas_registradas: int = 0
    mensagem: str


# ============================================================================
# ENDPOINTS - TRANSFORMADOR
# ============================================================================

@router.get(
    "/transformador/{transformador_id}/fonte",
    response_model=FonteDecisaoResponse,
    summary="Decidir fonte de satélite para transformador"
)
def decidir_fonte_transformador(
    engine: EngineDepends,
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    preferencia: Optional[str] = Query(
        None, 
        description="Preferência: 'CBERS-4A' ou 'GOOGLE_MAPS'"
    )
):
    """
    Decide qual fonte de satélite usar para um transformador.
    
    **Lógica:**
    1. Padrão: CBERS-4A WPM (gratuito, sem limite, 2m resolução)
    2. Se preferir Google Maps: verifica quota (25k/mês)
    3. Se quota excedida: fallback para CBERS-4A WPM
    
    **Exemplo de resposta:**
    ```json
    {
        "fonte": "CBERS-4A",
        "pode_usar": true,
        "motivo": "CBERS-4A - busca por transformador, resolução 2m WPM",
        "resolucao_metros": 2.0,
        "cobertura": "Brasil/América do Sul"
    }
    ```
    """
    try:
        sat_service = SatelliteServiceV2(engine)
        resultado = sat_service.decidir_fonte_satelite_transformador(
            transformador_id=transformador_id,
            preferencia_armazenada=preferencia
        )
        return FonteDecisaoResponse(**resultado)
    except Exception as e:
        logger.error(f"❌ Erro ao decidir fonte: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/transformador/{transformador_id}/imagens",
    response_model=BuscaTransformadorResponse,
    summary="Buscar imagens de satélite para transformador"
)
def buscar_imagens_transformador(
    engine: EngineDepends,
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    cobertura_nuvem_max: int = Query(
        30,
        ge=0,
        le=100,
        description="Máximo % de cobertura de nuvens"
    ),
    dias_passados: int = Query(
        90,
        ge=1,
        le=365,
        description="Buscar imagens dos últimos N dias"
    ),
    max_imagens: int = Query(
        50,
        ge=1,
        le=100,
        description="Máximo de imagens a retornar"
    )
):
    """
    Busca imagens de satélite para transformador de múltiplas fontes.
    
    **Fontes incluídas:**
    - **CBERS-4A**: Imagens de satélite brasileiras (gratuito, 2m resolução)
    - **Google Maps**: Imagens de alta resolução (0.3m, se API key configurada)
    
    **Parâmetros:**
    - **cobertura_nuvem_max**: Máximo % de nuvens para CBERS-4A (padrão: 30%)
    - **dias_passados**: Buscar imagens dos últimos N dias para CBERS-4A (padrão: 90)
    
    A área poligonal é obtida automaticamente do banco de dados do transformador.
    
    **Exemplo de resposta:**
    ```json
    {
        "fonte": "CBERS-4A",
        "transformador_id": 1001,
        "imagens_encontradas": 5,
        "imagens": [
            {
                "id": "CBERS_4A_WPM_20260101_...",
                "data": "2026-01-01",
                "cobertura_nuvem_percent": 15,
                "resolucao_metros": 2.0,
                "sensor": "CBERS-4A WPM",
                "fonte": "CBERS-4A",
                "banda_pan": "https://...",
                "banda_red": "https://...",
                "banda_green": "https://...",
                "banda_blue": "https://..."
            }
        ],
        "imagens_google_maps": [
            {
                "id": "GOOGLE_MAPS_1001_satellite",
                "data": "2026-01-31T10:00:00",
                "cobertura_nuvem_percent": 0,
                "resolucao_metros": 0.3,
                "sensor": "Google Maps",
                "fonte": "GOOGLE_MAPS",
                "tipo": "satellite",
                "url": "https://maps.googleapis.com/..."
            }
        ],
        "bbox": [-60.5, -15.8, -60.0, -15.3],
        "area_poligonal_km": 2.0,
        "periodo": "2025-10-01 a 2026-01-01",
        "resolucao_metros": 2.0,
        "status": "sucesso"
    }
    ```
    """
    try:
        # Calcular datas
        data_fim = datetime.now().strftime('%Y-%m-%d')
        data_inicio = (
            datetime.now() - 
            __import__('datetime').timedelta(days=dias_passados)
        ).strftime('%Y-%m-%d')
        
        sat_service = SatelliteServiceV2(engine)
        inpe_service = INPEServiceV2(engine, sat_service)
        
        # Buscar imagens CBERS-4A
        resultado = inpe_service.buscar_imagens_cbers4a_transformador(
            transformador_id=transformador_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
            cobertura_nuvem_max=cobertura_nuvem_max,
            max_imagens=max_imagens
        )
        
        if resultado['status'] != 'sucesso' and resultado.get('imagens_encontradas', 0) == 0:
            logger.warning(f"⚠️ Nenhuma imagem encontrada: {resultado.get('observacoes')}")
        
        # Buscar também imagens do Google Maps
        imagens_google_maps = []
        try:
            import os
            google_api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
            if google_api_key:
                google_service = GoogleMapsServiceV2(engine, api_key=google_api_key)
                resultado_google = google_service.buscar_imagens_transformador(
                    transformador_id=transformador_id,
                    zoom=18,
                    tamanho="640x640"
                )
                
                if resultado_google.get('sucesso'):
                    # Converter formato das imagens do Google Maps para o schema
                    for img in resultado_google.get('imagens', []):
                        imagens_google_maps.append({
                            'id': f"GOOGLE_MAPS_{transformador_id}_{img['tipo']}",
                            'data': img['data_obtencao'],
                            'cobertura_nuvem_percent': 0.0,  # Google Maps não tem info de nuvens
                            'resolucao_metros': 0.3,  # Google Maps tem alta resolução (~0.3m)
                            'sensor': 'Google Maps',
                            'fonte': 'GOOGLE_MAPS',
                            'tipo': img['tipo'],
                            'url': img['url'],
                            'banda_pan': None,
                            'banda_red': None,
                            'banda_green': None,
                            'banda_blue': None
                        })
                    logger.info(f"✅ {len(imagens_google_maps)} imagens do Google Maps adicionadas")
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível buscar imagens do Google Maps: {e}")
        
        # Adicionar imagens do Google Maps ao resultado
        resultado['imagens_google_maps'] = imagens_google_maps
        
        return BuscaTransformadorResponse(**resultado)
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar imagens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/transformador/{transformador_id}/imagens/historico",
    response_model=ListarImagensTransformadorResponse,
    summary="Listar imagens salvas do transformador no banco"
)
def listar_imagens_historico_transformador(
    engine: EngineDepends,
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    limit: int = Query(50, ge=1, le=100, description="Máximo de registros a retornar"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
    apenas_sucesso: bool = Query(True, description="Retornar apenas requisições bem-sucedidas")
):
    """
    Lista imagens salvas no banco de dados para um transformador.
    
    Retorna histórico de requisições CBERS-4A com:
    - **ID da requisição** - identificador único
    - **imagem_id** - ID CBERS-4A (ex: CBERS_4A_WFI_20260130_210_132_L4)
    - **url_download** - URL para download da imagem
    - **data_imagem** - Data de aquisição
    - **cobertura_nuvem_percentual** - % de cobertura de nuvens
    - **resolucao_metros** - Resolução em metros
    - **status** - sucesso / sem_cobertura / erro
    - **data_requisicao** - Quando a requisição foi feita
    
    **Parâmetros:**
    - `transformador_id`: ID do transformador
    - `limit`: Máximo de registros (padrão: 50)
    - `offset`: Página (padrão: 0)
    - `apenas_sucesso`: Filtrar por sucesso (padrão: true)
    
    **Exemplo:**
    ```
    GET /satelite/v2/transformador/1/imagens/historico?limit=10&apenas_sucesso=true
    ```
    """
    try:
        from sqlalchemy import text
        
        # Query SQL para buscar imagens
        where_clause = "WHERE transformador_id = :transformador_id"
        if apenas_sucesso:
            where_clause += " AND status = 'sucesso'"
        
        query = f"""
            SELECT 
                id,
                imagem_id,
                url_download,
                data_imagem,
                cobertura_nuvem_percentual,
                resolucao_metros,
                status,
                data_requisicao
            FROM requisicoes_satelite_cbers4a
            {where_clause}
            ORDER BY data_requisicao DESC
            LIMIT :limit OFFSET :offset
        """
        
        # Query para contar total
        count_query = f"""
            SELECT COUNT(*) as total
            FROM requisicoes_satelite_cbers4a
            {where_clause}
        """
        
        with engine.begin() as conn:
            # Executar query de contagem
            result_count = conn.execute(
                text(count_query),
                {'transformador_id': transformador_id}
            )
            total_sucesso = result_count.scalar() or 0
            
            # Executar query de dados
            result = conn.execute(
                text(query),
                {
                    'transformador_id': transformador_id,
                    'limit': limit,
                    'offset': offset
                }
            )
            
            imagens = []
            for row in result:
                imagens.append(ImagemSalvaResponse(
                    id=row[0],
                    imagem_id=row[1],
                    url_download=row[2],
                    data_imagem=str(row[3]) if row[3] else None,
                    cobertura_nuvem_percentual=float(row[4]) if row[4] is not None else None,
                    resolucao_metros=float(row[5]) if row[5] is not None else None,
                    status=row[6],
                    data_requisicao=str(row[7]) if row[7] else None
                ))
        
        # Contar total geral (sem limite)
        with engine.begin() as conn:
            result_total = conn.execute(
                text(count_query),
                {'transformador_id': transformador_id}
            )
            total_requisicoes = result_total.scalar() or 0
        
        return ListarImagensTransformadorResponse(
            transformador_id=transformador_id,
            total_requisicoes=total_requisicoes,
            total_sucesso=total_sucesso,
            imagens=imagens
        )
        
    except Exception as e:
        logger.error(f"❌ Erro ao listar imagens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/transformador/multiplos/imagens",
    response_model=Dict[int, BuscaTransformadorResponse],
    summary="Buscar imagens para múltiplos transformadores"
)
def buscar_imagens_multiplos_transformadores(
    engine: EngineDepends,
    transformador_ids: List[int] = Query(
        ...,
        description="Lista de IDs de transformadores"
    ),
    cobertura_nuvem_max: int = Query(
        25,
        ge=0,
        le=100,
        description="Máximo % de cobertura de nuvens"
    )
):
    """
    Busca imagens para múltiplos transformadores de uma vez.
    
    A área poligonal é obtida automaticamente do banco de dados.
    
    **Exemplo de request:**
    ```
    POST /satelite/v2/transformador/multiplos/imagens?transformador_ids=1&transformador_ids=2&transformador_ids=3
    ```
    
    **Retorna um dicionário com transformador_id como chave**
    
    Ideal para:
    - Análise em lote
    - Comparação entre transformadores
    - Relatórios consolidados
    """
    try:
        if not transformador_ids:
            raise HTTPException(status_code=400, detail="Lista de IDs vazia")
        
        if len(transformador_ids) > 100:
            raise HTTPException(
                status_code=400,
                detail="Máximo 100 transformadores por requisição"
            )
        
        sat_service = SatelliteServiceV2(engine)
        inpe_service = INPEServiceV2(engine, sat_service)
        
        resultados = {}
        for trans_id in transformador_ids:
            resultado = inpe_service.buscar_imagens_cbers4a_transformador(
                transformador_id=trans_id,
                cobertura_nuvem_max=cobertura_nuvem_max
            )
            resultados[trans_id] = BuscaTransformadorResponse(**resultado)
        
        return resultados
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar multiplos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/transformador/{transformador_id}/registrar-imagem",
    response_model=RegistrarImagemTransformadorResponse,
    summary="Registrar imagem CBERS-4A com bandas para transformador"
)
def registrar_imagem_transformador(
    transformador_id: int = Path(..., gt=0, description="ID do transformador"),
    requisicao: RegistrarImagemTransformadorRequest = None,
    engine: EngineDepends = None
):
    """
    Registra uma imagem de satélite (CBERS-4A ou Google Maps) para um transformador.
    
    **Funcionalidades:**
    - **ID da imagem**: Gerado automaticamente no formato timestamp
    - **CBERS-4A**: Aceita até 5 bandas (blue, green, red, nir, swir)
    - **Google Maps**: Aceita URLs satellite e/ou hybrid
    - Suporta ambas as fontes simultaneamente
    
    Salva no banco:
    - Metadados da imagem em `satelite_imagens`
    - URLs das bandas em `satelite_bandas` (para CBERS-4A)
    - URLs do Google Maps em campos separados
    
    **Exemplo 1 - CBERS-4A com bandas:**
    ```json
    {
      "transformador_id": 47,
      "subestacao_id": 5,
      "data_aquisicao": "2026-01-31T13:12:41",
      "resolucao_m": 2.0,
      "cobertura_nuvem_pct": 15.5,
      "urls_bandas": {
        "blue": "https://data.inpe.br/.../BAND0.tif",
        "green": "https://data.inpe.br/.../BAND1.tif",
        "red": "https://data.inpe.br/.../BAND2.tif",
        "nir": "https://data.inpe.br/.../BAND3.tif",
        "swir": "https://data.inpe.br/.../BAND4.tif"
      }
    }
    ```
    
    **Exemplo 2 - Google Maps:**
    ```json
    {
      "transformador_id": 47,
      "subestacao_id": 5,
      "data_aquisicao": "2026-01-31T13:12:41",
      "resolucao_m": 0.3,
      "cobertura_nuvem_pct": 0,
      "url_google_maps_satellite": "https://maps.googleapis.com/maps/api/staticmap?...",
      "url_google_maps_hybrid": "https://maps.googleapis.com/maps/api/staticmap?..."
    }
    ```
    
    **Resposta:**
    ```json
    {
      "sucesso": true,
      "imagem_id": 123,
      "bandas_registradas": 5,
      "mensagem": "Imagem registrada com sucesso (ID: IMG_20260131_131241_abc12345) - 5 bandas CBERS-4A"
    }
    ```
    """
    try:
        from ..services.inpe_satellite_service import INPESatelliteService, SatelliteMetadata, BoundingBox
        from uuid import uuid4
        
        # Gerar ID da imagem automaticamente
        imagem_id_gerado = f"IMG_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        
        logger.info(f"[REGISTRAR] Transformador {transformador_id}, Imagem {imagem_id_gerado}")
        
        service = INPESatelliteService(logger=logger)
        
        # Validar que pelo menos uma fonte foi fornecida
        tem_bandas = requisicao.urls_bandas and len(requisicao.urls_bandas) > 0
        tem_google_maps = requisicao.url_google_maps_satellite or requisicao.url_google_maps_hybrid
        
        if not tem_bandas and not tem_google_maps:
            raise HTTPException(
                status_code=400,
                detail="Forneça pelo menos urls_bandas (CBERS-4A) ou URLs do Google Maps"
            )
        
        # 1. Registrar metadados da imagem
        logger.info(f"  1. Registrando metadados com ID: {imagem_id_gerado}...")
        
        # Determinar sensor
        sensor = 'CBERS-4A' if tem_bandas else 'GOOGLE_MAPS'
        
        # Criar objeto SatelliteMetadata
        url_principal = None
        if tem_bandas:
            url_principal = list(requisicao.urls_bandas.values())[0]
        elif requisicao.url_google_maps_satellite:
            url_principal = requisicao.url_google_maps_satellite
        elif requisicao.url_google_maps_hybrid:
            url_principal = requisicao.url_google_maps_hybrid
        
        metadata = SatelliteMetadata(
            id=imagem_id_gerado,
            data_aquisicao=requisicao.data_aquisicao,
            sensor=sensor,
            resolucao_m=int(requisicao.resolucao_m),
            cobertura_nuvem_pct=requisicao.cobertura_nuvem_pct,
            url=url_principal,
            bounding_box=BoundingBox(min_lat=-23.5, max_lat=-23.0, min_lon=-46.5, max_lon=-46.0),
            propriedades={"imagem_id_gerado": imagem_id_gerado, "sensor": sensor}
        )
        
        imagem_id_db = service.armazenar_metadata_imagem(
            engine=engine,
            subestacao_id=requisicao.subestacao_id,
            metadata=metadata
        )
        
        logger.info(f"  Resultado: imagem_id_db={imagem_id_db}")
        
        if not imagem_id_db:
            logger.error("  ❌ Falha ao inserir")
            return RegistrarImagemTransformadorResponse(
                sucesso=False,
                mensagem="Falha ao registrar metadados da imagem"
            )
        
        logger.info(f"  ✅ Metadados registrados com imagem_id_db={imagem_id_db}")
        
        bandas_registradas = 0
        
        # 2. Registrar bandas CBERS-4A (se fornecidas)
        if tem_bandas:
            logger.info(f"  2. Registrando bandas CBERS-4A: {list(requisicao.urls_bandas.keys())}...")
            nome_para_numero = {
                'blue': (0, 'blue'),
                'green': (1, 'green'),
                'red': (2, 'red'),
                'nir': (3, 'nir'),
                'swir': (4, 'swir')
            }
            
            for nome_banda, url in requisicao.urls_bandas.items():
                if nome_banda not in nome_para_numero:
                    logger.warning(f"  ⚠️ Banda desconhecida: {nome_banda}")
                    continue
                
                numero_banda, nome_validado = nome_para_numero[nome_banda]
                
                sucesso = service.registrar_banda(
                    engine=engine,
                    imagem_id=imagem_id_db,
                    numero_banda=numero_banda,
                    nome_banda=nome_validado,
                    url_banda=url
                )
                
                if sucesso:
                    bandas_registradas += 1
                    logger.info(f"  ✅ Banda {nome_validado} registrada")
                else:
                    logger.warning(f"  ⚠️ Falha ao registrar banda {nome_validado}")
        
        # 3. Registrar URLs do Google Maps (se fornecidas)
        if tem_google_maps:
            logger.info("  3. Registrando URLs do Google Maps...")
            try:
                with engine.begin() as conn:
                    if requisicao.url_google_maps_satellite:
                        conn.execute(text("""
                            UPDATE satelite_imagens
                            SET url_google_maps_satellite = :url
                            WHERE id = :imagem_id
                        """), {
                            'url': requisicao.url_google_maps_satellite,
                            'imagem_id': imagem_id_db
                        })
                        logger.info("  ✅ URL Google Maps Satellite registrada")
                    
                    if requisicao.url_google_maps_hybrid:
                        conn.execute(text("""
                            UPDATE satelite_imagens
                            SET url_google_maps_hybrid = :url
                            WHERE id = :imagem_id
                        """), {
                            'url': requisicao.url_google_maps_hybrid,
                            'imagem_id': imagem_id_db
                        })
                        logger.info("  ✅ URL Google Maps Hybrid registrada")
            except Exception as e:
                logger.warning(f"  ⚠️ Erro ao registrar URLs Google Maps: {e}")
        
        mensagem = f"Imagem registrada com sucesso (ID: {imagem_id_gerado})"
        if bandas_registradas > 0:
            mensagem += f" - {bandas_registradas} bandas CBERS-4A"
        if tem_google_maps:
            mensagem += " - Google Maps"
        
        logger.info(f"  ✅ {mensagem}")
        
        return RegistrarImagemTransformadorResponse(
            sucesso=True,
            imagem_id=imagem_id_db,
            bandas_registradas=bandas_registradas,
            mensagem=mensagem
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao registrar imagem: {e}", exc_info=True)
        import traceback
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")



# ============================================================================
# ENDPOINTS - SUBESTAÇÃO
# ============================================================================

@router.get(
    "/subestacao/{subestacao_id}/fonte",
    response_model=FonteDecisaoResponse,
    summary="Decidir fonte de satélite para subestação"
)
def decidir_fonte_subestacao(
    engine: EngineDepends,
    subestacao_id: int = Path(..., gt=0, description="ID da subestação"),
    preferencia: Optional[str] = Query(
        None,
        description="Preferência: 'CBERS-4A' ou 'GOOGLE_MAPS'"
    )
):
    """
    Decide qual fonte de satélite usar para uma subestação.
    
    **Lógica:**
    1. CBERS-4A preferido (gratuito, sem limite)
    2. Google Maps como fallback (se houver quota)
    3. Tracking automático de requisições
    """
    try:
        sat_service = SatelliteServiceV2(engine)
        resultado = sat_service.decidir_fonte_satelite(
            subestacao_id=subestacao_id,
            preferencia_armazenada=preferencia
        )
        return FonteDecisaoResponse(**resultado)
    except Exception as e:
        logger.error(f"❌ Erro ao decidir fonte: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/subestacao/{subestacao_id}/imagens",
    response_model=Dict,
    summary="Buscar imagens de satélite para subestação"
)
def buscar_imagens_subestacao(
    engine: EngineDepends,
    subestacao_id: int = Path(..., gt=0, description="ID da subestação"),
    cobertura_nuvem_max: int = Query(
        30,
        ge=0,
        le=100,
        description="Máximo % de cobertura de nuvens"
    ),
    dias_passados: int = Query(
        90,
        ge=1,
        le=365,
        description="Buscar nos últimos N dias"
    )
):
    """
    Busca imagens CBERS-4A para toda a área de uma subestação (polígono).
    
    **Retorna:**
    - Imagens ordenadas por qualidade (menos nuvens primeiro)
    - Bounding box da subestação
    - Período da busca
    - Status da operação
    """
    try:
        data_fim = datetime.now().strftime('%Y-%m-%d')
        data_inicio = (
            datetime.now() -
            __import__('datetime').timedelta(days=dias_passados)
        ).strftime('%Y-%m-%d')
        
        sat_service = SatelliteServiceV2(engine)
        inpe_service = INPEServiceV2(engine, sat_service)
        
        resultado = inpe_service.buscar_imagens_cbers4a_poligono(
            subestacao_id=subestacao_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
            cobertura_nuvem_max=cobertura_nuvem_max
        )
        
        return resultado
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar imagens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - QUOTA E MONITORAMENTO
# ============================================================================

@router.get(
    "/quota/google-maps",
    response_model=QuotaGoogleMapsResponse,
    summary="Verificar quota Google Maps"
)
def verificar_quota_google_maps(
    engine: EngineDepends
):
    """
    Verifica quota mensal de Google Maps (25.000 requisições/mês).
    
    **Retorna:**
    - Requisições usadas
    - Requisições disponíveis
    - Percentual de uso
    - Mês atual
    
    **Exemplo de resposta:**
    ```json
    {
        "pode_usar": true,
        "usada": 5234,
        "disponivel": 19766,
        "percentual_uso": 20.9,
        "limite": 25000,
        "mes": "2024-01"
    }
    ```
    """
    try:
        sat_service = SatelliteServiceV2(engine)
        quota = sat_service.verificar_quota_google_maps()
        return QuotaGoogleMapsResponse(**quota)
    except Exception as e:
        logger.error(f"❌ Erro ao verificar quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/estatisticas",
    response_model=Dict,
    summary="Estatísticas de uso de satélites"
)
def obter_estatisticas(
    engine: EngineDepends
):
    """
    Retorna estatísticas de uso de satélites.
    
    **Informações:**
    - CBERS-4A: requisições bem-sucedidas, sem cobertura, cobertura média de nuvens
    - Google Maps: requisições, quota usada
    - Mês atual
    """
    try:
        sat_service = SatelliteServiceV2(engine)
        stats = sat_service.obter_estatisticas_satelite()
        return stats
    except Exception as e:
        logger.error(f"❌ Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# GOOGLE MAPS - TRANSFORMADOR
# ============================================================================

@router.get(
    "/google-maps/transformador/{id}/imagens",
    response_model=Dict,
    summary="Buscar imagens Google Maps para transformador"
)
def buscar_imagens_google_maps_transformador(
    id: int = Path(..., gt=0, description="ID do transformador"),
    engine: EngineDepends = None,
    zoom: int = Query(18, ge=10, le=20, description="Nível de zoom (10-20)"),
    tamanho: str = Query("640x640", regex="^\\d+x\\d+$", description="Tamanho da imagem"),
    api_key: str = Query(None, description="Chave da API Google Maps (opcional)"),
    auto_zoom: bool = Query(True, description="Calcular zoom automaticamente baseado na área poligonal")
):
    """
    Busca imagens estáticas do Google Maps para um transformador.
    
    Retorna URLs para satellite e hybrid views.
    A área poligonal é obtida automaticamente do banco de dados.
    
    **O zoom é calculado automaticamente baseado na área poligonal do transformador:**
    - Áreas pequenas (< 1 km²) → zoom alto (19-20)
    - Áreas médias (1-10 km²) → zoom médio (16-18)
    - Áreas grandes (> 10 km²) → zoom baixo (12-15)
    
    Você pode passar o parâmetro `auto_zoom=false` para forçar um zoom específico.
    
    **Parâmetros:**
    - **id**: ID do transformador
    - **zoom**: Nível de zoom (10-20, usado quando auto_zoom=false)
    - **tamanho**: Tamanho em pixels (WIDTHxHEIGHT)
    - **api_key**: Chave do Google Maps (se não fornecida, usa variável de ambiente)
    - **auto_zoom**: Se true, calcula zoom automaticamente pela área (default: true)
    
    **Exemplos:**
    - `GET /satelite/v2/google-maps/transformador/1/imagens` - Usa zoom automático
    - `GET /satelite/v2/google-maps/transformador/1/imagens?auto_zoom=false&zoom=17` - Força zoom 17
    
    **Resposta:**
    ```json
    {
        "sucesso": true,
        "transformador_id": 1,
        "nome": "Trafo Centro",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "imagens": [
            {
                "url": "https://maps.googleapis.com/...",
                "zoom": 17,
                "tipo": "satellite",
                "fonte": "GOOGLE_MAPS",
                "tamanho_pixels": "640x640",
                "area_poligonal_km": 5.2
            }
        ],
        "motivo": "Sucesso"
    }
    ```
    """
    try:
        logger.info(f"🗺️ Buscando imagens Google Maps para transformador ID: {id}")
        
        # Se api_key não foi fornecida, tentar buscar do ambiente
        import os
        if not api_key:
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        resultado = service.buscar_imagens_transformador(
            transformador_id=id,
            zoom=zoom,
            tamanho=tamanho,
            auto_zoom=auto_zoom
        )
        
        # Log detalhado do resultado
        logger.info(f"✅ Resultado para ID {id}:")
        logger.info(f"   - sucesso: {resultado.get('sucesso')}")
        logger.info(f"   - transformador_id: {resultado.get('transformador_id')}")
        logger.info(f"   - latitude: {resultado.get('latitude')}")
        logger.info(f"   - longitude: {resultado.get('longitude')}")
        logger.info(f"   - nome: {resultado.get('nome')}")
        if resultado.get('imagens'):
            logger.info(f"   - URL satellite: {resultado['imagens'][0]['url'][:100]}...")
        
        return resultado
    
    except Exception as e:
        logger.error(f"❌ Erro ao buscar imagens Google Maps para ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/google-maps/transformador/{id}/imagens-grid",
    response_model=Dict,
    summary="Buscar grid de imagens Google Maps para transformador"
)
def buscar_imagens_grid_transformador(
    id: int = Path(..., gt=0, description="ID do transformador"),
    engine: EngineDepends = None,
    zoom_grid: int = Query(20, ge=10, le=21, description="Nível de zoom para cada célula do grid"),
    tamanho: str = Query("640x640", regex="^\\d+x\\d+$", description="Tamanho de cada imagem"),
    api_key: str = Query(None, description="Chave da API Google Maps (opcional)")
):
    """
    Busca múltiplas imagens em grade (grid) para cobrir toda a área poligonal.
    
    Ideal para ter uma visão de alta resolução de toda a região com zoom máximo.
    Cada célula da grade é uma imagem independente em zoom máximo.
    
    **Funcionalidade:**
    - Calcula automaticamente a grade baseada na área poligonal
    - Retorna múltiplas imagens que cobrem a área inteira
    - Cada imagem tem coordenadas individuais (linha, coluna)
    - Oferece views satellite e hybrid para cada célula
    
    **Cálculo da grade:**
    - Quanto maior a área, mais imagens são geradas
    - Cada imagem cobre uma porção igual da área total
    - A cobertura por imagem varia com o zoom escolhido
    
    **Parâmetros:**
    - **id**: ID do transformador
    - **zoom_grid**: Nível de zoom (10-21, recomendado 20 para máxima resolução)
    - **tamanho**: Tamanho de cada imagem em pixels (WIDTHxHEIGHT)
    - **api_key**: Chave do Google Maps (se não fornecida, usa variável de ambiente)
    
    **Exemplos:**
    - `GET /satelite/v2/google-maps/transformador/1/imagens-grid` - Grid com zoom 20
    - `GET /satelite/v2/google-maps/transformador/1/imagens-grid?zoom_grid=18` - Grid com zoom 18
    
    **Resposta:**
    ```json
    {
        "sucesso": true,
        "transformador_id": 1,
        "nome": "Trafo Centro",
        "latitude_centro": -23.5505,
        "longitude_centro": -46.6333,
        "area_poligonal_km": 5.2,
        "zoom_grid": 20,
        "tamanho_imagem": "640x640",
        "dimensoes_grid": {
            "linhas": 2,
            "colunas": 2
        },
        "total_imagens": 4,
        "imagens": [
            {
                "linha": 0,
                "coluna": 0,
                "latitude": -23.545,
                "longitude": -46.625,
                "offset_lat_km": 0.5,
                "offset_lon_km": -0.5,
                "urls": {
                    "satellite": "https://maps.googleapis.com/...",
                    "hybrid": "https://maps.googleapis.com/..."
                },
                "zoom": 20,
                "tamanho_pixels": "640x640"
            }
        ],
        "motivo": "Sucesso"
    }
    ```
    """
    try:
        logger.info(f"📊 Buscando grid de imagens Google Maps para transformador ID: {id}")
        
        # Se api_key não foi fornecida, tentar buscar do ambiente
        import os
        if not api_key:
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        resultado = service.buscar_imagens_grid_transformador(
            transformador_id=id,
            tamanho=tamanho,
            zoom_grid=zoom_grid
        )
        
        # Log detalhado do resultado
        logger.info(f"✅ Grid gerado para ID {id}:")
        logger.info(f"   - sucesso: {resultado.get('sucesso')}")
        logger.info(f"   - total_imagens: {resultado.get('total_imagens')}")
        logger.info(f"   - dimensões: {resultado.get('dimensoes_grid')}")
        
        return resultado
    
    except Exception as e:
        logger.error(f"❌ Erro ao buscar grid para ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/google-maps/transformador/{id}/imagens-grid/salvar",
    response_model=Dict,
    summary="Buscar e salvar grid de imagens Google Maps no banco"
)
def salvar_imagens_grid_transformador(
    id: int = Path(..., gt=0, description="ID do transformador"),
    engine: EngineDepends = None,
    zoom_grid: int = Query(20, ge=10, le=21, description="Nível de zoom para cada célula do grid"),
    tamanho: str = Query("640x640", regex="^\\d+x\\d+$", description="Tamanho de cada imagem"),
    api_key: str = Query(None, description="Chave da API Google Maps (opcional)")
):
    """
    Busca múltiplas imagens em grade (grid) e salva no banco de dados.
    
    Este endpoint combina:
    1. Busca das imagens do grid (GET /imagens-grid)
    2. Download de cada imagem
    3. Registro no banco (tabela satelite_imagens)
    
    **Diferenças do endpoint GET:**
    - Este endpoint (POST) SALVA as imagens no banco
    - Endpoint GET apenas retorna as URLs
    - Útil para processamento posterior de detecção de telhados
    
    **Funcionalidade:**
    - Calcula automaticamente a grade baseada na área poligonal
    - Baixa e registra múltiplas imagens que cobrem a área inteira
    - Cada imagem é salva como registro independente em `satelite_imagens`
    - Metadados incluem posição no grid (linha, coluna)
    
    **Parâmetros:**
    - **id**: ID do transformador
    - **zoom_grid**: Nível de zoom (10-21, recomendado 20 para máxima resolução)
    - **tamanho**: Tamanho de cada imagem em pixels (WIDTHxHEIGHT)
    - **api_key**: Chave do Google Maps (se não fornecida, usa variável de ambiente)
    
    **Exemplos:**
    - `POST /satelite/v2/google-maps/transformador/1/imagens-grid/salvar` - Grid zoom 20
    - `POST /satelite/v2/google-maps/transformador/1/imagens-grid/salvar?zoom_grid=18` - Grid zoom 18
    
    **Resposta:**
    ```json
    {
        "sucesso": true,
        "transformador_id": 1,
        "subestacao_id": 1,
        "total_solicitadas": 4,
        "total_salvas": 4,
        "sensor": "Google_Maps_Grid_Z20",
        "dimensoes_grid": {
            "linhas": 2,
            "colunas": 2
        },
        "imagens": [
            {
                "imagem_id": 123,
                "linha": 0,
                "coluna": 0,
                "url": "https://maps.googleapis.com/..."
            }
        ]
    }
    ```
    
    **Uso Posterior:**
    As imagens salvas podem ser consultadas com:
    ```sql
    SELECT * FROM satelite_imagens 
    WHERE sensor LIKE 'Google_Maps_Grid%' 
    AND propriedades_json->>'transformador_id' = '1';
    ```
    """
    try:
        logger.info(f"💾 Salvando grid de imagens Google Maps para transformador ID: {id}")
        
        # Se api_key não foi fornecida, tentar buscar do ambiente
        import os
        if not api_key:
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        # 1. Buscar o grid de imagens
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        resultado_grid = service.buscar_imagens_grid_transformador(
            transformador_id=id,
            tamanho=tamanho,
            zoom_grid=zoom_grid
        )
        
        if not resultado_grid.get('sucesso'):
            logger.error(f"❌ Erro ao gerar grid: {resultado_grid.get('motivo')}")
            raise HTTPException(
                status_code=400,
                detail=f"Erro ao gerar grid: {resultado_grid.get('motivo')}"
            )
        
        # 2. Buscar subestacao_id do transformador
        with engine.connect() as conn:
            query = text("""
                SELECT subestacao_id 
                FROM transformadores 
                WHERE id = :id
            """)
            result = conn.execute(query, {'id': id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Transformador {id} não encontrado"
                )
            
            subestacao_id = row[0]
        
        # 3. Salvar imagens no banco
        from ..services.imagem_salvamento_service import ImagemSalvamentoService
        salvamento_service = ImagemSalvamentoService(engine)
        
        resultado_salvamento = salvamento_service.salvar_imagens_grid_google_maps(
            subestacao_id=subestacao_id,
            transformador_id=id,
            resultado_grid=resultado_grid
        )
        
        # Log detalhado do resultado
        if resultado_salvamento.get('sucesso'):
            logger.info(f"✅ Grid salvo para ID {id}:")
            logger.info(f"   - total_salvas: {resultado_salvamento.get('total_salvas')}")
            logger.info(f"   - total_solicitadas: {resultado_salvamento.get('total_solicitadas')}")
            logger.info(f"   - sensor: {resultado_salvamento.get('sensor')}")
        else:
            logger.error(f"❌ Erro ao salvar grid: {resultado_salvamento.get('erro')}")
        
        return resultado_salvamento
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao salvar grid para ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/google-maps/transformador/multiplos/imagens",
    response_model=Dict,
    summary="Buscar imagens Google Maps para múltiplos transformadores"
)
def buscar_imagens_google_maps_multiplos(
    engine: EngineDepends,
    transformador_ids: List[int] = Query(..., title="Lista de IDs de transformadores (máx 100)"),
    zoom: int = Query(18, ge=10, le=20),
    tamanho: str = Query("640x640", regex="^\\d+x\\d+$"),
    api_key: str = Query(None)
):
    """
    Busca imagens para múltiplos transformadores em lote.
    
    **Limitações:**
    - Máximo 100 transformadores por requisição
    - Respeita quota de 25k/mês do Google Maps
    
    **Resposta:**
    ```json
    {
        "total_solicitados": 5,
        "sucessos": 5,
        "erros": 0,
        "percentual_sucesso": 100,
        "resultados": [...]
    }
    ```
    """
    try:
        import os
        if not api_key:
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        resultado = service.buscar_imagens_multiplos_transformadores(
            transformador_ids=transformador_ids,
            zoom=zoom,
            tamanho=tamanho
        )
        return resultado
    
    except Exception as e:
        logger.error(f"❌ Erro ao buscar múltiplas imagens: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/google-maps/quota",
    response_model=Dict,
    summary="Status da quota Google Maps"
)
def obter_quota_google_maps(
    engine: EngineDepends
):
    """
    Retorna status da quota do Google Maps para o mês atual.
    
    Limite: 25.000 requisições/mês
    
    **Resposta:**
    ```json
    {
        "limite_mensal": 25000,
        "usada_mes_atual": 1250,
        "disponivel": 23750,
        "percentual_uso": 5.0,
        "transformadores_unicos": 42,
        "ultima_requisicao": "2024-01-15T10:30:00"
    }
    ```
    """
    try:
        import os
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        quota = service.obter_quota_google_maps_mes_atual()
        return quota
    
    except Exception as e:
        logger.error(f"❌ Erro ao verificar quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/google-maps/estatisticas",
    response_model=Dict,
    summary="Estatísticas de uso do Google Maps"
)
def obter_estatisticas_google_maps(
    engine: EngineDepends
):
    """
    Retorna estatísticas detalhadas de uso do Google Maps.
    
    Inclui:
    - Total de requisições históricas
    - Transformadores únicos buscados
    - Histórico dos últimos 30 dias
    - Quota do mês atual
    """
    try:
        import os
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        
        service = GoogleMapsServiceV2(engine=engine, api_key=api_key)
        stats = service.obter_estatisticas_google_maps()
        return stats
    
    except Exception as e:
        logger.error(f"❌ Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
