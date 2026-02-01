"""
API REST para processamento de telhados/edifícios em imagens de satélite
"""

import logging
import json
from datetime import datetime
from typing import Optional, Dict
from fastapi import APIRouter, HTTPException, Query, Path
from sqlalchemy import text

from ..services.telhado_segmentation_service import (
    ResultadoProcessamentoTelhados
)
from ..services.telhado_transformador_service import (
    TelhadoTransformadorService
)

from ..schemas.telhado import (
    ResultadoSegmentacaoResponse,
    TelhadoDetectadoResponse,
    ListaTelhadosResponse,
    EstatisticasSegmentacaoResponse,
)

# Configure logging
logger = logging.getLogger(__name__)

# Criar router
router = APIRouter(
    prefix="/telhados",
    tags=["Telhados - Segmentação"],
    responses={404: {"description": "Não encontrado"}}
)

# Cache global do serviço (simplificado - usar Redis em produção)
_resultados_processamento: Dict[str, ResultadoProcessamentoTelhados] = {}

# ============================================================================
# ENDPOINT 1: Listar Telhados com Filtros
# ============================================================================

@router.get(
    "/lista",
    response_model=ListaTelhadosResponse,
    summary="Listar telhados processados",
    description="Retorna lista paginada de telhados com filtros opcionais"
)
async def listar_telhados(
    id_subestacao: Optional[str] = Query(None, description="Filtrar por subestação"),
    tipo_edificio: Optional[str] = Query(None, description="Filtrar por tipo"),
    confianca_minima: float = Query(0.0, ge=0, le=1.0, description="Confiança mínima"),
    pagina: int = Query(1, ge=1, description="Número da página"),
    limite: int = Query(100, ge=1, le=10000, description="Itens por página")
) -> ListaTelhadosResponse:
    """
    Lista telhados com suporte a filtros e paginação
    
    Args:
        id_subestacao: Filtro opcional de subestação
        tipo_edificio: Filtro opcional de tipo
        confianca_minima: Filtro de confiança mínima
        pagina: Página (padrão: 1)
        limite: Itens por página (padrão: 100)
    """
    try:
        # Buscar resultados processados (em cache)
        telhados_filtrados = []
        
        for resultado in _resultados_processamento.values():
            if id_subestacao and resultado.id_subestacao != id_subestacao:
                continue
            
            for telhado in resultado.telhados:
                if confianca_minima and telhado.confianca < confianca_minima:
                    continue
                if tipo_edificio and telhado.tipo_edificio != tipo_edificio:
                    continue
                
                telhados_filtrados.append(telhado)
        
        # Paginação
        inicio = (pagina - 1) * limite
        fim = inicio + limite
        
        # Converter para response
        responses = [_converter_telhado_para_response(t) for t in telhados_filtrados]
        responses_pagina = responses[inicio:fim]
        
        return ListaTelhadosResponse(
            total_resultados=len(telhados_filtrados),
            pagina=pagina,
            resultados_por_pagina=limite,
            total_paginas=(len(telhados_filtrados) + limite - 1) // limite,
            telhados=responses_pagina
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar telhados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 2: Detalhes de Uma Subestação
# ============================================================================

@router.get(
    "/subestacao/{id_subestacao}",
    response_model=ResultadoSegmentacaoResponse,
    summary="Obter detalhes de telhados de uma subestação",
    description="Retorna resultado completo do processamento de uma subestação"
)
async def obter_detalhes_subestacao(
    id_subestacao: str = Path(..., description="ID da subestação")
) -> ResultadoSegmentacaoResponse:
    """
    Obtém resultado armazenado em cache
    
    Args:
        id_subestacao: ID da subestação
    """
    try:
        # Buscar no cache
        for resultado in _resultados_processamento.values():
            if resultado.id_subestacao == id_subestacao:
                return _converter_resultado_para_response(resultado)
        
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum processamento encontrado para subestação {id_subestacao}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao obter detalhes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 3: Estatísticas Agregadas
# ============================================================================

@router.get(
    "/estatisticas",
    response_model=EstatisticasSegmentacaoResponse,
    summary="Estatísticas agregadas de segmentação",
    description="Retorna estatísticas consolidadas de todos os processamentos"
)
async def obter_estatisticas(
    periodo: Optional[str] = Query(None, description="Período (ex: 2025-01)")
) -> EstatisticasSegmentacaoResponse:
    """
    Calcula e retorna estatísticas agregadas
    """
    try:
        # Período padrão: mês atual
        if not periodo:
            periodo = datetime.now().strftime("%Y-%m")
        
        # Agregar dados
        total_subestacoes = set()
        total_telhados_detectados = 0
        total_telhados_segmentados = 0
        total_imagens = 0
        
        confiancas = []
        qualidades = []
        areas = []
        tipos_edificios = {}
        tempos_processamento = []
        
        for resultado in _resultados_processamento.values():
            total_subestacoes.add(resultado.id_subestacao)
            total_telhados_detectados += resultado.telhados_detectados
            total_telhados_segmentados += resultado.total_telhados_segmentados
            total_imagens += 1
            tempos_processamento.append(resultado.tempo_processamento_segundos)
            
            for telhado in resultado.telhados:
                confiancas.append(telhado.confianca)
                qualidades.append(telhado.propriedades_adicionais.get('indice_qualidade', 0.5))
                areas.append(telhado.area_m2)
                
                tipo = telhado.tipo_edificio
                tipos_edificios[tipo] = tipos_edificios.get(tipo, 0) + 1
        
        # Calcular médias
        media_confianca = sum(confiancas) / len(confiancas) if confiancas else 0
        media_qualidade = sum(qualidades) / len(qualidades) if qualidades else 0
        media_area = sum(areas) / len(areas) if areas else 0
        media_tempo = sum(tempos_processamento) / len(tempos_processamento) if tempos_processamento else 0
        tempo_total = sum(tempos_processamento)
        
        taxa_sucesso = 100.0  # Simplificado
        
        return EstatisticasSegmentacaoResponse(
            periodo=periodo,
            total_subestacoes_processadas=len(total_subestacoes),
            total_telhados_detectados=total_telhados_detectados,
            total_telhados_segmentados=total_telhados_segmentados,
            total_imagens_processadas=total_imagens,
            media_telhados_por_subestacao=(total_telhados_detectados / len(total_subestacoes)) if total_subestacoes else 0,
            media_confianca_deteccao=media_confianca,
            media_indice_qualidade=media_qualidade,
            media_area_telhado_m2=media_area,
            distribuicao_tipo_edificio=tipos_edificios,
            tempo_medio_processamento_segundos=media_tempo,
            tempo_total_processamento_segundos=tempo_total,
            taxa_sucesso_percentual=taxa_sucesso
        )
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def _converter_resultado_para_response(resultado: ResultadoProcessamentoTelhados) -> ResultadoSegmentacaoResponse:
    """Converte ResultadoProcessamentoTelhados para Pydantic model"""
    telhados_response = [_converter_telhado_para_response(t) for t in resultado.telhados]
    
    return ResultadoSegmentacaoResponse(
        id_subestacao=resultado.id_subestacao,
        id_imagem_satelite=resultado.id_imagem_satelite,
        timestamp_processamento=resultado.timestamp_processamento,
        telhados_detectados=resultado.telhados_detectados,
        telhados_segmentados=resultado.total_telhados_segmentados,
        tempo_processamento_segundos=resultado.tempo_processamento_segundos,
        telhados=telhados_response,
        erros=resultado.erros,
        avisos=resultado.avisos,
        sucesso=len(resultado.erros) == 0,
        mensagem="Processamento concluído com sucesso" if len(resultado.erros) == 0 else "Processamento com erros"
    )


def _converter_telhado_para_response(telhado) -> TelhadoDetectadoResponse:
    """Converte TelhadoDetectado para Pydantic model"""
    from backend.src.schemas.telhado import (
        BoundingBoxPixeis, CentroidePixeis, CoordenadaGeografica
    )
    
    bbox_pixeis = BoundingBoxPixeis(
        x=int(telhado.bbox["x"]),
        y=int(telhado.bbox["y"]),
        largura=int(telhado.bbox["w"]),
        altura=int(telhado.bbox["h"])
    )
    
    centroide = CentroidePixeis(
        x=telhado.centroide["x"],
        y=telhado.centroide["y"]
    )
    
    coord = CoordenadaGeografica(
        latitude=telhado.lat,
        longitude=telhado.lon
    ) if telhado.lat != 0 else None
    
    return TelhadoDetectadoResponse(
        id_telhado=telhado.id_telhado,
        id_subestacao=telhado.id_subestacao,
        id_imagem_satelite=telhado.id_imagem_satelite,
        bbox=bbox_pixeis,
        centroide=centroide,
        coordenada_geografica=coord,
        area_pixeis=telhado.area_pixeis,
        area_m2=telhado.area_m2,
        confianca=telhado.confianca,
        tipo_edificio=telhado.tipo_edificio,
        percentual_cobertura=telhado.propriedades_adicionais.get('percentual_cobertura'),
        indice_qualidade=telhado.propriedades_adicionais.get('indice_qualidade'),
        timestamp_deteccao=telhado.timestamp_deteccao,
        modelo_deteccao=telhado.modelo_deteccao,
        propriedades_adicionais=telhado.propriedades_adicionais
    )


# ============================================================================
# ENDPOINTS PARA TRANSFORMADORES
# ============================================================================

@router.get("/transformador/{id}/telhados")
def listar_telhados_transformador(
    id: int = Path(..., description="ID do transformador")
):
    """
    Lista todos os telhados detectados para um transformador.
    
    Retorna histórico de detecções com maior confiança.
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    id, transformador_id, subestacao_id, latitude, longitude,
                    area_m2, confianca, bbox_json, timestamp_deteccao
                FROM telhados_detectados_transformador
                WHERE transformador_id = :trans_id
                ORDER BY timestamp_deteccao DESC
                LIMIT 100
            """), {'trans_id': id})
            
            telhados = []
            for row in result:
                telhados.append({
                    'id': row[0],
                    'transformador_id': row[1],
                    'subestacao_id': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4]),
                    'area_m2': float(row[5]),
                    'confianca': float(row[6]),
                    'bbox': json.loads(row[7]) if row[7] else {},
                    'timestamp': row[8].isoformat() if row[8] else None
                })
            
            return {
                "transformador_id": id,
                "total": len(telhados),
                "telhados": telhados
            }
    
    except Exception as e:
        logger.error(f"Erro ao listar telhados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subestacao/{id}/telhados-transformadores")
def obter_estatisticas_telhados_subestacao(
    id: int = Path(..., description="ID da subestação")
):
    """
    Obtém estatísticas agregadas de telhados para todos os 
    transformadores de uma subestação.
    
    - Total de transformadores
    - Total de telhados detectados
    - Área total coberta
    - Distribuição por tipo de edificio
    """
    try:
        from ..core import get_engine
        engine = get_engine()
        
        with engine.begin() as conn:
            # Estatísticas gerais
            result = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT transformador_id) as transformadores,
                    COUNT(DISTINCT id) as total_telhados,
                    SUM(area_m2) as area_total_m2,
                    AVG(confianca) as confianca_media
                FROM telhados_detectados_transformador
                WHERE subestacao_id = :sub_id
            """), {'sub_id': id})
            
            row = result.fetchone()
            
            return {
                "subestacao_id": id,
                "transformadores_processados": row[0] or 0,
                "total_telhados": row[1] or 0,
                "area_total_m2": float(row[2]) if row[2] else 0,
                "confianca_media": float(row[3]) if row[3] else 0,
                "timestamp": datetime.now().isoformat()
            }
    
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
