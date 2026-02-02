#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Serviço para rastrear e gerenciar quota de Google Maps API
Usa tabelas existentes: requisicoes_satelite_google, quota_satelite_google_mes
"""

import logging
from datetime import datetime
from typing import Optional, Dict
from sqlalchemy import text

logger = logging.getLogger(__name__)

class GoogleMapsQuotaService:
    """Gerencia quota e requisições de Google Maps usando tabelas existentes"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def registrar_requisicao(self,
                            transformador_id: int,
                            subestacao_id: Optional[int] = None,
                            tipo_requisicao: str = 'satellite',
                            zoom: Optional[int] = 19,
                            largura: Optional[int] = 640,
                            altura: Optional[int] = 640,
                            status: str = 'sucesso',
                            url: Optional[str] = None,
                            tempo_ms: int = 0,
                            codigo_resposta: int = 200,
                            custo_unidades: float = 1.0,
                            notas: Optional[str] = None) -> Dict:
        """
        Registra uma requisição ao Google Maps
        Tabela: public.requisicoes_satelite_google
        """
        
        try:
            sub_id = subestacao_id or 1
            custo_usd = custo_unidades * 0.007
            
            with self.engine.connect() as conn:
                query = text("""
                    INSERT INTO requisicoes_satelite_google 
                    (subestacao_id, tipo_requisicao, status, ano_mes, observacoes)
                    VALUES (:sub_id, :tipo, :status, TO_CHAR(NOW(), 'YYYY-MM'), :notas)
                    RETURNING id, data_requisicao
                """)
                
                notas_completas = f"Trafo:{transformador_id} Zoom:{zoom} Tempo:{tempo_ms}ms Custo:${custo_usd:.4f}"
                if notas:
                    notas_completas += f" {notas}"
                
                result = conn.execute(query, {
                    'sub_id': sub_id,
                    'tipo': tipo_requisicao,
                    'status': status,
                    'notas': notas_completas
                })
                
                row = result.fetchone()
                conn.commit()
                
                logger.info(f"✓ Requisição registrada: ID={row[0]}, Custo=${custo_usd:.4f}")
                
                return {
                    'sucesso': True,
                    'id_requisicao': row[0],
                    'custo_usd': custo_usd,
                    'timestamp': row[1].isoformat() if row[1] else datetime.now().isoformat()
                }
        
        except Exception as e:
            logger.error(f"Erro ao registrar requisição: {e}")
            return {'sucesso': False, 'erro': str(e)}
    
    def obter_quota_mes(self, mes: Optional[str] = None) -> Dict:
        """Obtém informações de quota do mês"""
        
        if not mes:
            mes = datetime.now().strftime('%Y-%m')
        
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT id, ano_mes, total_requisicoes, requisicoes_sucesso,
                           requisicoes_erro, limite_maximo, percentual_uso
                    FROM quota_satelite_google_mes
                    WHERE ano_mes = :mes
                """)
                
                result = conn.execute(query, {'mes': mes})
                row = result.fetchone()
                
                if row:
                    return {
                        'sucesso': True,
                        'mes_ano': str(row[1]),
                        'quota_usada_requests': int(row[2]),
                        'quota_total': int(row[5]),
                        'percentual_uso': float(row[6]),
                        'custo_estimado_usd': round(int(row[2]) * 0.007, 2)
                    }
                else:
                    return {'sucesso': False, 'erro': f'Nenhuma quota para {mes}'}
        
        except Exception as e:
            logger.error(f"Erro ao obter quota: {e}")
            return {'sucesso': False, 'erro': str(e)}
