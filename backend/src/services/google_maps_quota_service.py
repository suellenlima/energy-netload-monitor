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
    
    def obter_requisicoes_transformador(self,
                                       transformador_id: int,
                                       limite: int = 100) -> Dict:
        """Obtém histórico de requisições de um transformador"""
        
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT id, tipo_requisicao, status, data_requisicao, observacoes
                    FROM requisicoes_satelite_google
                    WHERE observacoes LIKE :trafo_filter
                    ORDER BY data_requisicao DESC
                    LIMIT :limite
                """)
                
                result = conn.execute(query, {
                    'trafo_filter': f'%Trafo:{transformador_id}%',
                    'limite': limite
                })
                
                requisicoes = []
                for row in result:
                    requisicoes.append({
                        'id': row[0],
                        'tipo': row[1],
                        'status': row[2],
                        'timestamp': row[3].isoformat() if row[3] else None,
                        'notas': row[4]
                    })
                
                return {'sucesso': True, 'total': len(requisicoes), 'requisicoes': requisicoes}
        
        except Exception as e:
            logger.error(f"Erro ao obter requisições: {e}")
            return {'sucesso': False, 'erro': str(e)}
    
    def obter_custo_total(self, transformador_id: int) -> Dict:
        """Calcula custo total acumulado de um transformador"""
        
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT COUNT(*) as total_requisicoes,
                           COUNT(CASE WHEN status = 'sucesso' THEN 1 END) as total_sucesso
                    FROM requisicoes_satelite_google
                    WHERE observacoes LIKE :trafo_filter
                """)
                
                result = conn.execute(query, {'trafo_filter': f'%Trafo:{transformador_id}%'})
                row = result.fetchone()
                
                if row:
                    total_sucesso = int(row[1])
                    return {
                        'sucesso': True,
                        'total_requisicoes': int(row[0]),
                        'total_requisicoes_sucesso': total_sucesso,
                        'total_unidades': total_sucesso,
                        'total_usd': round(total_sucesso * 0.007, 4)
                    }
                else:
                    return {
                        'sucesso': True,
                        'total_requisicoes': 0,
                        'total_requisicoes_sucesso': 0,
                        'total_unidades': 0,
                        'total_usd': 0.0
                    }
        
        except Exception as e:
            logger.error(f"Erro ao calcular custo: {e}")
            return {'sucesso': False, 'erro': str(e)}
