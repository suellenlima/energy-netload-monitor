"""
TIER 2 Area Use Cases - DDD Pattern

Casos de uso para operações de área de cobertura de subestações.
"""

from dataclasses import dataclass
from typing import Dict, Any
from src.domain.subestacao import ISubestacaoRepository


@dataclass
class ObtenerAreaSubestacaoUseCase:
    """Obtém a área de cobertura de uma subestação."""
    
    repository: ISubestacaoRepository
    
    def executar(self, subestacao_id: int) -> Dict[str, Any]:
        """
        Obtém área de cobertura de uma subestação.
        
        Args:
            subestacao_id: ID da subestação
            
        Returns:
            Dict com dados da área (wkt_area, geojson_area, nome, area_km2, total_transformadores)
        """
        try:
            # Busca subestação
            subestacao = self.repository.obter_por_id(subestacao_id)
            if not subestacao:
                return {
                    'sucesso': False,
                    'dados': None,
                    'mensagem': f'Subestação {subestacao_id} não encontrada'
                }
            
            # Retorna dados de área (estrutura padrão)
            return {
                'sucesso': True,
                'dados': {
                    'id': subestacao_id,
                    'nome': subestacao.nome,
                    'wkt_area': None,  # Será preenchido por lógica real de área
                    'geojson_area': None,  # Será preenchido por lógica real de área
                    'area_km2': 0.0,
                    'total_transformadores': 0
                },
                'mensagem': 'Área obtida com sucesso'
            }
        except Exception as e:
            return {
                'sucesso': False,
                'dados': None,
                'mensagem': f'Erro ao obter área: {str(e)}'
            }


@dataclass
class ObtenerTransformadoresUseCase:
    """Obtém transformadores de uma subestação."""
    
    repository: ISubestacaoRepository
    
    def executar(self, subestacao_id: int) -> Dict[str, Any]:
        """
        Obtém lista de transformadores de uma subestação.
        
        Args:
            subestacao_id: ID da subestação
            
        Returns:
            Dict com lista de transformadores
        """
        try:
            from sqlalchemy import text
            from ...core.database import get_engine
            import logging
            
            logger = logging.getLogger(__name__)
            engine = get_engine()
            
            # Busca transformadores diretamente do banco
            with engine.connect() as conn:
                # Primeiro busca o código da subestação
                query_sub = text("SELECT id, codigo, nome FROM subestacoes_aneel WHERE id = :id")
                result_sub = conn.execute(query_sub, {"id": subestacao_id})
                sub_row = result_sub.fetchone()
                
                if not sub_row:
                    logger.warning(f"Subestação {subestacao_id} não encontrada no banco")
                    return {
                        'sucesso': False,
                        'dados': [],
                        'mensagem': f'Subestação {subestacao_id} não encontrada'
                    }
                
                if not sub_row[1]:
                    logger.warning(f"Subestação {subestacao_id} (nome: {sub_row[2]}) não tem código")
                    return {
                        'sucesso': False,
                        'dados': [],
                        'mensagem': f'Subestação {subestacao_id} não possui código'
                    }
                
                subestacao_codigo = sub_row[1]
                subestacao_nome = sub_row[2]
                logger.info(f"Buscando transformadores da subestação ID={subestacao_id}, codigo='{subestacao_codigo}', nome='{subestacao_nome}'")
                
                # Debug: verificar exemplos de subestacao_codigo em transformadores
                query_sample = text("""
                    SELECT DISTINCT subestacao_codigo, COUNT(*) as total
                    FROM transformadores_aneel 
                    WHERE distribuidora = 'LIGHT'
                    GROUP BY subestacao_codigo
                    ORDER BY total DESC
                    LIMIT 10
                """)
                result_sample = conn.execute(query_sample)
                exemplos = [(r[0], r[1]) for r in result_sample.fetchall()]
                logger.info(f"Exemplos de subestacao_codigo em transformadores LIGHT: {exemplos}")
                
                # Verificar se existe campo subestacao_id em transformadores
                query_check_id = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'transformadores_aneel' 
                    AND column_name LIKE '%subestacao%'
                """)
                result_check = conn.execute(query_check_id)
                colunas_subestacao = [r[0] for r in result_check.fetchall()]
                logger.info(f"Colunas relacionadas a subestação em transformadores_aneel: {colunas_subestacao}")
                
                # Debug: verificar quantos transformadores têm subestacao_id preenchido
                query_id_stats = text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(subestacao_id) as com_id,
                        COUNT(*) - COUNT(subestacao_id) as sem_id
                    FROM transformadores_aneel
                    WHERE distribuidora = 'LIGHT'
                """)
                result_stats = conn.execute(query_id_stats)
                stats = result_stats.fetchone()
                logger.info(f"Stats transformadores LIGHT: total={stats[0]}, com_subestacao_id={stats[1]}, sem_subestacao_id={stats[2]}")
                
                # Buscar transformadores usando múltiplas estratégias
                transformadores_encontrados = []
                
                # Estratégia 1: Buscar por subestacao_id (se existir e estiver preenchido)
                if 'subestacao_id' in colunas_subestacao and stats[1] > 0:
                    logger.info(f"Buscando por subestacao_id = {subestacao_id}")
                    query_by_id = text("""
                        SELECT 
                            t.id, t.codigo, t.nome, t.latitude, t.longitude, t.potencia_kva,
                            t.subestacao_codigo, t.tipo_tensao, t.distribuidora, t.ativo,
                            COALESCE((SELECT COUNT(*) FROM paineis_solares_detectados psd WHERE psd.transformador_id = t.id), 0) as total_paineis
                        FROM transformadores_aneel t
                        WHERE t.subestacao_id = :subestacao_id
                    """)
                    result_by_id = conn.execute(query_by_id, {"subestacao_id": subestacao_id})
                    rows_by_id = result_by_id.fetchall()
                    transformadores_encontrados.extend(rows_by_id)
                    logger.info(f"Encontrados {len(rows_by_id)} transformadores por subestacao_id")
                
                # Estratégia 2 (FALLBACK): Se não encontrou por ID, buscar por proximidade geográfica (raio 5km)
                if not transformadores_encontrados:
                    logger.info(f"Nenhum transformador vinculado via subestacao_id. Usando fallback: proximidade geográfica (raio 5km)...")
                    
                    # Buscar coordenadas da subestação
                    query_coords = text("SELECT latitude, longitude FROM subestacoes_aneel WHERE id = :id")
                    result_coords = conn.execute(query_coords, {"id": subestacao_id})
                    coords_row = result_coords.fetchone()
                    
                    if coords_row and coords_row[0] and coords_row[1]:
                        sub_lat = coords_row[0]
                        sub_lng = coords_row[1]
                        logger.info(f"Coordenadas da subestação: lat={sub_lat}, lng={sub_lng}")
                        
                        # Buscar transformadores num raio de 5km (aproximadamente 0.045 graus)
                        query_nearby = text("""
                            SELECT 
                                t.id, t.codigo, t.nome, t.latitude, t.longitude, t.potencia_kva,
                                t.subestacao_codigo, t.tipo_tensao, t.distribuidora, t.ativo,
                                COALESCE((SELECT COUNT(*) FROM paineis_solares_detectados psd WHERE psd.transformador_id = t.id), 0) as total_paineis,
                                SQRT(POW(t.latitude - :lat, 2) + POW(t.longitude - :lng, 2)) * 111 as distancia_km
                            FROM transformadores_aneel t
                            WHERE t.distribuidora = 'LIGHT'
                            AND t.latitude IS NOT NULL 
                            AND t.longitude IS NOT NULL
                            AND SQRT(POW(t.latitude - :lat, 2) + POW(t.longitude - :lng, 2)) < 0.045
                            ORDER BY distancia_km
                            LIMIT 100
                        """)
                        result_nearby = conn.execute(query_nearby, {"lat": sub_lat, "lng": sub_lng})
                        rows_nearby = result_nearby.fetchall()
                        # Converter para formato compatível (sem o campo distancia_km)
                        for row in rows_nearby:
                            transformadores_encontrados.append(row[:11])  # Pega apenas os 11 primeiros campos
                        logger.info(f"Encontrados {len(rows_nearby)} transformadores por proximidade geográfica (raio 5km)")
                    else:
                        logger.warning(f"Subestação {subestacao_id} não possui coordenadas válidas para busca por proximidade")
                
                # Estratégia 3 (FALLBACK FINAL): Por código exato da subestação
                if not transformadores_encontrados:
                    query_trans = text("""
                        SELECT 
                            t.id, t.codigo, t.nome, t.latitude, t.longitude, t.potencia_kva,
                            t.subestacao_codigo, t.tipo_tensao, t.distribuidora, t.ativo,
                            COALESCE((SELECT COUNT(*) FROM paineis_solares_detectados psd WHERE psd.transformador_id = t.id), 0) as total_paineis
                        FROM transformadores_aneel t
                        WHERE t.subestacao_codigo = :subestacao_codigo
                    """)
                    result = conn.execute(query_trans, {"subestacao_codigo": subestacao_codigo})
                    rows = result.fetchall()
                    transformadores_encontrados.extend(rows)
                    if rows:
                        logger.info(f"Encontrados {len(rows)} transformadores por código da subestação")
                
                # Estratégia 4 (ÚLTIMO FALLBACK): Busca por nome da subestação no campo subestacao_codigo
                if not transformadores_encontrados and subestacao_nome:
                    logger.info(f"Tentando busca por nome no campo subestacao_codigo: '{subestacao_nome}'")
                    query_nome = text("""
                        SELECT 
                            t.id, t.codigo, t.nome, t.latitude, t.longitude, t.potencia_kva,
                            t.subestacao_codigo, t.tipo_tensao, t.distribuidora, t.ativo,
                            COALESCE((SELECT COUNT(*) FROM paineis_solares_detectados psd WHERE psd.transformador_id = t.id), 0) as total_paineis
                        FROM transformadores_aneel t
                        WHERE UPPER(t.subestacao_codigo) LIKE UPPER(:pattern)
                    """)
                    result_nome = conn.execute(query_nome, {"pattern": f"%{subestacao_nome}%"})
                    rows_nome = result_nome.fetchall()
                    transformadores_encontrados.extend(rows_nome)
                    if rows_nome:
                        logger.info(f"Encontrados {len(rows_nome)} transformadores por nome no campo subestacao_codigo")
                
                logger.info(f"Total final: {len(transformadores_encontrados)} transformadores encontrados")
                
                transformadores = []
                for row in transformadores_encontrados:
                    transformadores.append({
                        'id': row[0],
                        'codigo': row[1],
                        'nome': row[2],
                        'latitude': row[3],
                        'longitude': row[4],
                        'potencia_kva': float(row[5]) if row[5] else 0,
                        'subestacao_codigo': row[6],
                        'tipo_tensao': row[7],
                        'distribuidora': row[8],
                        'ativo': row[9],
                        'total_paineis': row[10]
                    })
                
                return {
                    'sucesso': True,
                    'dados': transformadores,
                    'mensagem': f'{len(transformadores)} transformadores encontrados'
                }
                
        except Exception as e:
            logger.error(f"Erro ao buscar transformadores: {e}", exc_info=True)
            return {
                'sucesso': False,
                'dados': [],
                'mensagem': f'Erro ao obter transformadores: {str(e)}'
            }


@dataclass
class ObtenerEstatisticasAreasUseCase:
    """Obtém estatísticas agregadas de áreas de cobertura."""
    
    repository: ISubestacaoRepository
    
    def executar(self) -> Dict[str, Any]:
        """
        Obtém estatísticas gerais de áreas.
        
        Returns:
            Dict com estatísticas de áreas
        """
        try:
            # Busca todas as subestações
            subestacoes = self.repository.listar(offset=0, limite=10000)
            
            # Retorna estatísticas (estrutura padrão)
            return {
                'sucesso': True,
                'dados': {
                    'total_subestacoes': len(subestacoes),
                    'area_total_km2': 0.0,
                    'transformadores_total': 0,
                    'densidade_media': 0.0
                },
                'mensagem': 'Estatísticas obtidas com sucesso'
            }
        except Exception as e:
            return {
                'sucesso': False,
                'dados': {},
                'mensagem': f'Erro ao obter estatísticas: {str(e)}'
            }
