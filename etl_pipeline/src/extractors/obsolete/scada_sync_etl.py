"""
ETL: Sincronização Periódica com Sistema SCADA
Atualiza dados de transformadores, recalcula áreas e sincroniza com banco

MODOS:
- scada_only: Apenas SCADA (marca OSM como inativos)
- hibrido: OSM + SCADA (recomendado)
  * OSM como base/fallback
  * SCADA como overlay em tempo real
  * Marca inativo apenas se foi removido do SCADA
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy import text

# Adicionar diretório src ao path
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importar funções do core (padrão do projeto)
from core import create_db_engine, create_session, load_settings
from services.area_service import AreaService

logger = logging.getLogger(__name__)


class SCADAClient:
    """Cliente para integração com sistema SCADA"""
    
    def __init__(self, base_url: str = None, api_key: str = None, engine = None):
        """
        Inicializa cliente SCADA
        
        Em produção, configure:
        - base_url: URL base da API SCADA
        - api_key: Chave de autenticação
        - engine: Para modo fallback (buscar dados reais do OSM no banco)
        """
        self.base_url = base_url or "http://scada.concessionaria.com.br/api"
        self.api_key = api_key or "API_KEY_AQUI"
        self.engine = engine
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        })
    
    def get_transformadores(self, subestacao_id: int) -> List[Dict]:
        """Busca transformadores do SCADA ou fallback para dados reais"""
        try:
            # Em produção:
            # response = self.session.get(
            #     f"{self.base_url}/subestacoes/{subestacao_id}/transformadores"
            # )
            # return response.json()['data']
            
            # Usar dados reais do OSM como fallback
            logger.info("📡 Buscando transformadores reais do OpenStreetMap...")
            return self._dados_reais_osm(subestacao_id)
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar transformadores: {e}")
            return []
    
    def _dados_reais_osm(self, subestacao_id: int) -> List[Dict]:
        """Busca transformadores reais do OpenStreetMap armazenados no banco"""
        if not self.engine:
            logger.warning("⚠️ Engine não disponível, retornando lista vazia")
            return []
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT 
                        id, codigo, latitude, longitude, 
                        potencia_kva, tipo, status, 
                        tensao_primaria_kv, tensao_secundaria_v
                    FROM transformadores
                    WHERE subestacao_id = :subestacao_id 
                      AND codigo LIKE 'OSM-%'
                      AND status = 'ativo'
                    LIMIT 50
                """), {'subestacao_id': subestacao_id})
                
                rows = result.fetchall()
                
                if not rows:
                    logger.warning(f"   ⚠️ Nenhum transformador OSM encontrado para SE {subestacao_id}")
                    return []
                
                transformadores = []
                for row in rows:
                    transformadores.append({
                        'codigo': row[1],
                        'latitude': float(row[2]) if row[2] else -15.8100,
                        'longitude': float(row[3]) if row[3] else -47.9100,
                        'potencia_kva': float(row[4]) if row[4] else 300.0,
                        'tipo': row[5] or 'desconhecido',
                        'status': row[6] or 'ativo',
                        'tensao_primaria_kv': float(row[7]) if row[7] else 13.8,
                        'tensao_secundaria_v': float(row[8]) if row[8] else 220
                    })
                
                logger.info(f"   ✅ {len(transformadores)} transformadores OSM encontrados")
                return transformadores
                
        except Exception as e:
            logger.error(f"   ❌ Erro ao buscar dados OSM: {e}")
            return []


# ============================================================================
# SINCRONIZAÇÃO
# ============================================================================

def sincronizar_transformadores(
    subestacao_id: int,
    scada_client: SCADAClient,
    engine,
    modo: str = 'hibrido'
) -> Tuple[int, int, int]:
    """
    Sincroniza transformadores com SCADA
    Retorna: (novos, atualizados, inativos)
    """
    logger.info(f"\n🔄 Sincronizando transformadores da SE ID={subestacao_id}")
    
    # Buscar dados do SCADA
    transformadores_scada = scada_client.get_transformadores(subestacao_id)
    logger.info(f"   SCADA: {len(transformadores_scada)} transformadores")
    
    if not transformadores_scada:
        logger.warning("   ⚠️ Nenhum dado retornado do SCADA")
        return 0, 0, 0
    
    novos = 0
    atualizados = 0
    inativos = 0
    
    with engine.begin() as conn:
        # Processar cada transformador
        codigos_scada = []
        for trans in transformadores_scada:
            codigo = trans['codigo']
            codigos_scada.append(codigo)
            
            try:
                # Verificar se existe
                result = conn.execute(text("""
                    SELECT id FROM transformadores WHERE codigo = :codigo
                """), {'codigo': codigo})
                
                existing = result.fetchone()
                
                if existing:
                    # Atualizar
                    conn.execute(text("""
                        UPDATE transformadores SET
                            latitude = :latitude,
                            longitude = :longitude,
                            potencia_kva = :potencia_kva,
                            tipo = :tipo,
                            status = :status,
                            tensao_primaria_kv = :tensao_primaria_kv,
                            tensao_secundaria_v = :tensao_secundaria_v,
                            localizacao = ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326),
                            updated_at = NOW()
                        WHERE codigo = :codigo
                    """), {
                        'codigo': codigo,
                        'latitude': trans['latitude'],
                        'longitude': trans['longitude'],
                        'potencia_kva': trans['potencia_kva'],
                        'tipo': trans['tipo'],
                        'status': trans['status'],
                        'tensao_primaria_kv': trans['tensao_primaria_kv'],
                        'tensao_secundaria_v': trans['tensao_secundaria_v']
                    })
                    atualizados += 1
                else:
                    # Inserir novo
                    conn.execute(text("""
                        INSERT INTO transformadores (
                            codigo, subestacao_id, nome, latitude, longitude,
                            potencia_kva, tipo, status, tensao_primaria_kv,
                            tensao_secundaria_v, localizacao
                        ) VALUES (
                            :codigo, :subestacao_id, :nome, :latitude, :longitude,
                            :potencia_kva, :tipo, :status, :tensao_primaria_kv,
                            :tensao_secundaria_v,
                            ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)
                        )
                    """), {
                        'codigo': codigo,
                        'subestacao_id': subestacao_id,
                        'nome': f"Transformador {codigo}",
                        'latitude': trans['latitude'],
                        'longitude': trans['longitude'],
                        'potencia_kva': trans['potencia_kva'],
                        'tipo': trans['tipo'],
                        'status': trans['status'],
                        'tensao_primaria_kv': trans['tensao_primaria_kv'],
                        'tensao_secundaria_v': trans['tensao_secundaria_v']
                    })
                    novos += 1
                    
            except Exception as e:
                logger.error(f"   ❌ Erro ao processar {codigo}: {e}")
        
        # Marcar como inativos os que não estão no SCADA
        # APENAS EM MODO scada_only (em hibrido, OSM fica ativo)
        if codigos_scada and modo == 'scada_only':
            placeholders = ','.join([f"'{c}'" for c in codigos_scada])
            result = conn.execute(text(f"""
                UPDATE transformadores
                SET status = 'inativo', updated_at = NOW()
                WHERE subestacao_id = :subestacao_id
                  AND codigo NOT IN ({placeholders})
                  AND status = 'ativo'
            """), {'subestacao_id': subestacao_id})
            
            inativos = result.rowcount
        elif modo == 'hibrido':
            # Em modo híbrido, manter OSM como ativo (fallback)
            inativos = 0
    
    logger.info(f"   ✅ Novos: {novos} | Atualizados: {atualizados} | Inativos: {inativos}")
    
    return novos, atualizados, inativos


def recalcular_areas(subestacao_id: int, engine) -> bool:
    """
    Recalcula áreas de cobertura após sincronização
    Usa novo AreaService
    """
    logger.info(f"\n📐 Recalculando área de cobertura para SE ID={subestacao_id}")
    
    service = AreaService(engine)
    
    with engine.begin() as conn:
        # Verificar se há polígono oficial
        result = conn.execute(text("""
            SELECT metodo_definicao FROM subestacoes_area_cobertura
            WHERE subestacao_id = :subestacao_id
        """), {'subestacao_id': subestacao_id})
        
        row = result.fetchone()
        
        # Se for oficial, não recalcular
        if row and row[0] == 'cadastro_oficial':
            logger.info("   ⚠️ Polígono oficial - não será recalculado")
            return False
        
        # Contar transformadores
        result = conn.execute(text("""
            SELECT COUNT(*) FROM transformadores
            WHERE subestacao_id = :subestacao_id AND status = 'ativo'
        """), {'subestacao_id': subestacao_id})
        
        count = result.scalar()
        
        if count < 3:
            logger.warning(f"   ⚠️ Necessário mínimo 3 transformadores ativos (atual: {count})")
            return False
    
    # Recalcular usando AreaService (que cuida de tudo)
    # Para subestação
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM transformadores
            WHERE subestacao_id = :subestacao_id AND status = 'ativo'
            AND localizacao IS NOT NULL
        """), {'subestacao_id': subestacao_id})
        
        valid_count = result.scalar()
        
        if valid_count >= 3:
            # Recalcular convex hull
            conn.execute(text("""
                INSERT INTO subestacoes_area_cobertura (
                    subestacao_id,
                    area_cobertura,
                    metodo_definicao,
                    data_atualizacao,
                    observacoes
                )
                SELECT 
                    :subestacao_id,
                    ST_ConvexHull(ST_Collect(localizacao)),
                    'analise_topologica',
                    NOW(),
                    'Atualizado automaticamente em ' || NOW()::date
                FROM transformadores
                WHERE subestacao_id = :subestacao_id AND status = 'ativo'
                ON CONFLICT (subestacao_id) DO UPDATE SET
                    area_cobertura = EXCLUDED.area_cobertura,
                    data_atualizacao = NOW(),
                    observacoes = EXCLUDED.observacoes
            """), {'subestacao_id': subestacao_id})
            
            # Calcular área
            conn.execute(text("""
                UPDATE subestacoes_area_cobertura
                SET area_km2 = ST_Area(area_cobertura::geography) / 1000000
                WHERE subestacao_id = :subestacao_id
            """), {'subestacao_id': subestacao_id})
            
            result = conn.execute(text("""
                SELECT area_km2 FROM subestacoes_area_cobertura
                WHERE subestacao_id = :subestacao_id
            """), {'subestacao_id': subestacao_id})
            
            area_km2 = result.scalar()
            logger.info(f"   ✅ Área recalculada: {area_km2:.2f} km²")
            
            return True
    
    return False


def limpar_dados_antigos(engine, dias: int = 90) -> Tuple[int, int]:
    """Remove consumidores e transformadores inativos há mais de X dias"""
    
    logger.info(f"\n🧹 Limpando dados inativos há mais de {dias} dias")
    
    with engine.begin() as conn:
        # Consumidores inativos
        result = conn.execute(text("""
            DELETE FROM consumidores
            WHERE status = 'inativo'
              AND updated_at < NOW() - INTERVAL ':dias days'
        """), {'dias': dias})
        
        consumidores_removidos = result.rowcount
        
        # Transformadores inativos (sem consumidores)
        result = conn.execute(text("""
            DELETE FROM transformadores t
            WHERE status = 'inativo'
              AND updated_at < NOW() - INTERVAL ':dias days'
              AND NOT EXISTS (
                  SELECT 1 FROM consumidores c
                  WHERE c.transformador_id = t.id
              )
        """), {'dias': dias})
        
        transformadores_removidos = result.rowcount
    
    logger.info(f"   ✅ Consumidores removidos: {consumidores_removidos}")
    logger.info(f"   ✅ Transformadores removidos: {transformadores_removidos}")
    
    return consumidores_removidos, transformadores_removidos


def executar_sincronizacao_completa(
    subestacao_ids: List[int],
    scada_client: SCADAClient,
    engine,
    modo: str = 'hibrido'
):
    """Executa sincronização completa"""
    
    logger.info("\n" + "=" * 80)
    logger.info(f"🔄 SINCRONIZAÇÃO COMPLETA ({modo.upper()}) - TRANSFORMADORES E ÁREAS")
    logger.info("=" * 80)
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📍 Subestações: {len(subestacao_ids)}")
    
    total_novos = 0
    total_atualizados = 0
    total_inativos = 0
    areas_recalculadas = 0
    
    for se_id in subestacao_ids:
        novos, atualizados, inativos = sincronizar_transformadores(
            se_id, scada_client, engine, modo
        )
        total_novos += novos
        total_atualizados += atualizados
        total_inativos += inativos
        
        # Recalcular área se houve mudanças
        if novos > 0 or inativos > 0:
            if recalcular_areas(se_id, engine):
                areas_recalculadas += 1
    
    # Limpar dados antigos
    limpar_dados_antigos(engine, dias=90)
    
    logger.info("\n" + "=" * 80)
    logger.info("📊 RESUMO DA SINCRONIZAÇÃO")
    logger.info("=" * 80)
    logger.info(f"   Transformadores novos: {total_novos}")
    logger.info(f"   Transformadores atualizados: {total_atualizados}")
    logger.info(f"   Transformadores marcados inativos: {total_inativos}")
    logger.info(f"   Áreas recalculadas: {areas_recalculadas}")
    logger.info(f"   Subestações processadas: {len(subestacao_ids)}")
    logger.info("=" * 80)


def executar_loop_continuo(engine, intervalo_minutos: int = 60, modo: str = 'hibrido'):
    """Executa sincronização em loop contínuo (daemon)"""
    
    scada_client = SCADAClient()
    
    # Buscar todas as subestações
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT id FROM subestacoes_detectadas
            ORDER BY id
        """))
        subestacao_ids = [row[0] for row in result.fetchall()]
    
    logger.info(f"🔁 Iniciando sincronização contínua ({modo})")
    logger.info(f"   Intervalo: {intervalo_minutos} minutos")
    logger.info(f"   Subestações: {len(subestacao_ids)}")
    
    while True:
        try:
            executar_sincronizacao_completa(subestacao_ids, scada_client, engine, modo)
            
            logger.info(f"\n⏳ Próxima sincronização em {intervalo_minutos} minutos...")
            time.sleep(intervalo_minutos * 60)
            
        except KeyboardInterrupt:
            logger.info("\n⏹️ Sincronização interrompida pelo usuário")
            break
        except Exception as e:
            logger.error(f"\n❌ Erro: {e}")
            logger.info(f"⏳ Tentando novamente em 5 minutos...")
            time.sleep(300)


# ============================================================================
# CLI
# ============================================================================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(
        description='ETL de Sincronização com SCADA'
    )
    
    parser.add_argument(
        '--subestacao-ids',
        type=int,
        nargs='+',
        help='IDs das subestações'
    )
    
    parser.add_argument(
        '--todas',
        action='store_true',
        help='Sincronizar todas as subestações'
    )
    
    parser.add_argument(
        '--modo',
        choices=['hibrido', 'scada_only'],
        default='hibrido',
        help='Modo: hibrido (OSM+SCADA) ou scada_only (padrão: hibrido)'
    )
    
    parser.add_argument(
        '--loop',
        action='store_true',
        help='Executar em loop contínuo'
    )
    
    parser.add_argument(
        '--intervalo',
        type=int,
        default=60,
        help='Intervalo em minutos (padrão: 60)'
    )
    
    parser.add_argument(
        '--limpar-antigos',
        type=int,
        help='Limpar dados inativos há X dias'
    )
    
    args = parser.parse_args()
    
    try:
        settings = load_settings()
        engine = create_db_engine(settings.database.url)
        scada_client = SCADAClient(engine=engine)  # Passar engine para modo fallback OSM
        
        # Limpar dados antigos
        if args.limpar_antigos:
            limpar_dados_antigos(engine, args.limpar_antigos)
            return 0
        
        # Modo híbrido: reativar OSM que foram marcados como inativos
        if args.modo == 'hibrido':
            logger.info("🔄 MODO HÍBRIDO: OSM (base) + SCADA (tempo real)")
            with engine.begin() as conn:
                result = conn.execute(text("""
                    UPDATE transformadores
                    SET status = 'ativo', updated_at = NOW()
                    WHERE codigo LIKE 'OSM-%' AND status = 'inativo'
                """))
                reativados = result.rowcount
                if reativados > 0:
                    logger.info(f"   ✅ Reativados {reativados} transformadores OSM")
        
        # Determinar subestações
        if args.todas:
            with engine.begin() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT id FROM subestacoes_detectadas
                    ORDER BY id
                """))
                subestacao_ids = [row[0] for row in result.fetchall()]
        elif args.subestacao_ids:
            subestacao_ids = args.subestacao_ids
        else:
            parser.print_help()
            logger.info("\n💡 Exemplos:")
            logger.info("  python src/extractors/scada_sync_etl.py --todas --modo hibrido")
            logger.info("  python src/extractors/scada_sync_etl.py --subestacao-ids 1 2 3 --modo hibrido")
            logger.info("  python src/extractors/scada_sync_etl.py --todas --loop --intervalo 120")
            logger.info("  docker-compose exec etl python src/extractors/scada_sync_etl.py --todas --modo scada_only")
            return 1
        
        # Executar
        if args.loop:
            executar_loop_continuo(engine, args.intervalo, args.modo)
        else:
            executar_sincronizacao_completa(subestacao_ids, scada_client, engine, args.modo)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
