"""
Script para distribuir transformadores OSM existentes para todas as subestações
baseado em proximidade geográfica usando Python puro (evita problemas SQL complexos).
"""

import logging
import sys
from pathlib import Path

from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, load_settings

logger = logging.getLogger(__name__)


def distribuir_transformadores_por_proximidade(engine):
    """Redistribui transformadores OSM para a subestação mais próxima."""
    
    logger.info("=" * 80)
    logger.info("🔄 REDISTRIBUINDO TRANSFORMADORES OSM POR PROXIMIDADE")
    logger.info("=" * 80)
    
    with engine.begin() as conn:
        # 1. ANTES - Contar transformadores atuais
        logger.info("\n📊 ANTES da redistribuição:")
        result = conn.execute(text("""
            SELECT subestacao_id, COUNT(*) as qtd
            FROM transformadores
            WHERE codigo LIKE 'OSM-%'
            GROUP BY subestacao_id
            ORDER BY qtd DESC
        """))
        
        antes = {}
        for se_id, qtd in result:
            logger.info(f"   SE {se_id}: {qtd} transformadores")
            antes[se_id] = qtd
        
        # 2. Carregag subestações
        logger.info("\n🔄 Carregando subestações...")
        result = conn.execute(text("""
            SELECT id_estacao, nome, latitude, longitude
            FROM subestacoes_ons
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """))
        
        subestacoes = {}
        for se_id, nome, lat, lon in result:
            subestacoes[se_id] = {'nome': nome, 'lat': float(lat), 'lon': float(lon)}
        
        logger.info(f"✅ {len(subestacoes)} subestações carregadas")
        
        # 3. Carregar transformadores OSM
        logger.info("\n🔄 Carregando transformadores OSM...")
        result = conn.execute(text("""
            SELECT id, latitude, longitude
            FROM transformadores
            WHERE codigo LIKE 'OSM-%'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        """))
        
        transformadores = []
        for trans_id, lat, lon in result:
            transformadores.append({'id': trans_id, 'lat': float(lat), 'lon': float(lon)})
        
        logger.info(f"✅ {len(transformadores)} transformadores OSM carregados")
        
        # 4. Para cada transformador, encontrar subestação mais próxima
        logger.info(f"\n🔄 Calculando proximidades ({len(transformadores)} transformadores)...")
        
        atualizacoes = []
        for i, t in enumerate(transformadores):
            if i % 500 == 0 and i > 0:
                logger.info(f"   Processados {i}/{len(transformadores)}...")
            
            min_dist = float('inf')
            se_id_proxima = None
            
            for se_id, s_data in subestacoes.items():
                # Distância euclidiana (graus)
                dist = ((t['lat'] - s_data['lat']) ** 2 + (t['lon'] - s_data['lon']) ** 2) ** 0.5
                if dist < min_dist:
                    min_dist = dist
                    se_id_proxima = se_id
            
            if se_id_proxima:
                atualizacoes.append((t['id'], se_id_proxima))
        
        # 5. Aplicar atualizações
        logger.info(f"\n✅ Aplicando {len(atualizacoes)} atualizações...")
        
        for trans_id, se_id in atualizacoes:
            conn.execute(text("""
                UPDATE transformadores SET subestacao_id = :se_id WHERE id = :trans_id
            """), {'se_id': se_id, 'trans_id': trans_id})
        
        logger.info(f"✅ Atualizações aplicadas!")
        
        # 6. DEPOIS - Verificar resultado
        logger.info("\n📊 DEPOIS da redistribuição (top 20):")
        result = conn.execute(text("""
            SELECT t.subestacao_id, s.nome, COUNT(*) as qtd
            FROM transformadores t
            LEFT JOIN subestacoes_ons s ON t.subestacao_id::text = s.id_estacao
            WHERE t.codigo LIKE 'OSM-%'
            GROUP BY t.subestacao_id, s.nome
            ORDER BY qtd DESC
            LIMIT 20
        """))
        
        depois = {}
        for se_id, nome, qtd in result:
            logger.info(f"   SE {se_id:>5} ({nome:>20}): {qtd:>5} transformadores")
            depois[se_id] = qtd
        
        # 7. Estatísticas finais
        result = conn.execute(text("""
            SELECT COUNT(*), COUNT(DISTINCT subestacao_id)
            FROM transformadores WHERE codigo LIKE 'OSM-%'
        """))
        
        total_osm, ses_com_osm = result.fetchone()
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ RESUMO FINAL DA REDISTRIBUIÇÃO")
        logger.info("=" * 80)
        logger.info(f"   Total de transformadores OSM: {total_osm}")
        logger.info(f"   Subestações com OSM: {ses_com_osm}")
        logger.info("=" * 80)
        
        return total_osm, ses_com_osm


def main():
    """Função principal"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    logger.info("🚀 Iniciando redistribuição de transformadores OSM...\n")
    
    try:
        settings = load_settings()
        if not settings.database.url:
            raise ValueError("DATABASE_URL não configurada")
        
        engine = create_db_engine(settings.database.url)
        total_osm, ses_com_osm = distribuir_transformadores_por_proximidade(engine)
        
        logger.info(f"\n✅ Concluído! {total_osm} transformadores em {ses_com_osm} subestações")
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
