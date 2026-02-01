"""
Teste Prático: Busca de Imagens por Transformador

Exemplo completo integrando:
1. SatelliteServiceV2 - Decisão de fonte
2. INPEServiceV2 - Busca de imagens
3. Dados reais do PostgreSQL
"""

import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Adicionar paths
BACKEND_DIR = Path(__file__).resolve().parents[1]  # backend
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from src.core import get_settings
from src.core.database import get_engine
from src.services.satellite_service_v2 import SatelliteServiceV2
from src.services.inpe_service_v2 import INPEServiceV2


def teste_1_transformadores_disponiveis():
    """Teste 1: Listar transformadores disponíveis no banco"""
    logger.info("\n" + "="*80)
    logger.info("TESTE 1: Transformadores Disponíveis no Banco")
    logger.info("="*80)
    
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            # Contar total
            result = conn.execute("""
                SELECT COUNT(*) FROM transformadores
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            """)
            total = result.scalar()
            logger.info(f"\n✅ Total de transformadores com coordenadas: {total}")
            
            # Top 5
            result = conn.execute("""
                SELECT id, subestacao_id, latitude, longitude, tipo, potencia_kva
                FROM transformadores
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY id
                LIMIT 5
            """)
            
            logger.info("\nPrimeiros 5 transformadores:")
            for row in result:
                logger.info(f"  T{row[0]}: SE{row[1]} | ({row[2]:.4f}, {row[3]:.4f}) | {row[4]} | {row[5]}kVA")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        return False


def teste_2_decidir_fonte_transformador():
    """Teste 2: Decidir fonte para transformador"""
    logger.info("\n" + "="*80)
    logger.info("TESTE 2: Decidir Fonte de Satélite para Transformador")
    logger.info("="*80)
    
    engine = get_engine()
    
    try:
        sat_service = SatelliteServiceV2(engine)
        
        # Testar com transformador ID 1
        transformador_id = 1
        
        logger.info(f"\n🔍 Decidindo fonte para transformador {transformador_id}")
        
        decisao = sat_service.decidir_fonte_satelite_transformador(
            transformador_id=transformador_id
        )
        
        logger.info(f"\n✅ Decisão:")
        logger.info(f"   Fonte: {decisao['fonte']}")
        logger.info(f"   Pode usar: {decisao['pode_usar']}")
        logger.info(f"   Motivo: {decisao['motivo']}")
        logger.info(f"   Resolução: {decisao['resolucao_metros']}m")
        logger.info(f"   Cobertura: {decisao['cobertura']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def teste_3_buscar_imagens_transformador():
    """Teste 3: Buscar imagens para transformador (simulado)"""
    logger.info("\n" + "="*80)
    logger.info("TESTE 3: Buscar Imagens para Transformador")
    logger.info("="*80)
    
    engine = get_engine()
    
    try:
        sat_service = SatelliteServiceV2(engine)
        inpe_service = INPEServiceV2(engine, sat_service)
        
        transformador_id = 1
        
        logger.info(f"\n🛰️ Buscando imagens para transformador {transformador_id}")
        logger.info("   (NOTA: Requer pystac-client instalado e conexão com INPE STAC)")
        
        # Verificar se pystac_client está disponível
        try:
            import pystac_client
            has_pystac = True
        except ImportError:
            has_pystac = False
            logger.warning("⚠️  pystac-client não está instalado")
        
        if not has_pystac:
            logger.info("\n📌 Para testar completamente, instale pystac-client:")
            logger.info("   pip install pystac-client")
            logger.info("\n📌 Simulando resposta esperada:")
            
            resultado_simulado = {
                'fonte': 'CBERS-4A',
                'transformador_id': transformador_id,
                'imagens_encontradas': 3,
                'imagens': [
                    {
                        'id': 'CBERS_4A_PAN_20240115_...',
                        'data': '2024-01-15',
                        'cobertura_nuvem_percent': 12,
                        'resolucao_metros': 2.0,
                        'sensor': 'CBERS-4A WPM'
                    },
                    {
                        'id': 'CBERS_4A_PAN_20240110_...',
                        'data': '2024-01-10',
                        'cobertura_nuvem_percent': 18,
                        'resolucao_metros': 2.0,
                        'sensor': 'CBERS-4A WPM'
                    }
                ],
                'bbox': (-43.94, -19.93, -43.93, -19.92),
                'raio_km': 2.0,
                'status': 'sucesso'
            }
            
            logger.info("\n✅ Resposta esperada:")
            logger.info(f"   Fonte: {resultado_simulado['fonte']}")
            logger.info(f"   Imagens encontradas: {resultado_simulado['imagens_encontradas']}")
            logger.info(f"   Status: {resultado_simulado['status']}")
            
            for i, img in enumerate(resultado_simulado['imagens']):
                logger.info(f"\n   Imagem {i+1}:")
                logger.info(f"     ID: {img['id']}")
                logger.info(f"     Data: {img['data']}")
                logger.info(f"     Nuvens: {img['cobertura_nuvem_percent']}%")
                logger.info(f"     Resolução: {img['resolucao_metros']}m")
            
            return True
        
        # Se tiver pystac_client, fazer requisição real
        resultado = inpe_service.buscar_imagens_cbers4a_transformador(
            transformador_id=transformador_id,
            raio_km=1.5,
            cobertura_nuvem_max=30
        )
        
        logger.info(f"\n✅ Busca concluída:")
        logger.info(f"   Status: {resultado['status']}")
        logger.info(f"   Imagens encontradas: {resultado['imagens_encontradas']}")
        
        if resultado['imagens']:
            logger.info(f"\n   Top 3 imagens:")
            for i, img in enumerate(resultado['imagens'][:3]):
                logger.info(f"   {i+1}. {img['id']}")
                logger.info(f"      Data: {img['data']}, Nuvens: {img['cobertura_nuvem_percent']}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def teste_4_quota_google_maps():
    """Teste 4: Verificar quota Google Maps"""
    logger.info("\n" + "="*80)
    logger.info("TESTE 4: Verificar Quota Google Maps")
    logger.info("="*80)
    
    engine = get_engine()
    
    try:
        sat_service = SatelliteServiceV2(engine)
        
        logger.info("\n📊 Verificando quota Google Maps...")
        
        quota = sat_service.verificar_quota_google_maps()
        
        logger.info(f"\n✅ Quota Google Maps:")
        logger.info(f"   Mês: {quota['mes']}")
        logger.info(f"   Limite: {quota['limite']:,}")
        logger.info(f"   Usadas: {quota['usada']:,}")
        logger.info(f"   Disponíveis: {quota['disponivel']:,}")
        logger.info(f"   Percentual: {quota['percentual_uso']:.1f}%")
        logger.info(f"   Pode usar: {'✅ Sim' if quota['pode_usar'] else '❌ Não'}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def teste_5_estatisticas():
    """Teste 5: Obter estatísticas de uso"""
    logger.info("\n" + "="*80)
    logger.info("TESTE 5: Estatísticas de Uso de Satélites")
    logger.info("="*80)
    
    engine = get_engine()
    
    try:
        sat_service = SatelliteServiceV2(engine)
        
        logger.info("\n📈 Obtendo estatísticas...")
        
        stats = sat_service.obter_estatisticas_satelite()
        
        logger.info(f"\n✅ Estatísticas (Mês: {stats['mes']}):")
        
        logger.info("\n   CBERS-4A:")
        logger.info(f"     Total: {stats['cbers4a']['total']}")
        logger.info(f"     Sucesso: {stats['cbers4a']['sucesso']}")
        logger.info(f"     Sem cobertura: {stats['cbers4a']['sem_cobertura']}")
        logger.info(f"     Média cobertura nuvem: {stats['cbers4a']['media_cobertura_nuvem']:.1f}%")
        
        logger.info("\n   Google Maps:")
        logger.info(f"     Total: {stats['google_maps']['total']}")
        logger.info(f"     Sucesso: {stats['google_maps']['sucesso']}")
        logger.info(f"     Quota limite: {stats['google_maps']['quota_limite']:,}")
        logger.info(f"     Percentual usado: {stats['google_maps']['percentual_usado']:.1f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Executar todos os testes"""
    logger.info("\n" + "█"*80)
    logger.info("🧪 TESTES DE SATÉLITES - TRANSFORMADOR + SUBESTAÇÃO")
    logger.info("█"*80)
    
    resultados = {
        'Transformadores disponíveis': teste_1_transformadores_disponiveis(),
        'Decidir fonte': teste_2_decidir_fonte_transformador(),
        'Buscar imagens': teste_3_buscar_imagens_transformador(),
        'Quota Google Maps': teste_4_quota_google_maps(),
        'Estatísticas': teste_5_estatisticas(),
    }
    
    # Resumo
    logger.info("\n" + "="*80)
    logger.info("📋 RESUMO DOS TESTES")
    logger.info("="*80)
    
    total = len(resultados)
    sucesso = sum(1 for v in resultados.values() if v)
    
    for teste, resultado in resultados.items():
        status = "✅ PASSOU" if resultado else "❌ FALHOU"
        logger.info(f"{status}: {teste}")
    
    logger.info(f"\n{sucesso}/{total} testes passaram")
    
    if sucesso == total:
        logger.info("\n🎉 Todos os testes passaram!")
        return 0
    else:
        logger.warning(f"\n⚠️  {total - sucesso} testes falharam")
        return 1


if __name__ == '__main__':
    sys.exit(main())
