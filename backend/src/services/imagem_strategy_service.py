"""
Serviço de Estratégia Híbrida para Imagens de Satélite

Implementa fallback automático:
1. CBERS-4A (2m, grátis) - Primeira escolha
2. Google Maps (0.3-0.6m, 25k grátis/mês) - Fallback
3. Sentinel-2 (10m, grátis) - Última opção
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List
from pathlib import Path
import numpy as np
from dotenv import load_dotenv

# Carregar variáveis de ambiente do .env
env_path = Path(__file__).parent.parent.parent.parent / 'backend' / '.env'
if not env_path.exists():
    env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

logger = logging.getLogger(__name__)


@dataclass
class ImagemObtida:
    """Resultado de busca de imagem"""
    fonte: str  # "cbers", "google_maps", "sentinel"
    imagem: np.ndarray
    resolucao_m: float
    latitude: float
    longitude: float
    timestamp: datetime
    metadata: dict


class ImagemStrategyService:
    """
    Gerenciador de estratégia híbrida para obtenção de imagens
    
    Implementa fallback automático baseado em disponibilidade e qualidade
    """
    
    def __init__(self, preferencia_resolucao: float = 2.0):
        """
        Inicializa estratégia
        
        Args:
            preferencia_resolucao: Resolução preferida em metros/pixel
                                   2.0 = CBERS-4A
                                   0.5 = Google Maps alta resolução
                                   10.0 = Sentinel-2
        """
        self.preferencia_resolucao = preferencia_resolucao
        
        # Inicializar serviços
        from .cbers_service import CBERSService
        from .google_maps_service import GoogleMapsService
        from .cache_service import CacheService
        
        self.cbers = CBERSService()
        self.google_maps = GoogleMapsService()
        self.cache = CacheService(cache_dir="data/cache/hibrido")
        
        # Estatísticas de uso
        self.stats = {
            "cbers": {"tentativas": 0, "sucessos": 0},
            "google_maps": {"tentativas": 0, "sucessos": 0},
            "sentinel": {"tentativas": 0, "sucessos": 0}
        }
        
        logger.info("Estratégia híbrida inicializada")
        logger.info(f"  Resolução preferida: {preferencia_resolucao}m/pixel")
        logger.info(f"  CBERS disponível: ✓")
        logger.info(f"  Google Maps disponível: {'✓' if self.google_maps.esta_disponivel() else '✗'}")
    
    def buscar_imagem_automatica(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5.0,
        usar_cache: bool = True,
        estrategia: str = "auto"
    ) -> Optional[ImagemObtida]:
        """
        Busca imagem com fallback automático
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            raio_km: Raio de busca (para CBERS)
            usar_cache: Usar cache se disponível
            estrategia: "auto", "alta_resolucao", "custo_zero", "rapido"
        
        Returns:
            ImagemObtida ou None se todas as fontes falharam
        """
        logger.info(f"Buscando imagem com estratégia '{estrategia}'")
        logger.info(f"  Localização: ({latitude}, {longitude})")
        logger.info(f"  Raio: {raio_km} km")
        
        # Definir ordem de tentativa baseado na estratégia
        ordem = self._definir_ordem_tentativa(estrategia)
        
        for fonte in ordem:
            logger.info(f"\n→ Tentando fonte: {fonte}")
            self.stats[fonte]["tentativas"] += 1
            
            try:
                resultado = self._buscar_de_fonte(
                    fonte=fonte,
                    latitude=latitude,
                    longitude=longitude,
                    raio_km=raio_km,
                    usar_cache=usar_cache
                )
                
                if resultado:
                    self.stats[fonte]["sucessos"] += 1
                    logger.info(f"✓ Imagem obtida de {fonte}")
                    logger.info(f"  Resolução: {resultado.resolucao_m}m/pixel")
                    logger.info(f"  Shape: {resultado.imagem.shape}")
                    return resultado
                else:
                    logger.warning(f"✗ {fonte} não retornou imagem")
                    
            except Exception as e:
                logger.error(f"✗ Erro ao buscar de {fonte}: {e}")
        
        # Todas as fontes falharam
        logger.error("✗ FALHA: Nenhuma fonte de imagem disponível")
        self._log_estatisticas()
        return None
    
    def _definir_ordem_tentativa(self, estrategia: str) -> List[str]:
        """Define ordem de tentativa baseado na estratégia"""
        
        if estrategia == "alta_resolucao":
            # Priorizar melhor resolução
            if self.google_maps.esta_disponivel():
                return ["google_maps", "cbers", "sentinel"]
            else:
                return ["cbers", "sentinel"]
        
        elif estrategia == "custo_zero":
            # Apenas fontes gratuitas
            return ["cbers", "sentinel"]
        
        elif estrategia == "rapido":
            # Priorizar cache e CBERS (mais rápido)
            return ["cbers", "google_maps", "sentinel"]
        
        else:  # "auto"
            # Estratégia inteligente baseada em resolução preferida
            if self.preferencia_resolucao <= 1.0:
                # Alta resolução necessária
                if self.google_maps.esta_disponivel():
                    return ["google_maps", "cbers", "sentinel"]
                else:
                    return ["cbers", "sentinel"]
            elif self.preferencia_resolucao <= 5.0:
                # CBERS suficiente
                return ["cbers", "google_maps", "sentinel"]
            else:
                # Baixa resolução aceitável
                return ["cbers", "sentinel", "google_maps"]
    
    def _buscar_de_fonte(
        self,
        fonte: str,
        latitude: float,
        longitude: float,
        raio_km: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem de uma fonte específica"""
        
        if fonte == "cbers":
            return self._buscar_cbers(latitude, longitude, raio_km, usar_cache)
        
        elif fonte == "google_maps":
            return self._buscar_google_maps(latitude, longitude, usar_cache)
        
        elif fonte == "sentinel":
            return self._buscar_sentinel(latitude, longitude, usar_cache)
        
        else:
            logger.error(f"Fonte desconhecida: {fonte}")
            return None
    
    def _buscar_cbers(
        self,
        latitude: float,
        longitude: float,
        raio_km: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem CBERS-4A"""
        
        # Período amplo para aumentar chances
        data_fim = datetime.now().strftime("%Y-%m-%d")
        data_inicio = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")  # 2 anos
        
        logger.info(f"  Buscando CBERS (período: {data_inicio} a {data_fim})")
        
        imagens = self.cbers.buscar_imagens(
            latitude=latitude,
            longitude=longitude,
            raio_km=raio_km,
            data_inicio=data_inicio,
            data_fim=data_fim,
            cobertura_nuvem_max=50.0  # Mais tolerante
        )
        
        if not imagens:
            logger.warning(f"  Nenhuma imagem CBERS encontrada")
            return None
        
        # Usar primeira imagem (menor cobertura de nuvens)
        imagem_cbers = imagens[0]
        logger.info(f"  Imagem encontrada: {imagem_cbers.id}")
        logger.info(f"    Data: {imagem_cbers.data}")
        logger.info(f"    Nuvens: {imagem_cbers.cobertura_nuvem}%")
        
        # Download RGB
        rgb = self.cbers.criar_composicao_rgb(image_id=imagem_cbers.id)
        
        if rgb is None:
            return None
        
        return ImagemObtida(
            fonte="cbers",
            imagem=np.array(rgb),
            resolucao_m=2.0,
            latitude=latitude,
            longitude=longitude,
            timestamp=imagem_cbers.data,
            metadata={
                "id": imagem_cbers.id,
                "sensor": imagem_cbers.sensor,
                "nuvens": imagem_cbers.cobertura_nuvem
            }
        )
    
    def _buscar_google_maps(
        self,
        latitude: float,
        longitude: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem Google Maps"""
        
        if not self.google_maps.esta_disponivel():
            logger.warning("  Google Maps API key não configurada")
            return None
        
        # Zoom 20 = ~0.3m/pixel (alta resolução)
        zoom = 20
        logger.info(f"  Buscando Google Maps (zoom={zoom})")
        
        imagem = self.google_maps.buscar_imagem_satelite(
            latitude=latitude,
            longitude=longitude,
            zoom=zoom,
            tamanho=(640, 640)
        )
        
        if imagem is None:
            return None
        
        return ImagemObtida(
            fonte="google_maps",
            imagem=imagem,
            resolucao_m=0.3,  # Aproximado para zoom 20
            latitude=latitude,
            longitude=longitude,
            timestamp=datetime.now(),
            metadata={
                "zoom": zoom,
                "api": "google_maps_static"
            }
        )
    
    def _buscar_sentinel(
        self,
        latitude: float,
        longitude: float,
        usar_cache: bool
    ) -> Optional[ImagemObtida]:
        """Busca imagem Sentinel-2 (fallback final)"""
        
        logger.warning("  Sentinel-2 não implementado nesta estratégia")
        logger.warning("  Resolução 10m inadequada para telhados")
        return None
    
    def _log_estatisticas(self):
        """Log de estatísticas de uso"""
        logger.info("\n" + "="*60)
        logger.info("ESTATÍSTICAS DE USO")
        logger.info("="*60)
        
        for fonte, stats in self.stats.items():
            tentativas = stats["tentativas"]
            sucessos = stats["sucessos"]
            taxa = (sucessos / tentativas * 100) if tentativas > 0 else 0
            
            logger.info(f"{fonte.upper():<15} {tentativas:>3} tentativas | {sucessos:>3} sucessos | {taxa:>5.1f}%")
    