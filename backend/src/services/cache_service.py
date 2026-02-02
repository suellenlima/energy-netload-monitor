"""
Serviço de Cache para Imagens de Satélite

Gerencia cache local de imagens CBERS-4A para evitar downloads repetidos.
Suporta múltiplas estratégias de cache e limpeza automática.

"""

import hashlib
import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

import numpy as np

logger = logging.getLogger(__name__)


class CacheService:
    """Serviço de cache para imagens de satélite"""
    
    def __init__(self, cache_dir: str = "data/cache/cbers", max_age_days: int = 30):
        """
        Inicializa o serviço de cache
        
        Args:
            cache_dir: Diretório base para cache
            max_age_days: Idade máxima de arquivos em cache (dias)
        """
        self.cache_dir = Path(cache_dir)
        self.max_age_days = max_age_days
        self.metadata_file = self.cache_dir / "cache_metadata.json"
        
        # Criar diretório se não existir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Carregar metadados
        self.metadata = self._load_metadata()
        
        logger.info(f"Cache inicializado: {self.cache_dir}")
        logger.info(f"Max idade: {max_age_days} dias")
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Carrega metadados do cache"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Erro ao carregar metadata: {e}")
        
        return {
            "created": datetime.now().isoformat(),
            "entries": {},
            "stats": {
                "hits": 0,
                "misses": 0,
                "total_size_mb": 0
            }
        }
    
    def _save_metadata(self):
        """Salva metadados do cache"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar metadata: {e}")
    
    def _generate_cache_key(self, image_id: str, banda: str, bbox: Optional[tuple] = None) -> str:
        """
        Gera chave única para cache
        
        Args:
            image_id: ID da imagem CBERS
            banda: Nome da banda
            bbox: Bbox opcional
            
        Returns:
            Hash MD5 da chave
        """
        key_parts = [image_id, banda]
        if bbox:
            key_parts.append(f"{bbox[0]:.6f}_{bbox[1]:.6f}_{bbox[2]:.6f}_{bbox[3]:.6f}")
        
        key_string = "_".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Retorna caminho completo do arquivo em cache"""
        # Organizar em subdiretórios usando primeiros 2 caracteres do hash
        subdir = self.cache_dir / cache_key[:2]
        subdir.mkdir(exist_ok=True)
        return subdir / f"{cache_key}.npy"
    
    def get(self, image_id: str, banda: str, bbox: Optional[tuple] = None) -> Optional[np.ndarray]:
        """
        Recupera imagem do cache
        
        Args:
            image_id: ID da imagem CBERS
            banda: Nome da banda
            bbox: Bbox opcional
            
        Returns:
            Array numpy ou None se não encontrado
        """
        cache_key = self._generate_cache_key(image_id, banda, bbox)
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            logger.debug(f"Cache MISS: {cache_key}")
            self.metadata["stats"]["misses"] += 1
            self._save_metadata()
            return None
        
        try:
            # Verificar idade do arquivo
            age_days = (datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)).days
            if age_days > self.max_age_days:
                logger.info(f"Cache expirado ({age_days} dias): {cache_key}")
                cache_path.unlink()
                return None
            
            # Carregar dados
            data = np.load(cache_path)
            
            # Atualizar estatísticas
            self.metadata["stats"]["hits"] += 1
            if cache_key in self.metadata["entries"]:
                self.metadata["entries"][cache_key]["last_access"] = datetime.now().isoformat()
                self.metadata["entries"][cache_key]["access_count"] += 1
            
            self._save_metadata()
            
            logger.debug(f"Cache HIT: {cache_key} ({data.shape})")
            return data
            
        except Exception as e:
            logger.error(f"Erro ao carregar do cache: {e}")
            # Remover arquivo corrompido
            if cache_path.exists():
                cache_path.unlink()
            return None
    
    def put(self, image_id: str, banda: str, data: np.ndarray, bbox: Optional[tuple] = None) -> bool:
        """
        Armazena imagem no cache
        
        Args:
            image_id: ID da imagem CBERS
            banda: Nome da banda
            data: Array numpy com dados
            bbox: Bbox opcional
            
        Returns:
            True se sucesso
        """
        try:
            cache_key = self._generate_cache_key(image_id, banda, bbox)
            cache_path = self._get_cache_path(cache_key)
            
            # Salvar dados
            np.save(cache_path, data)
            
            # Atualizar metadados
            size_mb = cache_path.stat().st_size / (1024 * 1024)
            self.metadata["entries"][cache_key] = {
                "image_id": image_id,
                "banda": banda,
                "bbox": bbox,
                "created": datetime.now().isoformat(),
                "last_access": datetime.now().isoformat(),
                "access_count": 0,
                "size_mb": round(size_mb, 2),
                "shape": data.shape
            }
            
            self._save_metadata()
            
            logger.info(f"Cache armazenado: {cache_key} ({size_mb:.2f} MB)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao armazenar no cache: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do cache"""
        total_size = sum(e.get("size_mb", 0) for e in self.metadata["entries"].values())
        total_entries = len(self.metadata["entries"])
        
        stats = {
            **self.metadata["stats"],
            "total_entries": total_entries,
            "total_size_mb": round(total_size, 2),
            "cache_dir": str(self.cache_dir),
            "max_age_days": self.max_age_days
        }
        
        # Calcular hit rate
        total_requests = stats["hits"] + stats["misses"]
        if total_requests > 0:
            stats["hit_rate"] = round(stats["hits"] / total_requests * 100, 2)
        else:
            stats["hit_rate"] = 0.0
        
        return stats
    