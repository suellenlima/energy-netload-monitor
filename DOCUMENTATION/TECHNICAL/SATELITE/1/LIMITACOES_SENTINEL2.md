# ⚠️ LIMITAÇÕES DO SENTINEL-2 PARA DETECÇÃO DE TELHADOS

## 🔍 Problema Identificado

**As imagens Sentinel-2 estão muito distantes e não dá para ver telhados individuais!**

### Por quê?
- **Resolução Sentinel-2**: 10 metros por pixel
- **Tamanho típico de um telhado**: 10-20 metros
- **Resultado**: 1 telhado = 1-4 pixels apenas! ❌

## 📊 Comparação de Resoluções

| Satélite/Fonte | Resolução | Adequado para Telhados? | Custo |
|----------------|-----------|------------------------|-------|
| **Sentinel-2** | 10m/pixel | ❌ NÃO (áreas grandes) | Grátis |
| **Planet Labs** | 3m/pixel | ⚠️ Parcial (áreas médias) | Pago |
| **Maxar/WorldView** | 0.3-0.5m/pixel | ✅ SIM (telhados individuais) | Caro |
| **Google Maps** | 0.3-1m/pixel | ✅ SIM | Limitado |
| **Bing Maps** | 0.3-1m/pixel | ✅ SIM | Limitado |
| **Drones** | 0.02-0.05m/pixel | ✅ EXCELENTE | Variável |

## 🎯 Soluções Práticas

### **Opção 1: Usar Google Maps / Bing Maps (RECOMENDADO)** ⭐

#### Vantagens:
- ✅ Alta resolução (0.3-1m por pixel)
- ✅ Gratuito para uso limitado
- ✅ Cobertura global
- ✅ Imagens atualizadas

#### Como usar:

**A) Via Google Static Maps API:**
```python
import requests

def download_google_maps_image(lat, lon, zoom=20, size="640x640"):
    """
    Download imagem de alta resolução do Google Maps
    
    Args:
        lat, lon: Coordenadas
        zoom: 20 = máxima resolução (0.3-0.5m/pixel)
        size: Tamanho da imagem
    """
    api_key = "SUA_API_KEY_AQUI"
    url = f"https://maps.googleapis.com/maps/api/staticmap"
    params = {
        "center": f"{lat},{lon}",
        "zoom": zoom,
        "size": size,
        "maptype": "satellite",
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    return response.content
```

**B) Via Bing Maps API:**
```python
def download_bing_maps_image(lat, lon, zoom=20):
    """Download imagem Bing Maps"""
    api_key = "SUA_BING_KEY"
    url = f"https://dev.virtualearth.net/REST/v1/Imagery/Map/Aerial/{lat},{lon}/{zoom}"
    params = {"key": api_key}
    
    response = requests.get(url, params=params)
    return response.content
```

**C) Atualizar o Serviço para usar Google Maps:**
```python
# Modificar: backend/src/services/telhado_segmentation_service.py

class TelhadoSegmentationService:
    def __init__(self):
        self.google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        self.use_google_maps = True  # Usar Google Maps em vez de Sentinel-2
    
    def download_imagem_alta_resolucao(self, lat, lon):
        """
        Download de imagem de alta resolução
        Prioridade: Google Maps > Bing Maps > Sentinel-2
        """
        if self.google_maps_api_key:
            return self._download_google_maps(lat, lon, zoom=20)
        else:
            return self._download_sentinel2(lat, lon)
```

---

### **Opção 2: Usar APIs de Imagens Comerciais**

#### **2.1 Maxar SecureWatch (Melhor Qualidade)**
```python
# Resolução: 0.3-0.5m por pixel
# Custo: Pago
# API: https://securewatch.maxar.com/
```

#### **2.2 Planet Labs**
```python
# Resolução: 3m por pixel
# Custo: Pago (com plano gratuito limitado)
# API: https://www.planet.com/
```

---

### **Opção 3: Usar Sentinel-2 Apenas para Grandes Áreas**

**Quando Sentinel-2 é adequado:**
- ✅ Análise de **áreas industriais** grandes
- ✅ Detecção de **clusters** de edifícios
- ✅ Monitoramento de **expansão urbana**
- ✅ **Classificação de uso do solo**

**Não adequado para:**
- ❌ Telhados residenciais individuais
- ❌ Detecção precisa de painéis solares
- ❌ Análise de telhados pequenos

---

## 🔧 Implementação Prática

### **Script para Comparar Resoluções:**

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Compara resoluções de diferentes fontes de imagens
"""

import requests
from PIL import Image
import io

def comparar_resolucoes(lat, lon):
    """
    Baixa e compara imagens de diferentes fontes
    """
    print("=" * 70)
    print("🔍 COMPARAÇÃO DE RESOLUÇÕES DE IMAGEM")
    print("=" * 70)
    
    fontes = {
        "Sentinel-2": {
            "resolucao": "10m/pixel",
            "adequado": "❌ Não para telhados individuais",
            "custo": "Grátis"
        },
        "Google Maps": {
            "resolucao": "0.3-1m/pixel", 
            "adequado": "✅ Sim, excelente",
            "custo": "Grátis (limitado)"
        },
        "Planet Labs": {
            "resolucao": "3m/pixel",
            "adequado": "⚠️ Parcial",
            "custo": "Pago"
        },
        "Maxar": {
            "resolucao": "0.3-0.5m/pixel",
            "adequado": "✅ Sim, melhor qualidade",
            "custo": "Caro"
        }
    }
    
    print(f"\n📍 Coordenadas: {lat}, {lon}\n")
    
    for fonte, info in fontes.items():
        print(f"{fonte}:")
        print(f"   Resolução: {info['resolucao']}")
        print(f"   Adequado: {info['adequado']}")
        print(f"   Custo: {info['custo']}")
        print()

if __name__ == "__main__":
    # Exemplo: Subestação em Manaus
    comparar_resolucoes(-2.8928, -60.0321)
```

---

## 🚀 Solução Recomendada (Passo a Passo)

### **1. Configure Google Maps API**

```bash
# 1. Obtenha API Key em: https://console.cloud.google.com/
# 2. Ative: Maps Static API
# 3. Configure no .env:

# backend/.env
GOOGLE_MAPS_API_KEY=sua_chave_aqui
USE_GOOGLE_MAPS=true
USE_SENTINEL2=false
```

### **2. Atualize o Backend**

Crie novo serviço: `backend/src/services/google_maps_service.py`

```python
import os
import requests
from typing import Tuple
from PIL import Image
import io

class GoogleMapsService:
    """Serviço para download de imagens do Google Maps"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY não configurada")
    
    def download_imagem(
        self,
        latitude: float,
        longitude: float,
        zoom: int = 20,  # Máxima resolução
        width: int = 640,
        height: int = 640
    ) -> bytes:
        """
        Download imagem de satélite do Google Maps
        
        Args:
            latitude: Latitude
            longitude: Longitude
            zoom: Nível de zoom (1-20, sendo 20 o mais próximo)
            width: Largura em pixels (máx: 640)
            height: Altura em pixels (máx: 640)
        
        Returns:
            Bytes da imagem
        """
        url = "https://maps.googleapis.com/maps/api/staticmap"
        
        params = {
            "center": f"{latitude},{longitude}",
            "zoom": zoom,
            "size": f"{width}x{height}",
            "maptype": "satellite",
            "key": self.api_key,
            "scale": 2  # Dobra a resolução
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response.content
    
    def calcular_resolucao(self, zoom: int, latitude: float) -> float:
        """
        Calcula resolução em metros por pixel
        
        Em zoom=20, temos aproximadamente:
        - 0.3-0.5 metros por pixel (depende da latitude)
        """
        import math
        
        # Fórmula simplificada
        meters_per_pixel = (
            156543.03392 * math.cos(math.radians(latitude)) / (2 ** zoom)
        )
        return meters_per_pixel
```

### **3. Atualize o Endpoint**

```python
# backend/src/api/telhado.py

@router.post("/segmentar-subestacao-hd")
async def segmentar_subestacao_alta_resolucao(
    subestacao_id: int,
    usar_google_maps: bool = True
):
    """
    Segmentar telhados usando imagens de ALTA RESOLUÇÃO
    """
    # Buscar coordenadas da subestação
    subestacao = await obter_subestacao(subestacao_id)
    
    if usar_google_maps:
        # Usar Google Maps (0.3-1m/pixel)
        google_service = GoogleMapsService()
        imagem_bytes = google_service.download_imagem(
            subestacao.latitude,
            subestacao.longitude,
            zoom=20
        )
    else:
        # Usar Sentinel-2 (10m/pixel) - não recomendado
        sentinel_service = SentinelService()
        imagem_bytes = sentinel_service.download_imagem(...)
    
    # Processar com YOLOv8
    resultado = processar_telhados(imagem_bytes)
    
    return resultado
```

### **4. Teste com Alta Resolução**

```python
# test_alta_resolucao.py

import requests

def testar_google_maps():
    """Testa segmentação com Google Maps"""
    
    response = requests.post(
        "http://localhost:8000/telhados/segmentar-subestacao-hd",
        params={
            "subestacao_id": 1,
            "usar_google_maps": True
        }
    )
    
    resultado = response.json()
    print(f"Telhados detectados: {resultado['telhados_detectados']}")
    print(f"Resolução usada: ~0.5m/pixel (Google Maps)")

if __name__ == "__main__":
    testar_google_maps()
```

---

## 📊 Comparação Visual

### **Sentinel-2 (10m/pixel):**
```
Casa de 10x10m = 1 pixel! ❌
[█] <- Toda a casa em 1 pixel
```

### **Google Maps (0.5m/pixel):**
```
Casa de 10x10m = 20x20 pixels! ✅
[████████████████████]
[████████████████████]
[████████████████████]
[████████████████████]
...
```

---

## ⚡ Próximos Passos

### **1. Para usar agora (Google Maps):**
```bash
# 1. Obtenha API Key
# 2. Configure no .env
# 3. Execute:
python test_google_maps_resolucao.py
```

### **2. Para datasets de treinamento:**
- Use imagens aéreas de alta resolução
- Dataset recomendado: [Open Solar Map](https://github.com/opensolarmap)
- Ou: Crie seu próprio com Google Maps

### **3. Alternativas gratuitas:**
- **OpenStreetMap**: Imagens disponíveis via API
- **USGS Earth Explorer**: Imagens de alta resolução dos EUA
- **Copernicus**: Dados complementares

---

## 💡 Recomendação Final

**Para detecção de telhados e painéis solares:**

1. ✅ **Use Google Maps ou Bing Maps** (0.3-1m/pixel)
2. ❌ **NÃO use Sentinel-2** (10m/pixel é insuficiente)
3. ⚠️ **Sentinel-2 serve apenas para:**
   - Análise de grandes áreas industriais
   - Classificação de uso do solo
   - Detecção de mudanças em larga escala

**Custos estimados:**
- Google Maps: ~$0.002 por imagem (grátis até 25.000/mês)
- Maxar: ~$15-30 por km²
- Drones: ~$500-2000 por levantamento

---

## 🔗 Links Úteis

- [Google Maps Platform Pricing](https://developers.google.com/maps/billing/gmp-billing)
- [Bing Maps API](https://www.microsoft.com/en-us/maps/choose-your-bing-maps-api)
- [Maxar SecureWatch](https://securewatch.maxar.com/)
- [Planet Labs](https://www.planet.com/)
- [Open Solar Map Dataset](https://github.com/opensolarmap)

---

**🎯 Ação Imediata:** Configure Google Maps API para obter imagens de alta resolução!
