# 🇧🇷 IMAGENS DO INPE PARA DETECÇÃO DE TELHADOS

## 📡 Satélites e Dados Disponíveis no INPE

### **1. CBERS-4A (China-Brazil Earth Resources Satellite)**

#### **Sensor WPM (Wide Panchromatic and Multispectral Camera)**
- **Resolução:** 2 metros (PAN) / 8 metros (Multiespectral)
- **Adequado para telhados:** ✅ **SIM!** (Muito melhor que Sentinel-2)
- **Custo:** 🆓 **GRATUITO**
- **Cobertura:** América do Sul prioritária
- **Acesso:** http://www.dgi.inpe.br/catalogo/

#### **Comparação:**
```
Telhado 10x10m:
- Sentinel-2 (10m): 1 pixel ❌
- CBERS-4A WPM (2m): 25 pixels ✅
- Google Maps (0.5m): 400 pixels ✅✅
```

#### **Vantagens do CBERS-4A:**
- ✅ Gratuito e brasileiro
- ✅ Resolução 2m é boa para telhados grandes
- ✅ Dados abertos sem restrições
- ✅ Foco na América do Sul
- ✅ API disponível

#### **Limitações:**
- ⚠️ Revisita: ~31 dias (menos frequente)
- ⚠️ 2m não é suficiente para painéis solares pequenos
- ⚠️ Processamento mais complexo que Google Maps

---

### **2. Amazonia-1 (Satélite Brasileiro)**

#### **Sensor AWFI (Advanced Wide Field Imager)**
- **Resolução:** 64 metros
- **Adequado para telhados:** ❌ **NÃO** (Pior que Sentinel-2)
- **Uso:** Monitoramento ambiental em larga escala

---

### **3. GOES-16 (NOAA - Disponível via INPE)**
- **Resolução:** 500m - 2km
- **Adequado para telhados:** ❌ **NÃO**
- **Uso:** Meteorologia

---

### **4. Landsat-8/9 (USGS - Disponível via INPE)**
- **Resolução:** 15m (PAN) / 30m (Multiespectral)
- **Adequado para telhados:** ❌ **NÃO**
- **Uso:** Análise de uso do solo

---

## 🎯 Comparação Completa de Resoluções

| Fonte | Resolução | Pixels (telhado 10x10m) | Adequado? | Custo | Acesso |
|-------|-----------|-------------------------|-----------|-------|--------|
| **Sentinel-2** | 10m | 1 pixel | ❌ NÃO | Grátis | Planetary Computer |
| **CBERS-4A WPM** | 2m | 25 pixels | ✅ SIM | Grátis | INPE Catálogo |
| **Landsat-8** | 15m | 0.44 pixels | ❌ NÃO | Grátis | INPE/USGS |
| **Google Maps** | 0.15-0.6m | 281-4498 pixels | ✅ **EXCELENTE** | Grátis (25k/mês) | Google API |
| **Bing Maps** | 0.3-1m | 100-1000 pixels | ✅ EXCELENTE | Grátis (limite) | Bing API |
| **Maxar** | 0.3m | 1024 pixels | ✅ EXCELENTE | Caro | SecureWatch |

---

## 🚀 Como Usar CBERS-4A do INPE

### **Opção 1: Portal de Catálogo do INPE**

#### **1. Acesso Web:**
```
http://www2.dgi.inpe.br/catalogo/explore
```

#### **2. Buscar imagens:**
- Selecione: **CBERS-4A**
- Sensor: **WPM**
- Desenhe área de interesse no mapa
- Filtro de nuvens: < 30%
- Download gratuito (requer cadastro)

---

### **Opção 2: API BDC (Brazil Data Cube)**

O **Brazil Data Cube** é uma plataforma moderna do INPE com API:

```python
from bdc_catalog import BDCCatalog

# Conectar ao BDC
catalog = BDCCatalog()

# Buscar coleções CBERS
collections = catalog.collections
cbers = [c for c in collections if 'CBERS' in c.name]

# Buscar itens
items = catalog.search(
    collections=['CBERS-4A-WPM'],
    bbox=[-60.1, -3.0, -60.0, -2.9],  # Manaus
    datetime='2025-01-01/2026-01-30',
    cloud_cover=30
)

# Download
for item in items:
    print(f"Image: {item.id}")
    print(f"Cloud cover: {item.properties['eo:cloud_cover']}")
    print(f"URL: {item.assets['pan'].href}")
```

#### **Instalação:**
```bash
pip install bdc-catalog
```

---

### **Opção 3: API STAC do INPE**

```python
from pystac_client import Client

# Conectar ao STAC do INPE
stac_url = "https://data.inpe.br/bdc/stac/v1"
catalog = Client.open(stac_url)

# Buscar CBERS-4A
search = catalog.search(
    collections=['CBERS-4A-WPM-L4-SR'],
    bbox=[-60.1, -3.0, -60.0, -2.9],
    datetime='2025-01-01/2026-01-30',
    max_items=10
)

items = list(search.get_items())
print(f"Encontradas {len(items)} imagens CBERS-4A")

for item in items:
    print(f"Data: {item.properties['datetime']}")
    print(f"Nuvens: {item.properties.get('eo:cloud_cover', 'N/A')}")
```

---

## 🔧 Implementação no Backend

### **Criar Serviço INPE:**

```python
# backend/src/services/inpe_service.py

import requests
from typing import List, Dict, Optional
from datetime import datetime

class INPEService:
    """Serviço para buscar imagens CBERS-4A do INPE"""
    
    def __init__(self):
        self.stac_url = "https://data.inpe.br/bdc/stac/v1"
        self.catalog = None
    
    def buscar_imagens_cbers4a(
        self,
        latitude: float,
        longitude: float,
        raio_km: float = 5,
        data_inicio: str = None,
        data_fim: str = None,
        cobertura_nuvem_max: int = 30
    ) -> List[Dict]:
        """
        Busca imagens CBERS-4A WPM (2m resolução)
        
        Args:
            latitude: Latitude central
            longitude: Longitude central
            raio_km: Raio de busca em km
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            cobertura_nuvem_max: Nuvens máximas (0-100)
        
        Returns:
            Lista de imagens encontradas
        """
        from pystac_client import Client
        
        # Calcular bounding box
        delta = raio_km / 111  # aproximação
        bbox = [
            longitude - delta,
            latitude - delta,
            longitude + delta,
            latitude + delta
        ]
        
        # Conectar ao STAC
        catalog = Client.open(self.stac_url)
        
        # Buscar
        search = catalog.search(
            collections=['CBERS-4A-WPM-L4-SR'],
            bbox=bbox,
            datetime=f"{data_inicio}/{data_fim}",
            max_items=50
        )
        
        items = list(search.get_items())
        
        # Filtrar por cobertura de nuvens
        resultados = []
        for item in items:
            cloud_cover = item.properties.get('eo:cloud_cover', 100)
            
            if cloud_cover <= cobertura_nuvem_max:
                resultados.append({
                    'id': item.id,
                    'data': item.properties['datetime'],
                    'cobertura_nuvem': cloud_cover,
                    'resolucao': '2m',
                    'sensor': 'CBERS-4A WPM',
                    'url_pan': item.assets.get('pan', {}).get('href'),
                    'url_red': item.assets.get('red', {}).get('href'),
                    'url_green': item.assets.get('green', {}).get('href'),
                    'url_blue': item.assets.get('blue', {}).get('href'),
                })
        
        # Ordenar por cobertura de nuvens
        resultados.sort(key=lambda x: x['cobertura_nuvem'])
        
        return resultados
    
    def download_imagem(self, url: str) -> bytes:
        """Download de banda individual"""
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.content
```

### **Adicionar Endpoint:**

```python
# backend/src/api/satelite.py

from ..services.inpe_service import INPEService

@router.post("/inpe/cbers4a/{subestacao_id}")
async def buscar_imagens_cbers4a(
    subestacao_id: int,
    data_inicio: str = Query(default=(datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")),
    data_fim: str = Query(default=datetime.now().strftime("%Y-%m-%d")),
    raio_km: float = Query(default=5.0),
    cobertura_nuvem_max: int = Query(default=30)
):
    """
    Busca imagens CBERS-4A do INPE
    Resolução: 2 metros (PAN)
    """
    # Buscar coordenadas da subestação
    subestacao = await obter_subestacao(subestacao_id)
    
    # Buscar imagens CBERS
    inpe_service = INPEService()
    imagens = inpe_service.buscar_imagens_cbers4a(
        latitude=subestacao.latitude,
        longitude=subestacao.longitude,
        raio_km=raio_km,
        data_inicio=data_inicio,
        data_fim=data_fim,
        cobertura_nuvem_max=cobertura_nuvem_max
    )
    
    return {
        "subestacao_id": subestacao_id,
        "subestacao": {
            "nome": subestacao.nome,
            "latitude": subestacao.latitude,
            "longitude": subestacao.longitude
        },
        "imagens_encontradas": len(imagens),
        "imagens": imagens,
        "fonte": "INPE CBERS-4A WPM",
        "resolucao": "2 metros por pixel"
    }
```

---

## 📊 Quando Usar Cada Fonte?

### **1. CBERS-4A (INPE) - 2m** 🇧🇷
**Use quando:**
- ✅ Precisa de imagens do Brasil/América do Sul
- ✅ Quer dados gratuitos e abertos
- ✅ Detectar grandes telhados industriais/comerciais
- ✅ Não precisa de resolução sub-métrica

**Não use para:**
- ❌ Painéis solares residenciais pequenos
- ❌ Detecção de detalhes finos (<2m)
- ❌ Necessita de revisita frequente

---

### **2. Google Maps - 0.15-0.6m** 🌍
**Use quando:**
- ✅ Precisa da MELHOR resolução
- ✅ Detectar telhados residenciais
- ✅ Identificar painéis solares
- ✅ Cobertura global
- ✅ Até 25.000 imagens grátis/mês

**Não use para:**
- ❌ Análise temporal (imagens não datadas)
- ❌ Processamento científico rigoroso

---

### **3. Sentinel-2 - 10m** 🛰️
**Use quando:**
- ✅ Análise de uso do solo
- ✅ Grandes áreas industriais (>100m)
- ✅ Classificação de vegetação
- ✅ Revisita frequente (5 dias)

**Não use para:**
- ❌ Telhados residenciais
- ❌ Detalhes urbanos
- ❌ Qualquer estrutura <50m

---

## 🎯 Recomendação Final

### **Para Detecção de Telhados no Brasil:**

**🥇 Melhor:** Google Maps (0.15-0.6m)
- Resolução excelente
- Grátis até 25k/mês
- Cobertura completa
- API simples

**🥈 Segunda Opção:** CBERS-4A INPE (2m)
- Dados brasileiros
- Totalmente gratuito
- Boa resolução para grandes telhados
- Ideal para áreas industriais

**🥉 Terceira Opção:** Bing Maps (0.3-1m)
- Alternativa ao Google
- Boa resolução
- Grátis com limites

**❌ Evitar:** Sentinel-2 (10m)
- Apenas para análise de grandes áreas

---

## 📚 Links Úteis

### **INPE:**
- **Catálogo:** http://www2.dgi.inpe.br/catalogo/
- **Brazil Data Cube:** https://data.inpe.br/bdc/
- **STAC API:** https://data.inpe.br/bdc/stac/v1
- **Documentação:** http://www.cbers.inpe.br/

### **Google Maps:**
- **API:** https://developers.google.com/maps/documentation/maps-static
- **Console:** https://console.cloud.google.com/

### **Tutoriais:**
- **CBERS-4A Python:** https://github.com/brazil-data-cube/bdc-catalog
- **Comparação de Satélites:** https://eos.com/blog/satellite-data/

---

## 🧪 Script de Teste

```bash
# Testar CBERS-4A do INPE
python test_inpe_cbers4a.py

# Testar Google Maps
python test_google_maps_resolucao.py

# Comparar todas as fontes
python comparar_fontes_satelite.py
```

---

## 💡 Conclusão

**Para seu projeto:**

1. **Imediato:** Use **Google Maps** (melhor resolução, mais simples)
2. **Alternativa brasileira:** Use **CBERS-4A** (2m, gratuito INPE)
3. **Evite:** Sentinel-2 para telhados individuais

**Combinação ideal:**
- Google Maps para telhados residenciais (0.5m)
- CBERS-4A para grandes áreas industriais (2m)
- Sentinel-2 para contexto regional (10m)
