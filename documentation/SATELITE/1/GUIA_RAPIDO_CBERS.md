# 🚀 Guia Rápido: Usar CBERS-4A na Aplicação

## ⚡ Início Rápido (5 minutos)

### 1. Testar sem servidor

```bash
cd c:\Hackathon\Git\energy-netload-monitor
python test_cbers_integration.py
```

**Resultado esperado:** ✅ "CBERSService - Buscar Imagens" passa

---

### 2. Iniciar o servidor

```bash
cd backend
python -m uvicorn src.main:app --reload
```

**URL:** http://localhost:8000

---

### 3. Testar endpoints CBERS

#### Via Swagger UI (mais fácil)

1. Abra: http://localhost:8000/docs
2. Vá para a seção "**Satélite**"
3. Procure por **`/satelite/cbers/{subestacao_id}/buscar`**
4. Clique em "**Try it out**"
5. Preencha:
   - `subestacao_id`: **1**
   - `raio_km`: **10**
   - `cobertura_nuvem_max`: **30**
6. Clique em "**Execute**"

#### Via cURL

```bash
curl -X GET "http://localhost:8000/satelite/cbers/1/buscar?raio_km=10&cobertura_nuvem_max=30"
```

#### Via Python

```python
import requests

response = requests.get(
    "http://localhost:8000/satelite/cbers/1/buscar",
    params={
        "raio_km": 10.0,
        "cobertura_nuvem_max": 30.0
    }
)

data = response.json()
print(f"Total imagens: {data['total_imagens']}")
```

---

## 📊 Endpoints Disponíveis

### 1. Buscar Imagens CBERS

**Endpoint:** `GET /satelite/cbers/{subestacao_id}/buscar`

**Parâmetros:**
- `subestacao_id` (path): ID da subestação
- `data_inicio` (query, opcional): Data início (YYYY-MM-DD)
- `data_fim` (query, opcional): Data fim (YYYY-MM-DD)
- `raio_km` (query, opcional): Raio de busca (default 5.0)
- `cobertura_nuvem_max` (query, opcional): % máx de nuvens (default 30.0)

**Exemplo de resposta:**
```json
{
  "subestacao": {
    "id": 1,
    "nome": "SE_DETECTADA_0",
    "latitude": -15.7939,
    "longitude": -47.8828,
    "distribuidora": "CEMIG"
  },
  "parametros_busca": {
    "data_inicio": "2024-07-30",
    "data_fim": "2026-01-30",
    "raio_km": 10.0,
    "cobertura_nuvem_max": 30.0
  },
  "total_imagens": 2,
  "imagens": [
    {
      "id": "CBERS4A_WPM_20241015_167_142_L4",
      "data_aquisicao": "2024-10-15T13:22:15",
      "sensor": "CBERS-4A WPM",
      "resolucao_m": 2,
      "cobertura_nuvem_pct": 12.5,
      "urls": {
        "pan": "https://data.inpe.br/.../pan.tif",
        "red": "https://data.inpe.br/.../red.tif",
        "green": "https://data.inpe.br/.../green.tif",
        "blue": "https://data.inpe.br/.../blue.tif"
      },
      "bbox": {
        "min_lon": -47.973,
        "min_lat": -15.884,
        "max_lon": -47.793,
        "max_lat": -15.704
      }
    }
  ]
}
```

---

### 2. Download de Banda

**Endpoint:** `GET /satelite/cbers/download-banda/{image_id}`

**Parâmetros:**
- `image_id` (path): ID da imagem CBERS
- `banda` (query): Nome da banda (pan, red, green, blue, nir)
- `bbox` (query, opcional): Recorte (min_lon,min_lat,max_lon,max_lat)

**Exemplo:**
```bash
curl "http://localhost:8000/satelite/cbers/download-banda/CBERS4A_WPM_20241015_167_142_L4?banda=red"
```

**Resposta:**
```json
{
  "image_id": "CBERS4A_WPM_20241015_167_142_L4",
  "banda": "red",
  "shape": [5000, 5000],
  "dtype": "uint16",
  "min": 0,
  "max": 10000,
  "mean": 3567.8
}
```

---

### 3. Composição RGB

**Endpoint:** `GET /satelite/cbers/composicao-rgb/{image_id}`

**Parâmetros:**
- `image_id` (path): ID da imagem CBERS
- `bbox` (query, opcional): Recorte
- `salvar_caminho` (query, opcional): Caminho para salvar PNG

**Exemplo:**
```bash
curl "http://localhost:8000/satelite/cbers/composicao-rgb/CBERS4A_WPM_20241015_167_142_L4?salvar_caminho=output.png"
```

---

## 🧪 Testes Manuais

### Teste 1: Verificar se serviço funciona

```python
from backend.src.services.cbers_service import CBERSService

service = CBERSService()
print(f"STAC URL: {service.stac_url}")
print(f"Coleção: {service.colecao_padrao}")

# Buscar imagens para Brasília
imagens = service.buscar_imagens(
    latitude=-15.7939,
    longitude=-47.8828,
    raio_km=10.0,
    data_inicio="2024-01-01",
    data_fim="2025-12-31"
)

print(f"Encontradas {len(imagens)} imagens")
for img in imagens:
    print(f"  - {img.id}: {img.data} ({img.cobertura_nuvem}% nuvens)")
```

---

### Teste 2: Baixar e visualizar imagem

```python
from backend.src.services.cbers_service import CBERSService
import matplotlib.pyplot as plt

service = CBERSService()

# Buscar imagens
imagens = service.buscar_imagens(
    latitude=-15.7939,
    longitude=-47.8828,
    raio_km=10.0,
    data_inicio="2024-01-01",
    data_fim="2025-12-31"
)

if imagens:
    # Criar composição RGB da primeira imagem
    img = imagens[0]
    rgb = service.criar_composicao_rgb(
        image_id=img.id,
        salvar_caminho="cbers_test.png"
    )
    
    print(f"✓ Imagem salva: cbers_test.png")
    print(f"  Tamanho: {rgb.size}")
    print(f"  Resolução: 2 metros/pixel")
    
    # Visualizar
    plt.imshow(rgb)
    plt.title(f"CBERS-4A: {img.data}")
    plt.axis('off')
    plt.show()
else:
    print("⚠ Nenhuma imagem encontrada")
```

---

## ⚠️ Troubleshooting

### Erro: "Nenhuma imagem encontrada"

**Causa:** CBERS-4A tem menor cobertura temporal que Sentinel-2

**Soluções:**
1. Aumentar período de busca (6-12 meses)
2. Testar com coordenadas do Brasil central/sudeste
3. Aumentar raio de busca para 20-30 km

**Exemplo:**
```python
# Busca mais ampla
imagens = service.buscar_imagens(
    latitude=-15.7939,
    longitude=-47.8828,
    raio_km=30.0,  # Raio maior
    data_inicio="2023-01-01",  # Período maior
    data_fim="2025-12-31",
    cobertura_nuvem_max=50  # Aceitar mais nuvens
)
```

---

### Erro: "Endpoint retorna 404"

**Causa:** Servidor não está rodando ou URL incorreta

**Verificar:**
1. Servidor rodando em http://localhost:8000
2. URL correta: `/satelite/cbers/...` (sem `/api/`)

**Teste:**
```bash
# Verificar se servidor está rodando
curl http://localhost:8000/health

# URL correta
curl "http://localhost:8000/satelite/cbers/1/buscar?raio_km=10"
```

---

### Erro: "pystac_client não encontrado"

**Causa:** Dependência não instalada

**Solução:**
```bash
cd backend
pip install pystac-client
```

---

### Erro: "rasterio não encontrado"

**Causa:** Dependência não instalada

**Solução:**
```bash
pip install rasterio
```

**No Windows pode precisar de:**
```bash
pip install rasterio --find-links=https://www.lfd.uci.edu/~gohlke/pythonlibs/
```

---

## 📝 Checklist de Validação

Antes de usar em produção, verifique:

- [ ] `python test_cbers_integration.py` passa
- [ ] Servidor inicia sem erros: `python -m uvicorn src.main:app --reload`
- [ ] Swagger UI carrega: http://localhost:8000/docs
- [ ] Endpoint `/satelite/cbers/1/buscar` retorna 200
- [ ] Pelo menos 1 imagem é encontrada para alguma subestação
- [ ] Composição RGB é criada com sucesso
- [ ] Arquivo PNG é salvo corretamente

---

## 🔄 Próximos Passos

### 1. Integrar com Frontend

Atualizar chamadas de API no frontend:

```javascript
// Antes (Sentinel-2)
const response = await fetch(`/api/satelite/planetary-computer/${id}`);

// Agora (CBERS-4A)
const response = await fetch(`/satelite/cbers/${id}/buscar`);
```

---

### 2. Atualizar Pipeline de Telhados

Modificar para usar CBERS-4A:

```python
# Em backend/src/services/telhado_segmentation_service.py

# Antes
imagem = baixar_sentinel2(subestacao_id)

# Agora
from .cbers_service import CBERSService
service = CBERSService()
imagens = service.buscar_imagens(lat, lon, raio_km=5)
if imagens:
    rgb = service.criar_composicao_rgb(imagens[0].id)
    # Processar com YOLOv8...
```

---

### 3. Implementar Cache

Para evitar downloads repetidos:

```python
import hashlib
import os

def download_com_cache(image_id, banda):
    cache_dir = "data/cache/cbers"
    os.makedirs(cache_dir, exist_ok=True)
    
    cache_key = hashlib.md5(f"{image_id}_{banda}".encode()).hexdigest()
    cache_path = f"{cache_dir}/{cache_key}.tif"
    
    if os.path.exists(cache_path):
        print(f"✓ Usando cache: {cache_path}")
        return rasterio.open(cache_path).read(1)
    else:
        data = service.download_banda(image_id, banda)
        # Salvar no cache...
        return data
```

---

## 📚 Documentação Adicional

- **Visão geral:** `MIGRACAO_CBERS.md`
- **Limitações Sentinel-2:** `LIMITACOES_SENTINEL2.md`
- **Guia completo INPE:** `IMAGENS_INPE.md`
- **Código-fonte:** `backend/src/services/cbers_service.py`

---

**Última atualização:** 2025-01-30  
**Status:** ✅ Pronto para uso  
**Suporte:** Execute `python test_cbers_integration.py` para validar
