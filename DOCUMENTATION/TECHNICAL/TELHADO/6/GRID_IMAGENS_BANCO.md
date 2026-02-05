# Registro de Imagens Grid do Google Maps no Banco de Dados

## 📋 Visão Geral

Este documento explica como registrar as imagens-grid do endpoint Google Maps no banco de dados, reaproveitando a infraestrutura existente.

---

## 🎯 Endpoints Disponíveis

### 1. **GET /satelite/v2/google-maps/transformador/{id}/imagens-grid**
Retorna URLs das imagens do grid **sem salvar no banco**

### 2. **POST /satelite/v2/google-maps/transformador/{id}/imagens-grid/salvar** ✨ **NOVO**
Busca imagens do grid **e salva no banco de dados**

---

## 🆕 Novo Endpoint: Salvar Grid no Banco

### Endpoint
```
POST /api/v1/satelite/v2/google-maps/transformador/{id}/imagens-grid/salvar
```

### Descrição
Este endpoint combina três operações:
1. ✅ Gera o grid de coordenadas para cobrir a área poligonal
2. ✅ Baixa cada imagem do Google Maps
3. ✅ Registra cada imagem na tabela `satelite_imagens`

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|-----------|------|-------------|--------|-----------|
| `id` | int (path) | Sim | - | ID do transformador |
| `zoom_grid` | int (query) | Não | 20 | Nível de zoom (10-21) |
| `tamanho` | string (query) | Não | "640x640" | Tamanho em pixels (WxH) |
| `api_key` | string (query) | Não | env | Chave Google Maps API |

### Request

**Exemplo 1: Salvar grid padrão (zoom 20)**
```bash
POST /api/v1/satelite/v2/google-maps/transformador/47/imagens-grid/salvar
```

**Exemplo 2: Salvar com zoom customizado**
```bash
POST /api/v1/satelite/v2/google-maps/transformador/47/imagens-grid/salvar?zoom_grid=18
```

**Exemplo 3: Salvar com tamanho maior**
```bash
POST /api/v1/satelite/v2/google-maps/transformador/47/imagens-grid/salvar?zoom_grid=20&tamanho=1280x1280
```

### Response

```json
{
  "sucesso": true,
  "transformador_id": 47,
  "subestacao_id": 1,
  "total_solicitadas": 4,
  "total_salvas": 4,
  "sensor": "Google_Maps_Grid_Z20",
  "dimensoes_grid": {
    "linhas": 2,
    "colunas": 2
  },
  "imagens": [
    {
      "imagem_id": 123,
      "linha": 0,
      "coluna": 0,
      "url": "https://maps.googleapis.com/maps/api/staticmap?..."
    },
    {
      "imagem_id": 124,
      "linha": 0,
      "coluna": 1,
      "url": "https://maps.googleapis.com/maps/api/staticmap?..."
    },
    {
      "imagem_id": 125,
      "linha": 1,
      "coluna": 0,
      "url": "https://maps.googleapis.com/maps/api/staticmap?..."
    },
    {
      "imagem_id": 126,
      "linha": 1,
      "coluna": 1,
      "url": "https://maps.googleapis.com/maps/api/staticmap?..."
    }
  ]
}
```

---

## 🗄️ Estrutura no Banco de Dados

### Tabela: `satelite_imagens`

Cada célula do grid é salva como um registro independente:

| Campo | Tipo | Exemplo | Descrição |
|-------|------|---------|-----------|
| `id` | SERIAL | 123 | ID único da imagem |
| `subestacao_id` | INTEGER | 1 | ID da subestação |
| `sensor` | VARCHAR(50) | "Google_Maps_Grid_Z20" | Tipo de sensor/fonte |
| `data_aquisicao` | TIMESTAMPTZ | "2026-02-01T10:30:00Z" | Data do registro |
| `resolucao_m` | INTEGER | 1 | Resolução em metros/pixel |
| `url` | TEXT | "https://maps.googleapis..." | URL da imagem |
| `bbox_json` | JSONB | {...} | Bounding box geográfico |
| `propriedades_json` | JSONB | {...} | Metadados customizados |

### Metadados em `propriedades_json`

```json
{
  "transformador_id": 47,
  "tipo_imagem": "grid_cell",
  "grid_posicao": {
    "linha": 0,
    "coluna": 1
  },
  "grid_dimensoes": {
    "linhas": 2,
    "colunas": 2
  },
  "zoom": 20,
  "largura_pixels": 640,
  "altura_pixels": 640,
  "bandas": 3,
  "nome_bandas": ["Red", "Green", "Blue"],
  "resolucao_m_pixel": 1.1924,
  "offset_centro": {
    "lat_km": 0.5,
    "lon_km": -0.3
  },
  "rgb_medio": {
    "r": 142.5,
    "g": 138.2,
    "b": 125.8
  },
  "tamanho_arquivo_bytes": 98432,
  "indice_grid": 1,
  "total_imagens_grid": 4
}
```

---

## 🔍 Consultar Imagens Salvas

### Consulta SQL: Todas as imagens de um transformador

```sql
SELECT 
    id,
    sensor,
    url,
    propriedades_json->>'grid_posicao' as posicao,
    data_aquisicao
FROM satelite_imagens
WHERE sensor LIKE 'Google_Maps_Grid%'
  AND propriedades_json->>'transformador_id' = '47'
ORDER BY 
    (propriedades_json->'grid_posicao'->>'linha')::int,
    (propriedades_json->'grid_posicao'->>'coluna')::int;
```

### Consulta SQL: Grid completo com estatísticas

```sql
SELECT 
    propriedades_json->'grid_posicao'->>'linha' as linha,
    propriedades_json->'grid_posicao'->>'coluna' as coluna,
    url,
    resolucao_m,
    (propriedades_json->>'tamanho_arquivo_bytes')::int / 1024 as tamanho_kb,
    propriedades_json->'rgb_medio' as rgb_medio
FROM satelite_imagens
WHERE sensor = 'Google_Maps_Grid_Z20'
  AND propriedades_json->>'transformador_id' = '47'
ORDER BY linha, coluna;
```

### Consulta SQL: Área total coberta pelo grid

```sql
SELECT 
    COUNT(*) as total_imagens,
    MAX((propriedades_json->'grid_posicao'->>'linha')::int) + 1 as linhas,
    MAX((propriedades_json->'grid_posicao'->>'coluna')::int) + 1 as colunas,
    AVG((propriedades_json->>'resolucao_m_pixel')::float) as resolucao_media_m
FROM satelite_imagens
WHERE sensor LIKE 'Google_Maps_Grid%'
  AND propriedades_json->>'transformador_id' = '47';
```

---

## 🔄 Workflow de Uso

### Cenário 1: Salvar grid para análise de telhados

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"

# 1. Salvar grid de imagens
response = requests.post(
    f"{BASE_URL}/satelite/v2/google-maps/transformador/47/imagens-grid/salvar",
    params={
        "zoom_grid": 20,
        "tamanho": "640x640"
    }
)

resultado = response.json()

print(f"✅ {resultado['total_salvas']} imagens salvas")
print(f"   IDs: {[img['imagem_id'] for img in resultado['imagens']]}")

# 2. Processar cada imagem salva com detecção de telhados
for imagem in resultado['imagens']:
    imagem_id = imagem['imagem_id']
    
    # Detectar telhados usando modelo YOLO
    response = requests.post(
        f"{BASE_URL}/telhados/transformador/detectar-telhados",
        json={
            "transformador_id": 47,
            "subestacao_id": resultado['subestacao_id'],
            "url_imagem": imagem['url'],
            "fonte_imagem": "google_maps",
            "confianca_minima": 0.5
        }
    )
    
    deteccoes = response.json()
    print(f"   Imagem [{imagem['linha']},{imagem['coluna']}]: "
          f"{deteccoes['total_telhados']} telhados detectados")
```

### Cenário 2: Comparar diferentes níveis de zoom

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"
TRANSFORMADOR_ID = 47

# Salvar grids com diferentes zooms
for zoom in [18, 19, 20]:
    response = requests.post(
        f"{BASE_URL}/satelite/v2/google-maps/transformador/{TRANSFORMADOR_ID}/imagens-grid/salvar",
        params={"zoom_grid": zoom}
    )
    
    resultado = response.json()
    
    if resultado['sucesso']:
        print(f"Zoom {zoom}:")
        print(f"  - {resultado['total_salvas']} imagens")
        print(f"  - Sensor: {resultado['sensor']}")
        print(f"  - Grid: {resultado['dimensoes_grid']}")
```

### Cenário 3: Visualizar grid em mapa

```python
import requests
import folium

BASE_URL = "http://localhost:8000/api/v1"

# 1. Salvar grid
response = requests.post(
    f"{BASE_URL}/satelite/v2/google-maps/transformador/47/imagens-grid/salvar"
)
resultado = response.json()

# 2. Criar mapa
mapa = folium.Map(
    location=[resultado['latitude_centro'], resultado['longitude_centro']],
    zoom_start=18
)

# 3. Adicionar células do grid
for imagem in resultado['imagens']:
    folium.Marker(
        location=[imagem['latitude'], imagem['longitude']],
        popup=f"Grid [{imagem['linha']},{imagem['coluna']}]<br>ID: {imagem['imagem_id']}",
        icon=folium.Icon(color='blue', icon='th')
    ).add_to(mapa)

mapa.save('grid_map.html')
print("✅ Mapa salvo em grid_map.html")
```

---

## 🚀 Código Implementado

### 1. Serviço de Salvamento

**Arquivo:** `backend/src/services/image_service.py` (UNIFICADO)

**Método:**
```python
def salvar_imagem_google_maps(self,
                              subestacao_id: int,
                              transformador_id: int,
                              url: str,
                              latitude: float,
                              longitude: float,
                              zoom: int = 19,
                              largura: int = 640,
                              altura: int = 640,
                              vertices_poligono: Optional[list] = None,
                              resolucao_m: float = 1.0) -> Dict[str, Any]:
    """
    Salva todas as imagens de um grid do Google Maps no banco
    """
```

### 2. Endpoint REST

**Arquivo:** `backend/src/api/satelite_v2.py`

**Novo endpoint:**
```python
@router.post("/google-maps/transformador/{id}/imagens-grid/salvar")
def salvar_imagens_grid_transformador(...):
    """
    Busca múltiplas imagens em grade (grid) e salva no banco de dados
    """
```

---

## 📊 Benefícios

### ✅ Reaproveitamento
- Usa tabela `satelite_imagens` existente
- Usa `ImagemSalvamentoService` já implementado
- Compatível com endpoints de detecção de telhados

### ✅ Rastreabilidade
- Cada imagem tem ID único
- Metadados completos (posição, zoom, resolução)
- Timestamps para auditoria

### ✅ Processamento Posterior
- Imagens disponíveis para batch processing
- Pode ser usado com detecção de telhados
- Consultas SQL para análise

### ✅ Organização
- Grid organizado por linha/coluna
- Sensor identifica origem (Google_Maps_Grid_Z20)
- Fácil filtragem por transformador

---

## 🧪 Testes

### Teste 1: Salvar grid básico

```bash
curl -X POST "http://localhost:8000/api/v1/satelite/v2/google-maps/transformador/47/imagens-grid/salvar"
```

**Resultado esperado:**
```json
{
  "sucesso": true,
  "total_salvas": 4,
  "sensor": "Google_Maps_Grid_Z20"
}
```

### Teste 2: Verificar no banco

```sql
SELECT COUNT(*) 
FROM satelite_imagens 
WHERE sensor LIKE 'Google_Maps_Grid%';
```

### Teste 3: Consultar por transformador

```sql
SELECT 
    id,
    sensor,
    propriedades_json->'grid_posicao' as posicao
FROM satelite_imagens
WHERE propriedades_json->>'transformador_id' = '47'
ORDER BY id DESC
LIMIT 10;
```

---

## 🔧 Configuração

### Variáveis de Ambiente

```bash
# .env
GOOGLE_MAPS_API_KEY=sua_chave_api_aqui
```

### Quota do Google Maps

- Cada célula do grid = 1 requisição
- Grid 2x2 = 4 requisições
- Grid 3x3 = 9 requisições
- **Importante:** Monitore sua quota (25k/mês grátis)

---

## 📝 Notas Importantes

1. **Quota Google Maps:** Cada célula do grid consome 1 requisição da API
2. **Tempo de processamento:** Grid 2x2 leva ~5-10 segundos para baixar e salvar
3. **Armazenamento:** Cada imagem 640x640 ≈ 50-150 KB
4. **Zoom recomendado:** Zoom 20 para máxima resolução em áreas urbanas
5. **Tamanho recomendado:** 640x640 para balancear qualidade e performance

---

## 🎯 Casos de Uso

1. **Detecção de Telhados em Alta Resolução**
   - Salvar grid com zoom 20
   - Processar cada célula com modelo YOLO
   - Agregar resultados de todas as células

2. **Monitoramento Temporal**
   - Salvar grids em diferentes datas
   - Comparar mudanças na área
   - Detectar novas construções

3. **Análise de Cobertura**
   - Verificar completude da área
   - Identificar regiões com baixa qualidade
   - Otimizar parâmetros de zoom

4. **Geração de Dataset**
   - Salvar múltiplas áreas
   - Usar para treinar modelos
   - Anotar telhados manualmente

---

## ✅ Checklist de Implementação

- [x] Método `salvar_imagens_grid_google_maps` criado
- [x] Endpoint POST `/imagens-grid/salvar` implementado
- [x] Metadados de grid salvos em `propriedades_json`
- [x] Sensor identificado como `Google_Maps_Grid_Z{zoom}`
- [x] Bbox calculado para cada célula
- [x] Documentação completa
- [x] Exemplos de consultas SQL
- [x] Workflow de uso documentado

---

## 📚 Referências

- [Endpoint GET imagens-grid](../../backend/src/api/satelite_v2.py#L974)
- [Serviço GoogleMapsServiceV2](../../backend/src/services/google_maps_service_v2.py)
- [Tabela satelite_imagens](../../infrastructure/database/001_satelite_tables.sql)
- [ImageService](../../backend/src/services/image_service.py) (UNIFICADO)
  - Aliases: ImagemSalvamentoService, ImagemMultiFonteService, ImagemStrategyService

---

**Pronto para uso! 🎉**

Use o novo endpoint POST para salvar grids de imagens no banco e processá-las posteriormente com detecção de telhados.
