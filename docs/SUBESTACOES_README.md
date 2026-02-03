# 🏢 Detecção de Subestações - Guia de Implementação

## Visão Geral

Implementação completa de **detecção de subestações de distribuição** usando duas abordagens complementares:

1. **Dados Oficiais ONS** - Subestações registradas publicadas pelo Operador Nacional do Sistema
2. **Clustering Geoespacial** - Detecção automática de subestações implícitas analisando agrupamentos de geração distribuída

---

## 📊 Arquitetura

### Banco de Dados

**Novas Tabelas:**
- `subestacoes_ons` - Subestações oficiais do ONS com dados geoespaciais
- `subestacoes_detectadas` - Subestações inferidas via análise de clustering

```sql
-- Subestações ONS (dados públicos)
CREATE TABLE subestacoes_ons (
    id SERIAL PRIMARY KEY,
    nome TEXT UNIQUE,
    sigla_se TEXT,
    tensao_kv DOUBLE PRECISION,
    subsistema TEXT,
    distribuidora TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    fonte_dados TEXT,
    geom geometry(Point, 4326)
);

-- Subestações detectadas por clustering
CREATE TABLE subestacoes_detectadas (
    id SERIAL PRIMARY KEY,
    cluster_id INTEGER,
    nome TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distribuidora TEXT,
    subsistema TEXT,
    quantidade_gd INTEGER,          -- Pontos de GD no cluster
    potencia_total_mw DOUBLE PRECISION,
    raio_deteccao_km DOUBLE PRECISION,  -- Raio máximo do cluster
    data_deteccao TIMESTAMPTZ,
    geom geometry(Point, 4326)
);
```

### Backend

**Novo Módulo ETL:**
- `etl_pipeline/src/extractors/subestacoes_client.py` - Extração de dados oficiais

**Novo Serviço:**
- `backend/src/services/subestacoes_clustering.py` - Clustering e detecção

**Novo Router API:**
- `backend/src/api/subestacoes.py` - Endpoints REST

### Frontend

**Novo Componente:**
- `frontend/src/components/subestacoes.py` - UI com abas e mapas

---

## 🔌 API Endpoints

### GET `/subestacoes/ons`
Lista subestações do ONS (dados oficiais).

```bash
curl "http://localhost:8000/subestacoes/ons?distribuidora=CEMIG&limite=100"
```

**Resposta:**
```json
[
  {
    "id": 1,
    "nome": "SE Belo Horizonte 230 kV",
    "sigla_se": "BHO",
    "tensao_kv": 230,
    "subsistema": "SUDESTE",
    "distribuidora": "CEMIG DISTRIBUICAO S.A",
    "latitude": -19.9228,
    "longitude": -43.9387,
    "fonte_dados": "ONS_MOCK"
  }
]
```

### GET `/subestacoes/detectadas`
Lista subestações detectadas via clustering.

```bash
curl "http://localhost:8000/subestacoes/detectadas?distribuidora=CEMIG&limite=50"
```

**Resposta:**
```json
[
  {
    "id": 1,
    "cluster_id": 0,
    "nome": "SE_DETECTADA_0",
    "latitude": -19.925,
    "longitude": -43.938,
    "distribuidora": "CEMIG DISTRIBUICAO S.A",
    "subsistema": "SUDESTE",
    "quantidade_gd": 5,
    "potencia_total_mw": 2.5,
    "raio_deteccao_km": 3.2,
    "data_deteccao": "2026-01-21T10:30:00+00:00"
  }
]
```

### POST `/subestacoes/detectadas/atualizar`
Executa clustering para detectar novas subestações.

```bash
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?distribuidora=CEMIG&eps_km=5.0"
```

**Resposta:**
```json
{
  "status": "sucesso",
  "mensagem": "Detectadas e armazenadas 12 subestações",
  "quantidade": 12,
  "raio_km": 5.0
}
```

### GET `/subestacoes/geo`
Retorna dados em formato GeoJSON para visualização em mapas.

```bash
curl "http://localhost:8000/subestacoes/geo?origem=ambas&limite=100"
```

**Resposta:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [-43.9387, -19.9228]
      },
      "properties": {
        "nome": "SE Belo Horizonte 230 kV",
        "distribuidora": "CEMIG DISTRIBUICAO S.A",
        "tipo": "subestacao_ons",
        "origem": "ONS"
      }
    }
  ]
}
```

### GET `/subestacoes/resumo`
Resumo agregado por distribuidora.

```bash
curl "http://localhost:8000/subestacoes/resumo"
```

---

## 🧠 Algoritmo de Clustering

### DBSCAN (Density-Based Spatial Clustering)

**Parâmetros:**
- `eps_km` - Raio de busca (padrão: 5 km)
- `min_samples` - Mínimo de pontos por cluster (padrão: 3)

**Fluxo:**
1. Buscar todos os pontos de GD da distribuidora
2. Converter coordenadas para projeção Web Mercator (distâncias em metros)
3. Executar DBSCAN com parâmetros
4. Filtrar ruído (clusters com label -1)
5. Calcular centróide e raio para cada cluster válido
6. Armazenar subestações detectadas

**Exemplo de Clustering:**
```python
from backend.src.services.subestacoes_clustering import detect_subestacoes_by_clustering

# Detectar subestações
df = detect_subestacoes_by_clustering(
    engine,
    distribuidora="CEMIG DISTRIBUICAO S.A",
    eps_km=5.0,
    min_samples=3
)

print(f"Detectadas {len(df)} subestações")
# Output: Detectadas 12 subestações
```

---

## 📥 Carregamento de Dados

### Integrar ao ETL Pipeline

Adicionar ao arquivo de orquestração (ex: `etl_pipeline/src/main.py` ou DAG do Airflow):

```python
from etl_pipeline.src.extractors.subestacoes_client import run_extraction

# Executar extração de subestações
resultado = run_extraction()
print(f"Carregadas {resultado} subestações do ONS")
```

### Uso Manual

```python
from pathlib import Path
import logging
from sqlalchemy import create_engine
from etl_pipeline.src.extractors.subestacoes_client import run_extraction
from etl_pipeline.src.core import load_settings

# Configurar
settings = load_settings()
engine = create_engine(settings.database.url)
logger = logging.getLogger("etl")

# Executar
rows_loaded = run_extraction(engine=engine, settings=settings, logger=logger)
print(f"✅ Carregadas {rows_loaded} subestações")
```

---

## 🖥️ Frontend - Interface Streamlit

### Componentes

**Seção Principal:**
```python
from components.subestacoes import render_subestacoes_section

# Renderizar no app
render_subestacoes_section(client, distribuidora="CEMIG DISTRIBUICAO S.A")
```

**Abas Disponíveis:**

1. **🏢 ONS (Oficial)** - Tabela de subestações registradas
2. **🔍 Detectadas (Clustering)** - Subestações inferidas com análise de potência
3. **🗺️ Mapa** - Visualização geoespacial

### Controles Interativos

- **Atualizar Detecção** - Executa clustering em tempo real
- **Raio de Detecção (km)** - Ajusta sensibilidade do clustering
- **Limite de Registros** - Controla quantidade exibida

### Gráficos

- Potência por subsistema
- Distribuição de subestações
- Dados geoespaciais em mapa

---

## ⚙️ Instalação de Dependências

**Backend - Adicionar ao `requirements.txt`:**
```
scikit-learn>=1.0.0
scipy>=1.7.0
geopandas>=0.10.0
shapely>=1.8.0
```

**Frontend - Adicionar ao `requirements.txt`:**
```
streamlit>=1.20.0
pandas>=1.5.0
geopandas>=0.10.0
```

---

## 🔄 Fluxo de Dados Completo

```
┌─────────────────────────────────────────────────────────┐
│              ETL Pipeline                               │
├─────────────────────────────────────────────────────────┤
│ 1. subestacoes_client.py                                │
│    ├─ Busca dados ONS (ou mock)                        │
│    ├─ Transforma em GeoDataFrame                       │
│    └─ Carrega em subestacoes_ons                       │
└────────────────┬────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────┐
│              Banco de Dados (PostgreSQL+PostGIS)        │
├─────────────────────────────────────────────────────────┤
│ subestacoes_ons │ subestacoes_detectadas               │
│ ├─ nome         │ ├─ cluster_id                        │
│ ├─ sigla_se     │ ├─ potencia_total_mw                │
│ ├─ tensao_kv    │ ├─ quantidade_gd                     │
│ └─ geom         │ └─ raio_deteccao_km                 │
└────────────────┬────────────────────────────────────────┘
                 │
      ┌──────────┴──────────┐
      │                     │
┌─────▼──────┐      ┌──────▼────────┐
│  Backend   │      │    Frontend    │
│  API       │      │   Streamlit    │
├────────────┤      ├────────────────┤
│/subestacoes│      │render_         │
│  /ons      │      │subestacoes_    │
│  /detectadas      │section()       │
│  /geo      │      │                │
│  /resumo   │      │Abas:           │
│            │      │- ONS           │
│            │      │- Detectadas    │
│            │      │- Mapa          │
└────────────┘      └────────────────┘
      ▲
      │
  Clustering via
  subestacoes_clustering.py
  (DBSCAN)
```

---

## 🎯 Casos de Uso

### 1. Análise de Cobertura de Rede
"Quais são as subestações de uma distribuidora?"
```python
# Via API
GET /subestacoes/ons?distribuidora=CEMIG&limite=100
```

### 2. Detecção de Concentrações de GD
"Onde há agrupamentos de geração solar que podem conectar à mesma SE?"
```python
# Executar clustering
POST /subestacoes/detectadas/atualizar?eps_km=5.0
```

### 3. Análise de Potência por Nó
"Qual a potência total de GD que conecta a cada subestação detectada?"
```python
# Visualizar no frontend - Aba "Detectadas"
# Coluna "Potência (MW)"
```

### 4. Visualização Geoespacial
"Mapear SEs oficiais vs detectadas para validação?"
```python
# Via API ou Frontend
GET /subestacoes/geo?origem=ambas
```

---

## 📝 Notas de Implementação

### Dados Mock
Atualmente o `subestacoes_client.py` retorna dados mock para demonstração. Para usar dados reais:

1. **Obter dataset oficial do ONS**
   - Acessar: https://dados.ons.org.br
   - Formato esperado: CSV com colunas nome, sigla_se, tensao_kv, etc.

2. **Atualizar URL**
   ```python
   SUBESTACOES_ONS_URL = "https://dados.ons.org.br/dataset/subestacoes-ons/resource/..."
   ```

3. **Testar parsing**
   ```python
   df = extract_subestacoes_data(session, settings, logger)
   print(df.head())
   ```

### Ajuste de Parâmetros DBSCAN

**Para redes com muita GD espalhada:**
```python
eps_km = 10.0  # Aumentar raio
min_samples = 5  # Exigir mais pontos
```

**Para redes com GD concentrada:**
```python
eps_km = 3.0  # Reduzir raio
min_samples = 2  # Menos pontos necessários
```

---

## 🐛 Troubleshooting

### Nenhuma subestação detectada
- Verificar se há dados de GD na tabela `usinas_siga`
- Aumentar `eps_km` (raio de busca)
- Reduzir `min_samples` (mínimo de pontos)

### Clustering muito lento
- Limitar distribuidora específica
- Aumentar `eps_km` para menos clusters
- Considerar índices espaciais no PostGIS

### Dados ONS não carregam
- Verificar URL do dataset
- Validar formato CSV esperado
- Consultar logs de erro em `etl_pipeline/logs/`

---

## 📞 Suporte

Para questões sobre:
- **API**: Ver documentação em `/docs` (Swagger)
- **Frontend**: Checks logs do Streamlit
- **ETL**: Ver logs em `etl_pipeline/logs/`
- **DB**: Conectar via `psql` e verificar tabelas

---

**Versão:** 1.0  
**Último Update:** 2026-01-21  
**Autor:** Energy Netload Monitor Team
