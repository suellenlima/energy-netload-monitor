# ⚡ Quick Start - Sistema de Monitoramento de Carga Líquida

## 🚀 Iniciar Rápido (5 minutos)

### 1. Atualizar banco de dados
```bash
cd infrastructure/database
psql -U postgres < schema.sql
# Ou via docker:
docker-compose exec postgres psql -U postgres < /docker-entrypoint-initdb.d/schema.sql
```

### 2. Instalar dependências
```bash
# Backend (análise, anomalias, clustering)
pip install scikit-learn scipy geopandas shapely requests

# Frontend (dashboard, gráficos)
pip install streamlit pandas geopandas plotly

# Verificar instalação
python -c "import sklearn, geopandas, streamlit, plotly; print('✅ Todas as dependências OK')"
```

### 3. Carregar dados de subestações ONS
```bash
# Opção A: Via Python
python -c "
from pathlib import Path
import sys
sys.path.insert(0, 'etl_pipeline/src')
from extractors.subestacoes_client import run_extraction
from core import create_db_engine, load_settings

settings = load_settings()
engine = create_db_engine(settings.database.url)
rows = run_extraction(engine=engine, settings=settings)
print(f'✅ Carregadas {rows} subestações')
"

# Opção B: Via script
python scripts/demo_subestacoes.py
```

### 4. Iniciar serviços
```bash
# Terminal 1: Backend
cd backend
uvicorn src.main:app --reload --port 8000

# Terminal 2: Frontend
cd frontend
streamlit run src/app.py --server.port 8501

# Terminal 3: Banco de dados (se não em Docker)
docker-compose up -d postgres
```

### 5. Acessar
- **Frontend:** http://localhost:8501
- **API Docs:** http://localhost:8000/docs
- **Subestações Endpoint:** http://localhost:8000/subestacoes/ons

---

## 📊 Testar Funcionalidades Principais

### 1. Testar Análise de Carga e MMGD
```bash
# Obter dados históricos
curl "http://localhost:8000/analise/carga-oculta?subsistema=SUDESTE&limite=100"

# Verificar estado atual (tempo real)
curl "http://localhost:8000/analise/estado-atual?subsistema=SUDESTE"

# Ver perfis de carga por classe
curl "http://localhost:8000/analise/perfil-carga?classe=RESIDENCIAL"
```

### 2. Testar Detecção de Anomalias
```bash
# Executar detecção automática
curl -X POST "http://localhost:8000/analise/detectar-anomalias?distribuidora=CEMIG&limite=10"

# Ver histórico de alertas
curl "http://localhost:8000/analise/alertas-historico?dias=30&limite=50"

# Filtrar por severidade
curl "http://localhost:8000/analise/alertas-historico?dias=7&severidade=alto"
```

### 3. Testar Clustering de Subestações
```bash
# Executar clustering
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?eps_km=5.0"

# Listar resultados
curl "http://localhost:8000/subestacoes/detectadas?limite=20"

# Visualizar em GeoJSON
curl "http://localhost:8000/subestacoes/geo" | jq
```

### Via Frontend (5 Tabs)
1. **Abrir:** http://localhost:8501
2. **Sidebar:** Selecionar subsistema e distribuidora
3. **Atualizar:** Clicar em "Atualizar Dashboard"
4. **Explorar:**
   - ⚡ **KPIs (Topo)**: 4 métricas executivas sempre visíveis
   - 📊 **Visão Geral**: Carga líquida vs real, classes, estabelecimentos
   - ⚡ **Tempo Real**: Dashboard operacional com auto-refresh
   - 🏭 **Subestações**: ONS, Detectadas (clustering), Mapa
   - 📈 **Perfis & Análise**: Curvas típicas por classe
   - 🔍 **Auditoria**: Detecção de fraudes e histórico de alertas

---

## 📝 Exemplos de Uso

### Exemplo 1: Análise de Carga Líquida vs Real
```bash
curl "http://localhost:8000/analise/carga-oculta?subsistema=SUDESTE&limite=100"
```

**Resposta:**
```json
[
  {
    "data_hora": "2026-02-03T14:00:00",
    "carga_ons": 12450.5,
    "estimativa_solar_mw": 3420.8,
    "carga_real_estimada": 15871.3,
    "diferenca_percentual": 27.5
  },
  ...
]
```

### Exemplo 2: Estado Atual (Tempo Real)
```bash
curl "http://localhost:8000/analise/estado-atual?subsistema=SUDESTE"
```

**Resposta:**
```json
{
  "hora_atual": 14,
  "estimativas": {
    "carga_ons_mw": 12450.5,
    "geracao_mmgd_mw": 3420.8,
    "consumo_estimado_mw": 15871.3,
    "irradiancia_atual_wm2": 850.2
  },
  "status": "operacional"
}
```

### Exemplo 3: Detectar Anomalias Automaticamente
```bash
curl -X POST "http://localhost:8000/analise/detectar-anomalias?limite=10"
```

**Resposta:**
```json
{
  "status": "sucesso",
  "quantidade_anomalias": 7,
  "anomalias": [
    {
      "distribuidora": "CEMIG",
      "tipo": "desvio_consumo",
      "severidade": "alto",
      "descricao": "Desvio de consumo de 45.2%",
      "impacto_kw": 1250.5
    },
    ...
  ]
}
```

### Exemplo 4: Histórico de Alertas
```bash
curl "http://localhost:8000/analise/alertas-historico?dias=30&limite=50"
```

**Resposta:**
```json
{
  "total_alertas": 127,
  "alertas": [
    {
      "data_deteccao": "2026-02-01T10:30:00",
      "distribuidora": "CPFL",
      "tipo": "fator_carga_atipico",
      "severidade": "medio",
      "status": "ativo",
      "descricao": "Fator de carga de 0.12 (abaixo de 0.20)"
    },
    ...
  ]
}
```

### Exemplo 5: Listar Subestações ONS
```bash
curl "http://localhost:8000/subestacoes/ons?distribuidora=CEMIG&limite=50"
```

### Exemplo 6: Detectar Subestações via Clustering
```bash
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?distribuidora=CEMIG&eps_km=5.0"
```

### Exemplo 7: Python - Análise Completa
```python
from backend.src.services.load_calc import calculate_mmgd_generation, get_latest_load_data
from backend.src.services.anomaly_detection import detect_anomalies
from backend.src.core.database import get_engine

engine = get_engine()

# 1. Obter última carga
latest = get_latest_load_data(engine, subsistema="SUDESTE")
print(f"Carga ONS: {latest['carga_ons']} MW")

# 2. Estimar geração MMGD
mmgd = calculate_mmgd_generation(
    engine,
    subsistema="SUDESTE",
    irradiancia_wm2=850.0,
    hora=14
)
print(f"Geração MMGD estimada: {mmgd} MW")

# 3. Detectar anomalias
anomalias = detect_anomalies(engine, limite=10)
print(f"Encontradas {len(anomalias)} anomalias")
for a in anomalias:
    print(f"  - {a['distribuidora']}: {a['tipo']} ({a['severidade']})")
```

---

## 🎛️ Ajustar Parâmetros

### Sensibilidade do Clustering

**Rede com muita GD espalhada:**
```bash
# Aumentar raio e exigir mais pontos
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?eps_km=10.0&min_samples=5"
```

**Rede com GD concentrada:**
```bash
# Reduzir raio e aceitar menos pontos
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?eps_km=3.0&min_samples=2"
```

---

## 🐛 Diagnosticar Problemas

### Verificar dados carregados
```bash
# Conectar ao banco
docker-compose exec postgres psql -U postgres energy_monitor

# Contar registros
SELECT COUNT(*) FROM subestacoes_ons;
SELECT COUNT(*) FROM subestacoes_detectadas;
SELECT COUNT(*) FROM usinas_siga WHERE potencia_kw > 0;

# Ver amostra
SELECT nome, sigla_se, tensao_kv FROM subestacoes_ons LIMIT 5;
```

### Verificar API
```bash
# Health check
curl http://localhost:8000/health

# Documentação interativa
# Abrir: http://localhost:8000/docs
```

### Verificar Logs
```bash
# Backend
# Checar output do terminal onde uvicorn está rodando

# Frontend
# Checar output do terminal onde streamlit está rodando

# ETL
tail -f etl_pipeline/logs/etl.log
```

---

## 📚 Documentação Completa

- **[SUBESTACOES_README.md](SUBESTACOES_README.md)** - Guia detalhado
- **[TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md)** - Referência técnica
- **API Docs** - http://localhost:8000/docs (Swagger)

---

## ✅ Checklist de Validação

### Infraestrutura
- [ ] Banco de dados TimescaleDB + PostGIS está rodando
- [ ] Schema atualizado com 9 tabelas
- [ ] Dependências Python instaladas (sklearn, geopandas, streamlit, plotly)
- [ ] Backend FastAPI inicia sem erros (porta 8000)
- [ ] Frontend Streamlit carrega sem erros (porta 8501)

### Dados
- [ ] Dados de carga líquida ONS carregados
- [ ] Dados de GD detalhada disponíveis
- [ ] Subestações ONS carregadas (mock ou real)
- [ ] Perfis de carga configurados

### API (21+ Endpoints)
- [ ] `/analise/carga-oculta` retorna dados
- [ ] `/analise/estado-atual` funciona (tempo real)
- [ ] `/analise/detectar-anomalias` executa detecção
- [ ] `/analise/alertas-historico` retorna histórico
- [ ] `/subestacoes/ons` lista subestações
- [ ] `/subestacoes/detectadas/atualizar` executa clustering

### Frontend (5 Tabs)
- [ ] KPIs executivos aparecem no topo
- [ ] Tab "Visão Geral" exibe gráfico de carga
- [ ] Tab "Tempo Real" mostra estimativas atuais
- [ ] Tab "Subestações" tem 3 sub-tabs (ONS, Detectadas, Mapa)
- [ ] Tab "Perfis & Análise" exibe curvas típicas
- [ ] Tab "Auditoria" mostra histórico de alertas
- [ ] Auto-refresh funciona (opcional no tempo real)

---

## 🆘 Precisa de Ajuda?

| Problema | Solução |
|----------|---------|
| `ModuleNotFoundError: sklearn` | `pip install scikit-learn` |
| `ModuleNotFoundError: geopandas` | `pip install geopandas shapely` |
| `ModuleNotFoundError: plotly` | `pip install plotly` |
| Conexão com DB recusada | `docker-compose up -d postgres` |
| Porta 8000 em uso | `lsof -i :8000` (Linux/Mac) ou `netstat -ano \| findstr :8000` (Windows) |
| Porta 8501 em uso | `lsof -i :8501` (Linux/Mac) ou `netstat -ano \| findstr :8501` (Windows) |
| Dados vazios na API | Verificar se dados foram carregados no banco |
| KPIs não aparecem | Verificar endpoint `/analise/estado-atual` |
| Anomalias não detectadas | Executar `/analise/detectar-anomalias` (POST) |
| Gráficos não renderizam | Verificar console do navegador (F12) |
| ApiResult error | Verificar se endpoint retorna `{data, error, status_code}` |

---

## 🔍 Verificar Instalação

```bash
# Testar importações
python -c "
import sklearn
import geopandas
import streamlit
import plotly
import requests
print('✅ Todas as dependências OK')
"

# Testar conexão com DB
psql -U postgres -d energy_monitor -c "SELECT COUNT(*) FROM carga_liquida_ons;"

# Testar API
curl http://localhost:8000/health
curl http://localhost:8000/analise/carga-oculta?subsistema=SUDESTE&limite=1

# Testar Frontend
# Abrir http://localhost:8501 e verificar se KPIs aparecem
```

---

**Pronto para começar? 🚀**

```bash
# Tudo em um comando (Linux/Mac)
docker-compose up -d postgres && \
cd backend && uvicorn src.main:app --reload --port 8000 & \
cd ../frontend && streamlit run src/app.py --server.port 8501 &
echo "✅ Sistema iniciado!"
echo "📊 Frontend: http://localhost:8501"
echo "📖 API Docs: http://localhost:8000/docs"
```

---

**Última atualização:** 2026-02-03
**Versão:** 2.0
**Status:** ✅ Sistema Completo e Testado
