# ⚡ Energy Netload Monitor

Sistema completo de **monitoramento de carga líquida**, **estimativa de geração distribuída (MMGD)**, **detecção de anomalias** e **análise de subestações** para sistemas elétricos de distribuição.

## 🎯 Funcionalidades Principais

1. **Análise de Carga e MMGD** - Estimativa de geração solar distribuída em tempo real
2. **Detecção Automática de Anomalias** - Identificação de padrões anormais (desvio >30%, fator de carga atípico)
3. **Dashboard Multi-Tab** - 5 tabs principais + KPIs executivos sempre visíveis
4. **Monitoramento Tempo Real** - Auto-refresh com integração Open-Meteo (irradiância)
5. **Análise de Subestações** - Dados ONS + clustering geoespacial (DBSCAN)

## 📊 Tecnologias

- **Backend:** FastAPI + SQLAlchemy + scikit-learn (21+ endpoints)
- **Frontend:** Streamlit + Plotly (5 tabs interativas)
- **Database:** TimescaleDB + PostGIS (9 tabelas)
- **ETL:** 4 fontes de dados (ONS, ANEEL, BDGD, Open-Meteo)
- **ML:** DBSCAN clustering, detecção multi-critério de anomalias

## 🚀 Início Rápido

Para guia completo de instalação e uso, consulte **[docs/QUICKSTART.md](docs/QUICKSTART.md)** (5 minutos).

### Requisitos
- Docker Desktop com Docker Compose v2
- Python 3.8+ (para desenvolvimento local)

## Subindo o ambiente
```powershell
docker-compose up --build
```
Alternativa (Compose v2):
```powershell
docker compose up --build
```

## Servicos e portas
- Banco (Postgres + Timescale + PostGIS): 5432
- API (FastAPI): http://localhost:8000
- Dashboard (Streamlit): http://localhost:8501
- PgAdmin: http://localhost:5050
- Jupyter: http://localhost:8888

## PgAdmin
Login: admin@energy.com / admin

Adicionar servidor:
- Name: Energy Monitor
- Host: db
- Database: energy_monitor
- User: admin
- Password: admin123

## ETL (carregar dados)
Garanta que o servico `etl` esta rodando:
```powershell
docker-compose up -d etl
```

Criar schema
```powershell
Get-Content infrastructure/database/schema.sql | docker compose exec -T db psql -U admin -d energy_monitor
```

Habilitar PostGIS (se necessario):
```powershell
docker-compose exec db psql -U admin -d energy_monitor -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```

Executar extracoes:
```powershell
docker-compose exec etl python src/extractors/aneel_client.py
docker-compose exec etl python src/extractors/ons_client.py
docker-compose exec etl python src/extractors/subestacoes_client.py
docker-compose exec etl python src/extractors/gd_client.py
docker-compose exec etl python src/extractors/inpe_weather_client.py
docker-compose exec etl python src/fix_data.py
```

## Notebooks
Acesse http://localhost:8888 com token `admin`.

Notebook sugerido:
- `notebooks/03_treino_modelo_telhados.ipynb`

## Variaveis de ambiente
Valores padrao usados pelo compose (podem ser sobrescritos via `.env`):
- `DB_USER` (default: `admin`)
- `DB_PASS` (default: `admin123`)
- `DB_NAME` (default: `energy_monitor`)
- `PGADMIN_MAIL` (default: `admin@energy.com`)
- `PGADMIN_PASS` (default: `admin`)

## 📁 Estrutura do Repositório

```
energy-netload-monitor/
├── backend/
│   ├── src/api/              # 2 routers (analise, subestacoes)
│   │   ├── analise.py        # 8 endpoints de análise
│   │   └── subestacoes.py    # 5 endpoints de subestações
│   ├── src/services/         # Lógica de negócio
│   │   ├── load_calc.py      # Cálculo de carga e MMGD
│   │   ├── anomaly_detection.py  # Detecção automática
│   │   ├── subestacoes_clustering.py  # DBSCAN
│   │   ├── synthetic_load.py # Perfis sintéticos
│   │   └── realtime_estimation.py  # Tempo real
│   └── tests/                # Testes unitários
│
├── frontend/
│   └── src/
│       ├── app.py            # App principal (5 tabs)
│       └── components/       # 7 componentes UI
│           ├── kpis.py       # KPIs executivos
│           ├── charts.py     # Gráficos principais
│           ├── realtime.py   # Dashboard tempo real
│           ├── subestacoes.py # Análise de SEs
│           ├── audit.py      # Auditoria e alertas
│           ├── sidebar.py    # Controles
│           └── alerts.py     # Alertas
│
├── etl_pipeline/
│   └── src/
│       ├── extractors/       # 4 fontes de dados
│       │   ├── ons_client.py
│       │   ├── aneel_client.py
│       │   ├── bdgd_client.py
│       │   └── subestacoes_client.py
│       └── schedulers/       # Agendamento de jobs
│
├── infrastructure/
│   └── database/
│       ├── schema.sql        # 9 tabelas + índices
│       └── migrations/       # Histórico de alterações
│
├── docs/                     # 📚 Documentação completa
│   ├── INDEX.md              # Índice navegável
│   ├── QUICKSTART.md         # Início rápido (5 min)
│   ├── TECHNICAL_SUMMARY.md  # Arquitetura técnica
│   ├── IMPLEMENTACAO_COMPLETA.md  # Resumo executivo
│   ├── SUBESTACOES_README.md # Detalhes subestações
│   └── ETL_DIAGNOSTICO.md    # Pipeline ETL
│
├── notebooks/                # Jupyter notebooks
└── data/                     # Dados locais (ignorado)
```

## 🌐 APIs Disponíveis (21+ Endpoints)

### Análise de Carga (/analise/*)
- `GET /analise/carga-oculta` - Histórico carga líquida vs real
- `GET /analise/estado-atual` - Estimativas tempo real
- `GET /analise/classes-consumo` - Consumo por classe
- `GET /analise/estabelecimentos` - Distribuição estabelecimentos
- `GET /analise/perfil-carga` - Perfis típicos por classe
- `GET /analise/alertas-historico` - Histórico de anomalias
- `POST /analise/detectar-anomalias` - Executa detecção automática
- `GET /analise/simulacao-fraude` - Simulação de impacto

### Subestações (/subestacoes/*)
- `GET /subestacoes/ons` - Lista subestações oficiais ONS
- `GET /subestacoes/detectadas` - Lista subestações detectadas (clustering)
- `POST /subestacoes/detectadas/atualizar` - Executa DBSCAN
- `GET /subestacoes/geo` - GeoJSON para mapas
- `GET /subestacoes/resumo` - Estatísticas agregadas

**Documentação Interativa:** http://localhost:8000/docs (Swagger)

## 📊 Dashboard Frontend (5 Tabs)

1. **📊 Visão Geral** - Carga líquida vs real, classes de consumo, estabelecimentos
2. **⚡ Tempo Real** - Dashboard operacional com auto-refresh e irradiância solar
3. **🏭 Subestações** - ONS oficiais, detectadas (clustering), mapa GeoJSON
4. **📈 Perfis & Análise** - Curvas típicas por classe de consumo
5. **🔍 Auditoria** - Detecção de fraudes e histórico de alertas

**KPIs Executivos:** 4 cards sempre visíveis no topo (Carga ONS, Consumo Real, Geração MMGD, Status)

## 📚 Documentação

Para documentação completa, consulte a pasta **[docs/](docs/)**:

- **[docs/INDEX.md](docs/INDEX.md)** - Índice completo e navegável
- **[docs/QUICKSTART.md](docs/QUICKSTART.md)** - Guia de início rápido (5 minutos)
- **[docs/TECHNICAL_SUMMARY.md](docs/TECHNICAL_SUMMARY.md)** - Arquitetura técnica detalhada
- **[docs/IMPLEMENTACAO_COMPLETA.md](docs/IMPLEMENTACAO_COMPLETA.md)** - Resumo executivo do projeto
- **[docs/SUBESTACOES_README.md](docs/SUBESTACOES_README.md)** - Detalhes sobre clustering de subestações
- **[docs/ETL_DIAGNOSTICO.md](docs/ETL_DIAGNOSTICO.md)** - Pipeline ETL e fontes de dados

## 🔍 Recursos Avançados

### Detecção Automática de Anomalias
- **Desvio de Consumo:** Identifica variações >30% do esperado
- **Fator de Carga Atípico:** Detecta padrões <0.20 ou >0.95
- **Picos Anormais:** Identifica picos >2.5x a média
- **Histórico Completo:** Filtros por tipo, severidade e status

### Clustering de Subestações (DBSCAN)
- **Algoritmo:** DBSCAN geoespacial com haversine distance
- **Parâmetros Ajustáveis:** eps_km (raio), min_samples (mínimo de pontos)
- **Visualização:** GeoJSON para integração com mapas

### Estimativa MMGD em Tempo Real
- **Fonte:** Open-Meteo API (irradiância solar)
- **Cálculo:** Potência instalada × (irradiância/1000) × eficiência
- **Atualização:** Auto-refresh configurável

## 🧪 Testes

```bash
# Backend
cd backend
pytest

# Testes específicos
pytest tests/test_load_profiles.py
pytest tests/test_synthetic_load.py
pytest tests/test_subestacao_mix.py
```