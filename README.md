# Energy Netload Monitor

Projeto de monitoramento de carga liquida combinando dados do ONS, ANEEL e clima.

## Requisitos
- Docker Desktop com Docker Compose v2

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

## Estrutura do repositorio
- `backend/`: API FastAPI
- `frontend/`: Dashboard Streamlit
- `etl_pipeline/`: scripts de extracao e carga
- `infrastructure/`: scripts de banco
- `notebooks/`: exploracao e modelos
- `data/`: dados locais (ignorado no Git)

## Troubleshooting
- Warning "attribute `version` is obsolete": pode remover a linha `version` do `docker-compose.yml`.
- `service "etl" is not running`: execute `docker-compose up -d etl` antes do `exec`.
