# Energy Netload Monitor

Projeto de monitoramento de carga liquida combinando dados do ONS, ANEEL e clima.

## Requisitos
- Docker Desktop com Docker Compose v2

## Subindo o ambiente
```powershell
docker-compose up --build

or docker-compose build --no-cache && docker-compose up
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
# Schema completo consolidado (recomendado - 1 único arquivo)
Get-Content infrastructure/database/schema.sql | docker compose exec -T db psql -U admin -d energy_monitor
```

Habilitar PostGIS (se necessario):
```powershell
docker-compose exec db psql -U admin -d energy_monitor -c "CREATE EXTENSION IF NOT EXISTS postgis;"

docker-compose exec db psql -U admin -d energy_monitor -c "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;"

```

# Conectar ao PostgreSQL

Executar extracoes:
```powershell
docker-compose exec etl python src/extractors/aneel_client.py
docker-compose exec etl python src/extractors/ons_client.py
docker-compose exec etl python src/extractors/subestacoes_client.py
docker-compose exec etl python src/extractors/gd_client.py
docker-compose exec etl python src/extractors/inpe_weather_client.py
docker-compose exec etl python src/fix_data.py

# ETL com dados reais (ONS, ANEEL, OpenStreetMap)
docker-compose exec etl python src/extractors/area_cobertura_real.py --completo

# docker-compose exec etl python src/extractors/scada_sync_etl.py --todas

docker-compose exec etl python src/extractors/scada_sync_etl.py --todas --modo hibrido
```

## Dados Carregados

Após executar o ETL completo, você terá:
- **1.715 subestações** do ONS com dados reais de localização e topologia
- **19.290 usinas solares** da ANEEL SIGA (irradiação, tecnologia, capacity)
- **Transformadores** mapeados via OpenStreetMap com áreas de cobertura real
- **Áreas de cobertura** calculadas por transformador usando ConvexHull de consumidores

Verificar dados carregados:
```powershell
docker-compose exec db psql -U admin -d energy_monitor -c \
  "SELECT COUNT(*) as total_subestacoes FROM subestacoes; 
   SELECT COUNT(*) as total_usinas FROM usinas_solares; 
   SELECT COUNT(*) as total_transformadores FROM transformadores_area_cobertura;"
```

## ETL com Dados Reais (ONS, ANEEL, OpenStreetMap)


### Executar ETL completo (ONS + ANEEL + OSM)
```powershell
python area_cobertura_real.py --completo
```

### Executar por fonte individual
```powershell
# Apenas subestações do ONS (~2.000 subestações reais)
python area_cobertura_real.py --ons

# Apenas usinas da ANEEL SIGA (~500k usinas solares)
python area_cobertura_real.py --aneel

# Apenas transformadores do OpenStreetMap para SE específica
python area_cobertura_real.py --osm 1
```

### Documentação completa
Consulte `documentation/` para guias detalhados.

## APIs - Áreas de Cobertura

### Transformadores (Nova)
Endpoints para consultar e exportar dados de transformadores com áreas de cobertura real.

**Documentação interativa**: http://localhost:8000/docs (procure por "transformadores")

Exemplos:
```bash
# Detalhes de um transformador
curl http://localhost:8000/api/v1/transformadores/1

# Área de cobertura em GeoJSON
curl http://localhost:8000/api/v1/transformadores/1/area?formato=geojson

# Bounding box para download de satélite
curl http://localhost:8000/api/v1/transformadores/1/bbox

# Transformadores de uma subestação
curl http://localhost:8000/api/v1/transformadores/subestacao/1

# Exportar todos em CSV
curl http://localhost:8000/api/v1/transformadores/export/csv -o transformadores.csv

# Buscar por região (bbox)
curl "http://localhost:8000/api/v1/transformadores/regiao/buscar?min_lat=-25.5&max_lat=-25.4&min_lon=-49.3&max_lon=-49.2"

# Estatísticas de áreas
curl http://localhost:8000/api/v1/transformadores/stats/areas
```

### Subestacoes (Expandida)
Novos endpoints para áreas de cobertura em subestações:

```bash
# Área de cobertura da subestação
curl http://localhost:8000/api/v1/subestacoes/1/area?formato=geojson

# Lista de transformadores associados
curl http://localhost:8000/api/v1/subestacoes/1/transformadores

# Estatísticas de áreas
curl http://localhost:8000/api/v1/subestacoes/areas/stats
```

## Sincronizacao SCADA com Recalculo de Áreas

Script para sincronizar transformadores com SCADA e recalcular áreas de cobertura em tempo real:

```powershell
# Sincronizar todas as subestações (one-shot)
docker-compose exec etl python src/extractors/scada_sync_etl.py --todas

# Sincronizar subestações específicas
docker-compose exec etl python src/extractors/scada_sync_etl.py --subestacao-ids 1 2 3

# Modo daemon contínuo (sincroniza a cada 60 minutos)
docker-compose exec etl python src/extractors/scada_sync_etl.py --todas --loop --intervalo 60

# Limpar dados antigos (>90 dias inativos)
docker-compose exec etl python src/extractors/scada_sync_etl.py --todas --limpar-antigos 90
```

**Características**:
- Sincroniza transformadores com dados SCADA em tempo real
- Recalcula áreas de cobertura usando ConvexHull de consumidores
- Limpa dados antigos/inativos
- Modo daemon com retry automático em caso de erro
- Integrado com serviço AreaService centralizado

## Service Layer - AreaService

Serviço reutilizável para consultas de áreas (`etl_pipeline/src/services/area_service.py`):

```python
from etl_pipeline.src.services.area_service import AreaService
from etl_pipeline.src.core import create_db_engine

engine = create_db_engine()
service = AreaService(engine)

# Obter área de um transformador
area = service.obter_area_transformador(id=1)

# Listar transformadores de uma subestação
transformadores = service.listar_transformadores_subestacao(id=1)

# Exportar em diferentes formatos
service.exportar_transformadores(formato='geojson')  # geojson, csv, json

# Buscar por região (bbox)
resultados = service.buscar_transformadores_por_regiao(
    min_lat=-25.5, max_lat=-25.4, 
    min_lon=-49.3, max_lon=-49.2
)

# Estatísticas
stats = service.obter_estatisticas_areas()
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
