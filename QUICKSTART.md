# ⚡ Quick Start - Detecção de Subestações

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
# Backend
pip install scikit-learn scipy geopandas shapely

# Frontend
pip install streamlit pandas geopandas
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

## 📊 Testar Detecção Automática

### Via API
```bash
# 1. Executar clustering
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?eps_km=5.0"

# 2. Listar resultados
curl "http://localhost:8000/subestacoes/detectadas?limite=20"

# 3. Visualizar em GeoJSON
curl "http://localhost:8000/subestacoes/geo" | jq
```

### Via Frontend
1. Abrir http://localhost:8501
2. Selecionar distribuidora no sidebar
3. Clicar "Atualizar Dashboard"
4. Ir para seção "⚡ Análise de Subestações"
5. Aba "🔍 Detectadas (Clustering)"
6. Clicar "🔄 Atualizar Detecção"

---

## 📝 Exemplos de Uso

### Exemplo 1: Listar subestações da CEMIG
```bash
curl "http://localhost:8000/subestacoes/ons?distribuidora=CEMIG&limite=50"
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
  },
  ...
]
```

### Exemplo 2: Detectar subestações via clustering
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

### Exemplo 3: Obter resumo por distribuidora
```bash
curl "http://localhost:8000/subestacoes/resumo"
```

**Resposta:**
```json
[
  {
    "distribuidora": "CEMIG DISTRIBUICAO S.A",
    "total_ons": 5,
    "total_detectadas": 12,
    "total": 17
  },
  {
    "distribuidora": "COMPANHIA PAULISTA DE FORCA E LUZ",
    "total_ons": 8,
    "total_detectadas": 18,
    "total": 26
  }
]
```

### Exemplo 4: Python - Detectar subestações programaticamente
```python
from backend.src.services.subestacoes_clustering import (
    detect_subestacoes_by_clustering,
    load_detected_subestacoes
)
from backend.src.core.database import get_engine
import logging

engine = get_engine()
logger = logging.getLogger("demo")

# Executar detecção
df = detect_subestacoes_by_clustering(
    engine,
    distribuidora="CEMIG DISTRIBUICAO S.A",
    eps_km=5.0,
    min_samples=3,
    logger=logger
)

print(f"Detectadas {len(df)} subestações")
print(df[["nome", "quantidade_gd", "potencia_total_mw"]].head())

# Carregar no BD
rows = load_detected_subestacoes(df, engine, logger)
print(f"Carregadas {rows} linhas")
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

- [ ] Banco de dados está rodando
- [ ] Schema foi atualizado (novas tabelas)
- [ ] Dependências foram instaladas
- [ ] Backend inicia sem erros
- [ ] Frontend carrega sem erros
- [ ] Dados ONS foram carregados
- [ ] API /subestacoes/ons retorna dados
- [ ] Clustering pode ser executado
- [ ] Frontend exibe aba de subestações

---

## 🆘 Precisa de Ajuda?

| Problema | Solução |
|----------|---------|
| `ModuleNotFoundError: sklearn` | `pip install scikit-learn` |
| `ModuleNotFoundError: geopandas` | `pip install geopandas shapely` |
| Conexão com DB recusada | `docker-compose up -d postgres` |
| Porta 8000 em uso | `lsof -i :8000` e matar processo |
| Porta 8501 em uso | `lsof -i :8501` e matar processo |
| Dados vazios na API | Executar `python scripts/demo_subestacoes.py` |

---

**Pronto para começar? 🚀**

```bash
# Tudo em um comando (Linux/Mac)
docker-compose up -d postgres && \
python scripts/demo_subestacoes.py && \
echo "✅ Sistema pronto!"
```

---

**Última atualização:** 2026-01-21  
**Status:** ✅ Testado e pronto para produção
