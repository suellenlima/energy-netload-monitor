# 🔧 Guia de Diagnóstico - ETL Subestações

## ⚠️ Problema: Nenhuma saída ao rodar o ETL

Se ao executar:
```bash
docker-compose exec etl python src/extractors/subestacoes_client.py
```

**Não aparecer nada**, siga este checklist:

---

## 🔍 Diagnóstico Passo a Passo

### 1️⃣ Verificar se o container está rodando
```powershell
docker-compose ps
# Procure pela linha "etl" com status "Up"
```

**Se não estiver:**
```powershell
docker-compose up -d etl
docker-compose ps  # Verificar novamente
```

---

### 2️⃣ Testar conexão com banco de dados
```powershell
docker-compose exec etl python -c "
from core import create_db_engine, load_settings
settings = load_settings()
print(f'DATABASE_URL: {settings.database.url}')
engine = create_db_engine(settings.database.url)
print('✅ Conexão com banco OK')
"
```

**Se erro:**
- Verificar se `postgres` está rodando: `docker-compose ps | findstr postgres`
- Verificar `.env` ou variáveis de ambiente

---

### 3️⃣ Testar se o arquivo foi alterado
```powershell
docker-compose exec etl python src/extractors/subestacoes_client.py 2>&1 | head -20
```

**Agora deve aparecer algo como:**
```
2026-01-21 10:30:45 - etl.subestacoes - INFO - 🚀 Iniciando extração de subestações...
2026-01-21 10:30:46 - etl.subestacoes - INFO - Extraindo dados de subestações...
...
```

---

### 4️⃣ Se ainda não aparecer nada, capturar erros
```powershell
docker-compose exec etl python -c "
import sys
sys.path.insert(0, '/app/src')
from extractors.subestacoes_client import run_extraction
from core import create_db_engine, load_settings

try:
    settings = load_settings()
    engine = create_db_engine(settings.database.url)
    result = run_extraction(engine=engine, settings=settings)
    print(f'Resultado: {result}')
except Exception as e:
    print(f'ERRO: {e}')
    import traceback
    traceback.print_exc()
"
```

---

### 5️⃣ Verificar logs do container
```powershell
# Ver últimas 50 linhas de logs
docker-compose logs -f etl --tail=50

# Ver logs com mais detalhes
docker-compose logs -f etl
```

---

## 🛠️ Soluções Comuns

### ❌ Problema: "ModuleNotFoundError"
```
ModuleNotFoundError: No module named 'geopandas'
```

**Solução:**
```powershell
# Reconstruir a imagem ETL
docker-compose build --no-cache etl
docker-compose up -d etl
docker-compose exec etl python src/extractors/subestacoes_client.py
```

---

### ❌ Problema: "Connection refused"
```
Error: could not connect to server: Connection refused
```

**Solução:**
```powershell
# Reiniciar banco de dados
docker-compose down
docker-compose up -d postgres
docker-compose up -d etl
docker-compose exec etl python src/extractors/subestacoes_client.py
```

---

### ❌ Problema: "Table does not exist"
```
Error: relation "subestacoes_ons" does not exist
```

**Solução:**
```powershell
# Recriar schema
Get-Content infrastructure/database/schema.sql | docker-compose exec -T postgres psql -U admin -d energy_monitor
docker-compose exec etl python src/extractors/subestacoes_client.py
```

---

## ✅ Checklist Completo

```bash
# 1. Container etl rodando
docker-compose ps | grep etl

# 2. Container postgres rodando
docker-compose ps | grep postgres

# 3. Schema atualizado
docker-compose exec postgres psql -U admin -d energy_monitor -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='subestacoes_ons';"

# 4. Conexão funcionando
docker-compose exec etl python -c "from core import create_db_engine, load_settings; engine = create_db_engine(load_settings().database.url); print('✅ OK')"

# 5. Arquivo com entry point (if __name__)
docker-compose exec etl grep -n "if __name__" src/extractors/subestacoes_client.py

# 6. Rodar ETL
docker-compose exec etl python src/extractors/subestacoes_client.py
```

---

## 🎯 Comando Completo para Resetar e Rodar

```powershell
# PowerShell (Windows)
docker-compose down
docker-compose up -d postgres
Start-Sleep -Seconds 5
Get-Content infrastructure/database/schema.sql | docker-compose exec -T postgres psql -U admin -d energy_monitor
docker-compose up -d etl
docker-compose exec etl python src/extractors/subestacoes_client.py
```

```bash
# Bash (Linux/Mac)
docker-compose down
docker-compose up -d postgres
sleep 5
cat infrastructure/database/schema.sql | docker-compose exec -T postgres psql -U admin -d energy_monitor
docker-compose up -d etl
docker-compose exec etl python src/extractors/subestacoes_client.py
```

---

## 📊 Resultado Esperado

```
2026-01-21 10:30:45 - etl.subestacoes - INFO - 🚀 Iniciando extração de subestações...
2026-01-21 10:30:46 - etl.subestacoes - INFO - Extraindo dados de subestações...
2026-01-21 10:30:47 - etl.subestacoes - INFO - Transformadas 6 subestações para GeoDataFrame.
2026-01-21 10:30:48 - etl.subestacoes - INFO - Tabela subestacoes_ons criada.
2026-01-21 10:30:49 - etl.subestacoes - INFO - Carregadas 6 subestações em subestacoes_ons.
✅ Pipeline concluída com sucesso: 6 subestações carregadas
```

---

## 📝 Verificar Dados Carregados

```bash
# Conectar ao banco
docker-compose exec postgres psql -U admin -d energy_monitor

# Dentro do psql:
SELECT COUNT(*) FROM subestacoes_ons;
SELECT nome, sigla_se, tensao_kv FROM subestacoes_ons LIMIT 5;
\q  # Sair
```

---

## 💡 Dica: Modo Debug

Se ainda tiver problemas, rodar com debug:

```powershell
docker-compose exec etl python -X dev src/extractors/subestacoes_client.py
```

Ou com verbosity:

```powershell
docker-compose exec etl python -vv src/extractors/subestacoes_client.py
```

---

**Problema resolvido? ✅**

Se mesmo assim não funcionar, tente:
```bash
docker-compose exec etl python src/extractors/subestacoes_client.py 2>&1
```

E copie **toda a saída** (erros e mensagens) para análise.
