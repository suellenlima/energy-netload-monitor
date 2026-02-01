# 🌍 ETL com Dados REAIS - Guia Completo

## 📊 Fontes de Dados Reais Disponíveis

### ✅ Implementadas e Testadas

| Fonte | Tipo | URL | Status | Dados |
|-------|------|-----|--------|-------|
| **ONS** | Subestações | `https://ons-aws-prod-opendata.s3.amazonaws.com/` | ✅ Online | ~2.000 subestações |
| **ANEEL SIGA** | Usinas GD | `https://dadosabertos.aneel.gov.br/` | ✅ Online | ~1M usinas |
| **OpenStreetMap** | Rede elétrica | `https://overpass-api.de/api/interpreter` | ✅ Online | Transformadores, linhas |

### 🔄 Em Desenvolvimento

| Fonte | Tipo | Acesso | Status |
|-------|------|--------|--------|
| **SCADA Distribuidoras** | Tempo real | API privada | 🔒 Requer credenciais |
| **ANEEL BIG** | Consumidores | Dados abertos | 🚧 Em análise |
| **Google Maps API** | Satélite | API paga | 💰 Opcional |

---

## 🚀 Quickstart: ETL Completo em 3 Comandos

### 1️⃣ Atualizar Schema do Banco

```powershell
# Conectar ao Docker
cd C:\Hackathon\Git\energy-netload-monitor

# Aplicar novo schema
Get-Content infrastructure\database\005_schema_dados_reais.sql | docker compose exec -T db psql -U admin -d energy_monitor
```

**O que faz:**
- Adiciona colunas para rastrear origem dos dados (`fonte_dados`)
- Cria tabela `usinas_geracao` para dados ANEEL
- Cria tabela `etl_execucao_log` para auditoria
- Adiciona índices de performance
- Cria views de qualidade de dados

---

### 2️⃣ Instalar Dependências Python

```powershell
pip install geopandas shapely requests psycopg2-binary pandas
```

**Pacotes:**
- `geopandas` - Análise espacial
- `shapely` - Geometrias (polígonos, pontos)
- `requests` - Download de APIs
- `psycopg2` - Conexão PostgreSQL

---

### 3️⃣ Executar ETL Completo

```powershell
python etl_area_cobertura_real.py --completo
```

**Tempo estimado:** 5-10 minutos  
**O que faz:**
1. ✅ Baixa subestações do ONS (~2.000 registros)
2. ✅ Filtra e carrega no banco
3. ✅ Baixa usinas ANEEL SIGA (~1M registros)
4. ✅ Filtra usinas solares (~500k)
5. ✅ Busca transformadores OSM para 5 primeiras SEs
6. ✅ Calcula área real baseada em transformadores

---

## 📋 Execução Detalhada por Fonte

### 🔌 FONTE 1: ONS - Subestações Reais

#### Extrair apenas ONS

```powershell
python etl_area_cobertura_real.py --ons
```

**Saída esperada:**
```
================================================================================
ETL: SUBESTAÇÕES ONS (Dados Reais)
================================================================================
🔌 Extraindo subestações do ONS (dados reais)...
✅ 1847 subestações extraídas do ONS
🔄 Transformando dados do ONS...
✅ 1823 subestações transformadas
💾 Carregando 1823 subestações no banco...
✅ 1823 subestações carregadas
================================================================================
✅ ETL ONS CONCLUÍDO: 1823 subestações
================================================================================
```

#### Verificar no banco

```sql
-- Contar subestações por fonte
SELECT fonte_dados, COUNT(*) as total
FROM subestacoes_detectadas
GROUP BY fonte_dados;

-- Subestações por subsistema
SELECT subsistema, COUNT(*) as total
FROM subestacoes_detectadas
WHERE fonte_dados = 'ONS'
GROUP BY subsistema
ORDER BY total DESC;

-- Top 10 subestações
SELECT nome, subsistema, tensao_nominal_kv, distribuidora
FROM subestacoes_detectadas
WHERE fonte_dados = 'ONS'
ORDER BY tensao_nominal_kv DESC
LIMIT 10;
```

---

### ☀️ FONTE 2: ANEEL SIGA - Usinas de Geração

#### Extrair apenas ANEEL

```powershell
python etl_area_cobertura_real.py --aneel
```

**Saída esperada:**
```
================================================================================
ETL: USINAS ANEEL SIGA (Dados Reais)
================================================================================
☀️ Extraindo usinas de geração (ANEEL SIGA)...
✅ 987234 usinas extraídas da ANEEL
☀️ 456789 usinas solares identificadas
🔄 Transformando dados ANEEL...
✅ 456234 usinas transformadas
💾 456234 usinas solares identificadas
================================================================================
✅ ETL ANEEL CONCLUÍDO: 456234 usinas solares
================================================================================
```

#### Verificar no banco

```sql
-- Contar usinas por tipo
SELECT tipo_geracao, COUNT(*) as total
FROM usinas_geracao
GROUP BY tipo_geracao
ORDER BY total DESC;

-- Usinas por estado
SELECT estado, COUNT(*) as total, 
       ROUND(SUM(potencia_outorgada_kw)/1000, 2) as potencia_mw
FROM usinas_geracao
WHERE tipo_geracao = 'UFV'
GROUP BY estado
ORDER BY total DESC
LIMIT 10;

-- Maiores usinas solares
SELECT nome, municipio, estado, 
       ROUND(potencia_outorgada_kw/1000, 2) as potencia_mw
FROM usinas_geracao
WHERE tipo_geracao = 'UFV'
ORDER BY potencia_outorgada_kw DESC
LIMIT 20;
```

---

### 🗺️ FONTE 3: OpenStreetMap - Transformadores

#### Extrair OSM para uma subestação específica

```powershell
# Para subestação ID=1
python etl_area_cobertura_real.py --osm 1
```

**Saída esperada:**
```
================================================================================
ETL: TRANSFORMADORES OSM para SE 1
================================================================================
🗺️ Extraindo transformadores do OSM (bbox: (-15.88, -48.00, -15.70, -47.82))...
✅ 47 transformadores extraídos do OSM
💾 Carregando 47 transformadores OSM...
✅ 47 transformadores carregados
📐 Calculando área de cobertura para SE 1...
✅ Área calculada: 23.45 km²
================================================================================
✅ ETL OSM CONCLUÍDO: 47 transformadores
================================================================================
```

#### Verificar no banco

```sql
-- Transformadores por fonte
SELECT fonte_dados, COUNT(*) as total
FROM transformadores
GROUP BY fonte_dados;

-- Transformadores por subestação (OSM)
SELECT se.nome, COUNT(t.id) as total_transformadores,
       ROUND(SUM(t.potencia_kva), 2) as potencia_total
FROM transformadores t
JOIN subestacoes_detectadas se ON se.id = t.subestacao_id
WHERE t.fonte_dados = 'OSM'
GROUP BY se.nome
ORDER BY total_transformadores DESC;

-- Área de cobertura calculada
SELECT se.nome, ac.area_km2, ac.metodo_definicao,
       ac.fonte_dados, ac.observacoes
FROM subestacoes_area_cobertura ac
JOIN subestacoes_detectadas se ON se.id = ac.subestacao_id
WHERE ac.fonte_dados = 'calculado'
ORDER BY ac.area_km2 DESC;
```

---

## 📊 Consultas de Validação

### Qualidade dos Dados

```sql
-- View de qualidade geral
SELECT * FROM vw_qualidade_dados
ORDER BY tabela, fonte_dados;
```

**Exemplo de saída:**
```
tabela          | fonte_dados | total_registros | sem_coordenadas | sem_nome | qualidade_percentual
----------------|-------------|-----------------|-----------------|----------|--------------------
subestacoes     | ONS         | 1823            | 0               | 12       | 99.34
subestacoes     | satelite    | 1715            | 0               | 0        | 100.00
transformadores | OSM         | 247             | 0               | 3        | 98.79
usinas          | ANEEL       | 456234          | 234             | 1234     | 99.68
```

### Cobertura Completa

```sql
-- View de cobertura por subestação
SELECT * FROM vw_cobertura_subestacoes
WHERE area_km2 IS NOT NULL
ORDER BY area_km2 DESC
LIMIT 20;
```

**Exemplo de saída:**
```
nome                    | fonte_subestacao | area_km2 | transformadores_ativos | consumidores_ativos | geracao_distribuida_kw
------------------------|------------------|----------|------------------------|---------------------|----------------------
SE Brasília Sul         | ONS              | 245.67   | 47                     | 0                   | 12340.5
SE Campinas             | ONS              | 189.23   | 38                     | 0                   | 8975.2
SE Rio Centro           | satelite         | 156.78   | 29                     | 0                   | 0
```

### Comparação Circular vs Real

```sql
-- Comparar área circular (π×r²) vs área real (convex hull)
WITH areas_comparadas AS (
    SELECT 
        se.id,
        se.nome,
        ac.area_km2 as area_real,
        PI() * POWER(10, 2) as area_circular_10km,
        ROUND(
            100.0 * ac.area_km2 / (PI() * POWER(10, 2)),
            2
        ) as percentual_real
    FROM subestacoes_detectadas se
    JOIN subestacoes_area_cobertura ac ON ac.subestacao_id = se.id
    WHERE ac.area_km2 IS NOT NULL
)
SELECT 
    nome,
    ROUND(area_real, 2) as area_real_km2,
    ROUND(area_circular_10km, 2) as area_circular_km2,
    percentual_real || '%' as percentual_area_real
FROM areas_comparadas
ORDER BY area_real DESC
LIMIT 20;
```

---

## 🔄 Atualização Periódica

### Cron Job (Linux/Mac)

Criar script `update_etl.sh`:

```bash
#!/bin/bash
cd /caminho/energy-netload-monitor

# Atualizar ONS (semanal)
python etl_area_cobertura_real.py --ons >> logs/etl_ons.log 2>&1

# Atualizar ANEEL (mensal)
python etl_area_cobertura_real.py --aneel >> logs/etl_aneel.log 2>&1

# Atualizar OSM (diário para 10 primeiras SEs)
for i in {1..10}; do
    python etl_area_cobertura_real.py --osm $i >> logs/etl_osm.log 2>&1
done

# Log de execução
echo "ETL executado em $(date)" >> logs/etl_execucoes.log
```

Agendar:
```bash
# Executar todo domingo às 3h
0 3 * * 0 /caminho/update_etl.sh
```

### Task Scheduler (Windows)

```powershell
# Criar tarefa
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\Hackathon\Git\energy-netload-monitor\etl_area_cobertura_real.py --completo" `
    -WorkingDirectory "C:\Hackathon\Git\energy-netload-monitor"

$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Sunday `
    -At "3:00AM"

Register-ScheduledTask `
    -TaskName "ETL Area Cobertura Real" `
    -Action $action `
    -Trigger $trigger `
    -Description "Atualização semanal de dados do ONS, ANEEL e OSM"
```

---

## 🐛 Troubleshooting

### Erro: "Connection refused to database"

```powershell
# Verificar se Docker está rodando
docker ps

# Reiniciar containers
docker compose restart

# Verificar logs
docker compose logs db
```

### Erro: "Timeout connecting to ONS"

```python
# Aumentar timeout no código
response = requests.get(ONS_SUBESTACOES_URL, timeout=60)  # Era 30
```

### Erro: "No module named 'geopandas'"

```powershell
# Instalar com conda (recomendado)
conda install geopandas

# OU com pip (pode precisar de GDAL)
pip install geopandas
```

### Erro: "Shapely geometry is invalid"

```sql
-- Limpar geometrias inválidas
UPDATE subestacoes_detectadas
SET localizacao = ST_MakeValid(localizacao)
WHERE NOT ST_IsValid(localizacao);
```

---

## 📈 Monitoramento

### Log de Execuções

```sql
-- Últimas execuções
SELECT tipo_etl, fonte_dados, data_execucao, status,
       registros_inseridos, duracao_segundos
FROM etl_execucao_log
ORDER BY data_execucao DESC
LIMIT 20;

-- Taxa de sucesso por tipo
SELECT tipo_etl,
       COUNT(*) as total_execucoes,
       COUNT(CASE WHEN status = 'sucesso' THEN 1 END) as sucessos,
       ROUND(100.0 * COUNT(CASE WHEN status = 'sucesso' THEN 1 END) / COUNT(*), 2) as taxa_sucesso
FROM etl_execucao_log
GROUP BY tipo_etl;

-- Tempo médio de execução
SELECT tipo_etl,
       COUNT(*) as execucoes,
       ROUND(AVG(duracao_segundos), 2) as duracao_media_seg,
       ROUND(MAX(duracao_segundos), 2) as duracao_maxima_seg
FROM etl_execucao_log
WHERE status = 'sucesso'
GROUP BY tipo_etl;
```

### Alertas

```sql
-- Execuções com falha nas últimas 24h
SELECT * FROM etl_execucao_log
WHERE status = 'falha'
  AND data_execucao >= NOW() - INTERVAL '24 hours'
ORDER BY data_execucao DESC;

-- Dados desatualizados (>7 dias)
SELECT se.nome, ac.data_atualizacao,
       NOW() - ac.data_atualizacao as tempo_desatualizado
FROM subestacoes_detectadas se
JOIN subestacoes_area_cobertura ac ON ac.subestacao_id = se.id
WHERE ac.data_atualizacao < NOW() - INTERVAL '7 days'
ORDER BY ac.data_atualizacao ASC;
```

---

## 🎯 Próximos Passos

### 1. Integração SCADA

```python
# Implementar em etl_area_cobertura_real.py
class SCADAClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.headers = {'Authorization': f'Bearer {api_key}'}
    
    def get_transformadores(self, subestacao_id):
        response = requests.get(
            f"{self.base_url}/subestacoes/{subestacao_id}/transformadores",
            headers=self.headers
        )
        return response.json()
```

### 2. Google Maps API (Alta Resolução)

```python
def extrair_imagem_google_maps(lat, lon, api_key, zoom=20):
    """Extrair imagem de satélite de alta resolução"""
    url = f"https://maps.googleapis.com/maps/api/staticmap"
    params = {
        'center': f'{lat},{lon}',
        'zoom': zoom,
        'size': '640x640',
        'maptype': 'satellite',
        'key': api_key
    }
    response = requests.get(url, params=params)
    return response.content
```

### 3. Machine Learning (Detecção Automática)

- YOLO para detectar transformadores em imagens
- Segmentação de telhados com painéis solares
- Classificação de tipo de subestação

---

## 📞 Suporte

**Arquivos criados:**
- `etl_area_cobertura_real.py` - ETL com dados reais
- `infrastructure/database/005_schema_dados_reais.sql` - Schema atualizado
- `documentation/ETL_DADOS_REAIS.md` - Este guia

**Comandos úteis:**
```powershell
# Ajuda
python etl_area_cobertura_real.py --help

# ETL completo
python etl_area_cobertura_real.py --completo

# Apenas uma fonte
python etl_area_cobertura_real.py --ons
python etl_area_cobertura_real.py --aneel
python etl_area_cobertura_real.py --osm 1
```

---

**Versão:** 2.0 (Dados Reais)  
**Data:** 31/01/2026  
**Status:** ✅ Implementado e testado com fontes reais  
**Fontes:** ONS, ANEEL SIGA, OpenStreetMap
