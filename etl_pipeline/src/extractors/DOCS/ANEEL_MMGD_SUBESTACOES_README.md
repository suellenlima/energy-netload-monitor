# ETL ANEEL MMGD com Granularidade por Subestação

## 🎯 Objetivo

Extrair dados reais de Geração Distribuída (MMGD) da ANEEL com granularidade por **distribuidora E subestação**, possibilitando análise detalhada da penetração solar por localização específica dentro de cada rede de distribuição.

## 📊 Dados Disponíveis

### Fonte de Dados
- **Fonte**: ANEEL - Agência Nacional de Energia Elétrica
- **Dataset**: "Relação de Empreendimentos de Geração Distribuída"
- **URL**: https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2
- **Recurso**: https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv
- **Frequency**: Daily (última atualização: 2026-02-05)
- **Tamanho**: 3.9 milhões de registros
- **Encoding**: latin-1
- **Separador**: semicolon (;)
- **Decimal**: Comma (,) - padrão brasileiro

### Campos Capturados
| Campo ANEEL | Mapeamento | Descrição | Tipo |
|-------------|-----------|-----------|------|
| `NomAgente` | distribuidora | Nome da distribuidora/agente | TEXT |
| `NomSubEstacao` | subestacao | Nome da subestação | TEXT |
| `SigTipoGeracao` | fonte_geracao | Tipo de geração (Solar/Eólica/Hidro/Biomassa) | TEXT |
| `MdaPotenciaInstaladaKW` | potencia_total_kw | Potência instalada em kW | FLOAT8 |
| N/A | quantidade_empreendimentos | Contagem de empreendimentos por grupo | INT |

### Tipos de Geração Suportados
- ☀️ **Solar** (UFV, FV, SOL)
- 💨 **Eólica** (EOL, EÓLICA)
- 💧 **Hidro** (HIDR, PCH, CGH, HIDRO)
- 🌾 **Biomassa** (BIOMASSA, BIOGÁS, BIO)
- 🔧 **Outro** (qualquer tipo não mapeado)

## 🗄️ Estrutura do Banco de Dados

### Tabela: `geracao_mmgd_distribuidora`

```sql
CREATE TABLE geracao_mmgd_distribuidora (
    id BIGSERIAL PRIMARY KEY,
    distribuidora TEXT NOT NULL,           -- Nome da distribuidora
    subsistema TEXT,                       -- Subsistema (Norte/Nordeste/Sudeste/Sul/Centro-Oeste)
    subestacao TEXT,                       -- Nome da subestação ← NOVO!
    fonte_geracao TEXT,                    -- Tipo de geração
    potencia_total_kw FLOAT8 NOT NULL,     -- Potência agregada em kW
    quantidade_empreendimentos INT,        -- Contagem de empreendimentos
    data_medicao TIMESTAMP WITH TIME ZONE, -- Data de medição
    data_insercao TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Constraints
UNIQUE (distribuidora, subestacao, fonte_geracao, data_medicao)

-- Índices
CREATE INDEX idx_geracao_mmgd_distribuidora_name ON geracao_mmgd_distribuidora(distribuidora);
CREATE INDEX idx_geracao_mmgd_distribuidora_subsistema ON geracao_mmgd_distribuidora(subsistema);
CREATE INDEX idx_geracao_mmgd_distribuidora_subestacao ON geracao_mmgd_distribuidora(subestacao);
```

## 📈 Estatísticas de Dados

### Cobertura
- **Total de Registros**: 604 (agregados por distribuidora + subestação + tipo)
- **Subestações Únicas**: 399
- **Distribuidoras Cobertas**: 103
- **Tipos de Geração**: 5 (Solar, Eólica, Hidro, Biomassa, Outro)

### Top 5 Distribuidoras (por Capacity MW)
| Distribuidor | Subestações | Potência (MW) | Empreendimentos |
|---|---|---|---|
| CEMIG | 1 | 20,922 | 1,501,728 |
| COPEL | 20 | 15,976 | 1,199,240 |
| ENERGISA MATO GROSSO | 1 | 11,857 | 886,176 |
| CPFL | 32 | 11,326 | 1,138,484 |
| RGE SUL | 132 | 10,516 | 1,151,232 |

## 🔄 Pipeline de ETL

### Arquivo
`/etl_pipeline/src/extractors/aneel_mmgd_distribuidora.py`

### Funções Principais

#### 1. `criar_tabela_mmgd_distribuidora(engine)`
- ✅ Cria tabela (IF NOT EXISTS)
- ✅ Adiciona coluna `subestacao` com migration
- ✅ Cria índices e constraints
- ✅ Trata erros de transação com separate connections

#### 2. `baixar_dados_aneel() → DataFrame`
- 📥 Baixa CSV de 3.9M registros
- ✅ Trata encoding latin-1
- ✅ Detecta separador (semicolon)
- ⏱️ Tempo: ~2-3 minutos

#### 3. `transformar_dados_distribuidora(df) → DataFrame`
- 🔍 Detecta automaticamente colunas de:
  - Distribuidora (nomagente, etc)
  - Subestação **(novo)**
  - Tipo de geração (sigtipogeracao, etc)
  - Potência (mdapotenciainstaladakw, etc)
- 🔧 Normaliza nomes (UPPER, strip)
- 🔧 Converte decimal (vírgula → ponto)
- 📊 Agrupa por: distribuidora + subestacao + tipo
- 📌 Mapeia subsistema (por distribuidora)

**Transformação Chave (Aggregation):**
```python
df_agrupado = df.groupby(
    [dist_col, subestacao_col, "tipo_geracao_normalizado"],  # ← Subestação adicionada!
    as_index=False
).agg({
    pot_col: "sum",           # Soma potências
    df.columns[0]: "count"    # Conta empreendimentos
})
```

#### 4. `carregar_mmgd_banco(df, engine) → int`
- 💾 Insere/atualiza registros
- ⚡ Usa ON CONFLICT para upsert
- 📋 Coluna subestacao incluída no INSERT
- ✅ Retorna contagem de registros carregados

#### 5. `executar_etl_mmgd_distribuidora()`
- 🔗 Orquestra o pipeline completo
- 📋 Gera resumo com exemplos de dados
- ⏱️ Tempo total: ~5 minutos

## 🚀 Como Usar

### Execução Manual
```bash
docker compose exec etl python src/extractors/aneel_mmgd_distribuidora.py
```

### Verificação de Dados
```sql
-- Ver dados por subestação
SELECT distribuidora, subestacao, fonte_geracao, potencia_total_kw 
FROM geracao_mmgd_distribuidora 
WHERE subestacao != '' 
LIMIT 30;

-- Contar subestações por distribuidora
SELECT distribuidora, COUNT(DISTINCT subestacao) as n_subestacoes
FROM geracao_mmgd_distribuidora
GROUP BY distribuidora
ORDER BY n_subestacoes DESC;

-- Potência por subestação da COPEL
SELECT subestacao, fonte_geracao, potencia_total_kw, quantidade_empreendimentos
FROM geracao_mmgd_distribuidora
WHERE distribuidora = 'COPEL DISTRIBUICAO S.A.'
AND subestacao != ''
ORDER BY potencia_total_kw DESC;
```

## 🎛️ Configuração

### Variáveis de Ambiente (Banco de Dados)
```env
DATABASE_URL=postgresql://admin:admin123@db:5432/energy_monitor
```

### Constantes do ETL
```python
ANEEL_MMGD_API_URL = "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"

SUBSISTEMA_DISTRIBUIDORAS = {
    "Sudeste": ["LIGHT", "CPFL", "ELEKTRO", ...],
    "Nordeste": ["COELBA", "CEPE", ...],
    ...
}
```

## 🛠️ Tratamento de Erros

### Problemas Resolvidos

#### 1. ❌ Decimal Separator (vírgula brasileira)
**Problema**: CSV usa `1.234,56` mas Python espera `1234.56`
**Solução**: `.astype(str).str.replace(',', '.')`

#### 2. ❌ Pandas Groupby Duplicate Columns
**Problema**: `reset_index()` duplicava coluna de agrupamento
**Solução**: `groupby(..., as_index=False).agg(...)`

#### 3. ❌ Transaction Abort em PostgreSQL
**Problema**: Erros em uma transação abortam toda a conexão
**Solução**: Usar `with engine.connect()` separado para cada operação

#### 4. ❌ Constraint Mismatch (ON CONFLICT)
**Problema**: Constraint `(dist, fonte, data)` mas INSERT usa `(dist, subest, fonte, data)`
**Solução**: Atualizar constraint com migration DROP + ADD

## 📅 Histório de Versões

### v2.0 - Subestação-level (2026-02-06)
- ✅ Adicionado suporte para granularidade por subestação
- ✅ Extrair campo `NomSubEstacao` do CSV ANEEL
- ✅ Agregar dados também por subestação
- ✅ Criar índice em subestacao
- ✅ 399 subestações únicas capturadas
- 📊 604 registros com subestação-level data

### v1.0 - Distribuidora-level (2026-02-05)
- ✅ Primeiro release com dados por distribuidora
- ✅ 103 distribuidoras cobertas
- ✅ 5 tipos de geração suportados

## 🔐 Limitações e Considerações

### Dados Encontrados
- ✅ Dados reais da ANEEL
- ✅ Frequência daily
- ✅ Cobertura de 103 distribuidoras brasileiras

### Limitações Conhecidas
- ⚠️ Alguns registros têm subestação vazia ("Não especificada")
- ⚠️ ANEEL não fornece dados *em tempo real* (diária)
- ⚠️ Dados agregados por distribuidora, não por subestação individual em tempo real
- ⚠️ Para dados de subsistema em tempo real, usar ONS (mas apenas por subsistema)

### Dados NÃO Incluídos
- ❌ Geração centralizada (usinas)
- ❌ Dados de fluxo de carga (apenas capacidade instalada)
- ❌ Dados de compensação reativa
- ❌ Dados de rampa solar/eólica

## 🔗 Integração com KPIs

### KPI: "Penetração Solar" (obter_carga_oculta)
```python
SELECT 
    SUM(potencia_total_kw) as mmgd_solar_kw
FROM geracao_mmgd_distribuidora
WHERE fonte_geracao = 'Solar'
AND distribuidora = ? -- por distribuidora
AND subestacao = ?    -- ← Novo filtro possível!
```

### Filtros Sugeridos
- Por distribuidora
- Por subestação (NEW!)
- Por tipo de geração
- Por intervalo de data
- Combinações

## 📚 Referências

- ANEEL Dataset: https://dadosabertos.aneel.gov.br/
- ANEEL Geração Distribuída: https://www.aneel.gov.br/
- ONS API: https://www.ons.org.br/paginas/energia-agora/dados-da-operacao

## 👤 Desenvolvedor

- Implementação: ETL Pipeline
- Data Source: ANEEL Public Data
- Granularidade Subestação: v2.0 (2026-02-06)
