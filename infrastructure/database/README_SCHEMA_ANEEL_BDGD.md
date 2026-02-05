# Schema ANEEL BDGD - Documentação Completa

## 📋 Visão Geral

O arquivo `schema.sql (unificado)` define a **estrutura de dados única** para infraestrutura elétrica ANEEL BDGD e detecção de telhados com painéis solares. Contém:

- **8 tabelas principais** para transformadores, subestações, consumidores (BT/MT/AT) e telhados
- **5 views** pré-calculadas para análise rápida
- **3 funções** de validação de dados
- **Índices otimizados** para queries comuns
- **Constraints** para garantir integridade

### Princípios de Design

✅ **Single Source of Truth**: SQL schema como autoridade de dados  
✅ **Validação em Banco**: Constraints CHECK, validações de domínio  
✅ **PostGIS Integrado**: Suporte completo a geometrias (Point)  
✅ **Type Safety**: ENUMs para categorias controladas  
✅ **Auditoria**: Timestamps automáticos (data_criacao, data_atualizacao)  
✅ **Performance**: Índices GiST para buscas geoespaciais, índices simples para filtros

---

## 📦 Extensões PostgreSQL Necessárias

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
```

---

## 📊 Estrutura de Tabelas

### Nível 1: Infraestrutura de Distribuição

#### 1. **transformadores_aneel**
Dados de transformadores de distribuição (camada UNTRD do BDGD)

| Campo | Tipo | Constraints | Descrição |
|-------|------|------------|-----------|
| `id` | SERIAL | PRIMARY KEY | Identificador único |
| `codigo` | VARCHAR(50) | UNIQUE NOT NULL | Código do transformador |
| `nome` | VARCHAR(200) | | Nome/descrição |
| `distribuidora` | VARCHAR(100) | NOT NULL | Distribuidora responsável |
| `subestacao_codigo` | VARCHAR(50) | | Código da subestação associada |
| `potencia_kva` | DECIMAL(10,2) | CHECK > 0 | Potência nominal |
| `tensao_primaria_kv` | DECIMAL(10,2) | CHECK > 0 | Tensão primária |
| `tensao_secundaria_kv` | DECIMAL(10,2) | CHECK > 0 | Tensão secundária |
| `tipo_tensao` | VARCHAR(10) | IN ('BT','MT','AT') | Classificação automática |
| `latitude` | DECIMAL(10,7) | CHECK [-90,90] | Coordenada Y |
| `longitude` | DECIMAL(11,7) | CHECK [-180,180] | Coordenada X |
| `localizacao` | GEOMETRY(Point,4326) | | Ponto geográfico (PostGIS) |
| `ativo` | BOOLEAN | DEFAULT TRUE | Status operacional |
| `data_criacao` | TIMESTAMP | DEFAULT NOW() | Data de inserção |
| `data_atualizacao` | TIMESTAMP | DEFAULT NOW() | Data de última atualização |

**Índices:**
```
idx_transformadores_aneel_codigo           -- Busca por código
idx_transformadores_aneel_tipo_tensao      -- Filtro por tensão
idx_transformadores_aneel_distribuidora    -- Filtro por distribuidora
idx_transformadores_aneel_localizacao      -- GIST (busca geoespacial)
idx_transformadores_aneel_coordenadas      -- Filtro por lat/lon
idx_transformadores_aneel_data             -- Ordenação temporal
```

---

#### 2. **subestacoes_aneel**
Dados de subestações (agrupamento de barramentos, camada CTMT)

| Campo | Tipo | Constraints | Descrição |
|-------|------|------------|-----------|
| `id` | SERIAL | PRIMARY KEY | |
| `codigo` | VARCHAR(50) | UNIQUE NOT NULL | Código da subestação |
| `nome` | VARCHAR(200) | | Nome/descrição |
| `distribuidora` | VARCHAR(100) | NOT NULL | Distribuidora |
| `tensao_kv` | DECIMAL(10,2) | CHECK > 0 | Tensão operacional |
| `tensao_operacao_kv` | DECIMAL(10,2) | CHECK > 0 | Tensão de operação |
| `latitude` | DECIMAL(10,7) | CHECK [-90,90] | Coordenada Y |
| `longitude` | DECIMAL(11,7) | CHECK [-180,180] | Coordenada X |
| `localizacao` | GEOMETRY(Point,4326) | | Ponto geográfico (PostGIS) |
| `ativo` | BOOLEAN | DEFAULT TRUE | |
| `codigo_ons` | VARCHAR(100) | | Referência ONS |

**Índices:**
```
idx_subestacoes_aneel_codigo               -- Busca por código
idx_subestacoes_aneel_distribuidora        -- Filtro por distribuidora
idx_subestacoes_aneel_tensao               -- Filtro por tensão
idx_subestacoes_aneel_localizacao          -- GIST (busca geoespacial)
idx_subestacoes_aneel_codigo_ons           -- Busca por referência ONS
idx_subestacoes_aneel_fonte                -- Filtro por fonte dados
```

---

### Nível 2: Consumidores (Separados por Tensão)

#### 3. **consumidores_bt_aneel** (Baixa Tensão - UCBT)
Unidades consumidoras em baixa tensão

| Campo Crítico | Tipo | Descrição |
|---------------|------|-----------|
| `id` | BIGSERIAL | PRIMARY KEY |
| `codigo` | VARCHAR(50) | UNIQUE NOT NULL |
| `distribuidora` | VARCHAR(100) | NOT NULL |
| `subestacao_codigo` | VARCHAR(50) | Referência para subestação |
| `transformador_mt_codigo` | VARCHAR(50) | Transformador associado |
| `carga_instalada_kw` | DECIMAL(12,2) | Potência do consumidor |
| `latitude`, `longitude` | DECIMAL | Coordenadas geográficas |
| `localizacao` | GEOMETRY(Point,4326) | Ponto geográfico |
| `energia_01` a `energia_12` | DECIMAL(15,2) | 12 períodos de consumo (kWh) |
| `dic_01` a `dic_12` | DECIMAL(10,2) | Duração interrupção (h) |
| `fic_01` a `fic_12` | DECIMAL(10,2) | Frequência interrupção (h) |
| `ativo` | BOOLEAN | DEFAULT TRUE |

**Índices:**
```
idx_consumidores_bt_codigo                 -- Busca por código
idx_consumidores_bt_distribuidora          -- Filtro por distribuidora
idx_consumidores_bt_subestacao             -- Filtro por subestação
idx_consumidores_bt_transformador_mt       -- Filtro por transformador
idx_consumidores_bt_localizacao            -- GIST (busca geoespacial)
idx_consumidores_bt_carga                  -- Ordenação por potência
```

---

#### 4. **consumidores_mt_aneel** (Média Tensão - UCMT)
Unidades consumidoras em média tensão

**Campos adicionais vs BT:**
- `demanda_01` a `demanda_12`: Demanda máxima 12 períodos (kW)
- `demanda_contratada_kw`: Demanda contratada
- `circuito_mt_codigo`: Referência ao circuito MT
- `transformador_at_codigo`: Transformador AT associado

---

#### 5. **consumidores_at_aneel** (Alta Tensão - UCAT)
Unidades consumidoras em alta tensão

**Campos adicionais vs MT:**
- `demanda_ponta_01` a `demanda_ponta_12`: Demanda máxima na ponta (kW)
- `demanda_fora_ponta_01` a `demanda_fora_ponta_12`: Demanda máxima fora ponta (kW)
- `energia_ponta_01` a `energia_ponta_12`: Energia ativa na ponta (kWh)
- `energia_fora_ponta_01` a `energia_fora_ponta_12`: Energia ativa fora ponta (kWh)

---

### Nível 3: Dimensões e Auditoria

#### 6. **distribuidoras_aneel** (Dimensão)
Tabela de metadados com estatísticas consolidadas

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | SERIAL | PRIMARY KEY |
| `nome` | VARCHAR(100) | UNIQUE (ex: 'COPEL', 'IENERGIA_87') |
| `codigo_arquivo` | VARCHAR(200) | Código no arquivo original |
| `estado` | VARCHAR(2) | UF (ex: 'PR', 'SP') |
| `regiao` | VARCHAR(50) | Região geográfica |
| `total_transformadores` | INT | COUNT agregado |
| `total_subestacoes` | INT | COUNT agregado |
| `total_consumidores` | INT | COUNT agregado |
| `potencia_total_kva` | DECIMAL(15,2) | SUM agregado |

**Índices:**
```
idx_distribuidoras_aneel_nome
idx_distribuidoras_aneel_codigo
idx_distribuidoras_aneel_estado
```

---

#### 7. **aneel_bdgd_processamento** (Log de Auditoria)
Rastreamento de execução de ETL

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | BIGSERIAL | PRIMARY KEY |
| `data_inicio` | TIMESTAMP | Início do processo |
| `data_fim` | TIMESTAMP | Fim do processo |
| `tempo_total_segundos` | FLOAT | Duração total |
| `distribuidora_processada` | VARCHAR(100) | Qual distribuidora |
| `transformadores_inseridos` | INTEGER | Novos registros |
| `transformadores_atualizados` | INTEGER | Atualizações |
| `subestacoes_inseridas` | INTEGER | |
| `subestacoes_atualizadas` | INTEGER | |
| `consumidores_inseridos` | INTEGER | |
| `consumidores_atualizados` | INTEGER | |
| `status` | VARCHAR(20) | 'em_processamento' \| 'concluido' \| 'erro' |
| `mensagem_erro` | TEXT | Descrição de erro |
| `parametros_execucao` | JSONB | Parâmetros utilizados |

**Índices:**
```
idx_aneel_processamento_data               -- Ordenação temporal
idx_aneel_processamento_distribuidora      -- Filtro por distribuidora
idx_aneel_processamento_status             -- Filtro por status
```

---

### Nível 4: Detecção de Telhados ⭐ (NOVO)

#### 8. **telhados_detectados_transformador**
Telhados com painéis solares detectados por transformador

| Campo | Tipo | Constraints | Descrição |
|-------|------|------------|-----------|
| `id` | SERIAL | PRIMARY KEY | |
| `transformador_id` | INTEGER | FK → transformadores_aneel | Transformador associado |
| `subestacao_id` | INTEGER | FK → subestacoes_aneel | Subestação associada |
| `latitude` | DOUBLE PRECISION | NOT NULL | Coordenada Y |
| `longitude` | DOUBLE PRECISION | NOT NULL | Coordenada X |
| `area_m2` | DOUBLE PRECISION | NOT NULL | Área do telhado em m² |
| `confianca` | DOUBLE PRECISION | CHECK [0,1] | Score de confiança (0-1) |
| `bbox_json` | JSONB | | Bounding box em pixels (opcional) |
| `fonte_imagem` | VARCHAR(50) | DEFAULT 'google_maps' | google_maps \| sentinel2 \| etc |
| `resolucao_cm` | DOUBLE PRECISION | DEFAULT 30.0 | Resolução em cm/pixel |
| `timestamp_deteccao` | TIMESTAMP | NOT NULL | Data de detecção |
| `timestamp_criacao` | TIMESTAMP | DEFAULT NOW() | Data de inserção em DB |
| `timestamp_atualizacao` | TIMESTAMP | DEFAULT NOW() | Última atualização |
| `url_imagem_origem` | TEXT | | URL para recuperar imagem original |

**Índices:**
```
idx_telhados_trafo_transformador           -- JOIN com transformadores
idx_telhados_trafo_subestacao              -- JOIN com subestações
idx_telhados_trafo_timestamp               -- Ordenação temporal
idx_telhados_trafo_confianca               -- Filtro por confiança
```

---

## 🔍 Views Úteis

### 1. **v_aneel_cobertura_resumo**
Resumo de cobertura de dados por distribuidora

```sql
SELECT * FROM v_aneel_cobertura_resumo 
WHERE tipo = 'transformadores' 
ORDER BY cobertura_geografica_pct DESC;
```

**Colunas:**
- `tipo`: 'transformadores' ou 'consumidores'
- `distribuidora`: Nome da distribuidora
- `total`: Total de registros
- `com_coordenadas`: Quantos têm lat/lon válidas
- `com_dados_tecnicos`: Quantos têm dados completos
- `potencia_media_kva`: Potência média

---

### 2. **vw_telhados_completo**
Telhados com contexto de transformador e subestação

```sql
SELECT * FROM vw_telhados_completo
WHERE confianca >= 0.80
ORDER BY timestamp_deteccao DESC;
```

**Colunas:**
- Dados do telhado: id, lat, lon, area_m2, confianca, fonte_imagem, timestamp_deteccao
- Contexto transformador: transformador_codigo, transformador_nome, potencia_kva, distribuidora
- Contexto subestação: subestacao_codigo, subestacao_nome

---

### 3. **vw_telhados_estatisticas**
Estatísticas gerais agregadas de telhados

```sql
SELECT * FROM vw_telhados_estatisticas;
```

**Colunas:**
- `total_telhados`: Total detectado
- `total_transformadores`: Transformadores com telhados
- `total_subestacoes`: Subestações com telhados
- `area_media_m2`: Área média
- `area_total_m2`: Área total
- `confianca_media`: Confiança média
- `confianca_minima`, `confianca_maxima`: Range de confiança
- `primeira_deteccao`, `ultima_deteccao`: Período de detecção

---

### 4. **vw_telhados_por_subestacao**
Agregações de telhados por subestação

```sql
SELECT * FROM vw_telhados_por_subestacao
WHERE total_telhados > 0
ORDER BY area_total_m2 DESC
LIMIT 10;
```

**Colunas por subestação:**
- `total_telhados`: Quantidade detectada
- `total_transformadores`: Transformadores com telhados
- `area_media_m2`, `area_total_m2`: Agregações de área
- `confianca_media`: Confiança média
- `ultima_deteccao`: Última data detectada

---

### 5. **vw_telhados_por_transformador**
Agregações de telhados por transformador

```sql
SELECT * FROM vw_telhados_por_transformador
WHERE total_telhados > 0
ORDER BY area_total_m2 DESC;
```

**Colunas por transformador:**
- `transformador_id`, `transformador_codigo`, `transformador_nome`
- `potencia_kva`: Potência do transformador
- `subestacao_codigo`: Subestação associada
- `total_telhados`: Quantidade de telhados
- `area_media_m2`, `area_total_m2`: Agregações
- `confianca_media`: Confiança média

---

## 🔒 Funções de Validação

### 1. **fn_validar_coordenadas(lat, lon)**
Valida se coordenadas estão dentro dos limites válidos

```sql
SELECT fn_validar_coordenadas(-25.5, -49.3);   -- TRUE
SELECT fn_validar_coordenadas(95, -49.3);      -- EXCEPTION: Latitude fora de range
```

Regras:
- Latitude: -90 ≤ lat ≤ 90
- Longitude: -180 ≤ lon ≤ 180
- NULL é válido (significa coordenada não disponível)

**Usada em Constraints:**
```sql
CONSTRAINT chk_transformadores_coordenadas 
    CHECK (fn_validar_coordenadas(latitude, longitude))
```

---

### 2. **fn_validar_potencia(potencia)**
Valida se potência é positiva

Regras:
- Potência > 0 se definida
- NULL é válido

**Usada em Constraints:**
```sql
CONSTRAINT chk_transformadores_potencia_positiva 
    CHECK (potencia_kva IS NULL OR potencia_kva > 0)
```

---

### 3. **fn_validar_tensao(tensao)**
Valida se tensão é positiva

Regras:
- Tensão > 0 se definida
- NULL é válido

**Usada em Constraints:**
```sql
CONSTRAINT chk_transformadores_tensao_positiva_pri 
    CHECK (tensao_primaria_kv IS NULL OR tensao_primaria_kv > 0)
```

---

## 🔐 Constraints de Domínio

### ENUMs

```
enum_tipo_tensao: 'BT' | 'MT' | 'AT' | 'DESCONHECIDO'
enum_metodo_calculo: 'convex_hull' | 'buffer_500m' | 'buffer_1km' | 'buffer_2km' | 'buffer_5km'
enum_status_processamento: 'em_processamento' | 'concluido' | 'erro' | 'parcial'
```

### CHECK Constraints

| Tabela | Campo | Regra |
|--------|-------|-------|
| transformadores_aneel | potencia_kva | > 0 \| NULL |
| transformadores_aneel | tensao_primaria_kv | > 0 \| NULL |
| transformadores_aneel | tensao_secundaria_kv | > 0 \| NULL |
| transformadores_aneel | tipo_tensao | IN ('BT','MT','AT') \| NULL |
| subestacoes_aneel | tensao_kv | > 0 \| NULL |
| subestacoes_aneel | tensao_operacao_kv | > 0 \| NULL |
| telhados_detectados_transformador | confianca | [0, 1] |

---

## 🎯 Casos de Uso Comuns

### 1. Telhados com alta confiança agregados por transformador

```sql
SELECT 
    t.transformador_codigo,
    t.transformador_nome,
    COUNT(*) as total_telhados,
    ROUND(SUM(t.area_m2), 2) as area_total_m2,
    ROUND(AVG(t.confianca), 3) as confianca_media,
    ROUND(MIN(t.confianca), 3) as confianca_minima
FROM vw_telhados_completo t
WHERE t.confianca >= 0.80
GROUP BY t.transformador_codigo, t.transformador_nome
ORDER BY area_total_m2 DESC;
```

---

### 2. Top 10 subestações com maior potencial solar

```sql
SELECT 
    subestacao_codigo,
    subestacao_nome,
    total_telhados,
    ROUND(area_total_m2, 2) as area_m2,
    ROUND(confianca_media, 3) as confianca_media,
    ultima_deteccao
FROM vw_telhados_por_subestacao
WHERE total_telhados > 0
ORDER BY area_total_m2 DESC
LIMIT 10;
```

---

### 3. Distribuição de detecções por período

```sql
SELECT 
    DATE(t.timestamp_deteccao) as data_deteccao,
    tr.distribuidora,
    COUNT(*) as total_novo,
    ROUND(SUM(t.area_m2), 2) as area_m2,
    ROUND(AVG(t.confianca), 3) as confianca_media
FROM telhados_detectados_transformador t
JOIN transformadores_aneel tr ON t.transformador_id = tr.id
WHERE t.timestamp_deteccao >= NOW() - INTERVAL '30 days'
GROUP BY DATE(t.timestamp_deteccao), tr.distribuidora
ORDER BY data_deteccao DESC;
```

---

### 4. Transformadores com telhados de confiança heterogênea

```sql
SELECT 
    t.transformador_codigo,
    COUNT(*) as total_telhados,
    ROUND(MIN(t.confianca), 3) as conf_minima,
    ROUND(AVG(t.confianca), 3) as conf_media,
    ROUND(MAX(t.confianca), 3) as conf_maxima,
    ROUND(MAX(t.confianca) - MIN(t.confianca), 3) as variacao
FROM vw_telhados_completo t
GROUP BY t.transformador_codigo
HAVING COUNT(*) >= 2
ORDER BY variacao DESC;
```

---

## 📐 Especificações de Dados

### Coordenadas Geográficas

- **SRID**: 4326 (WGS84 - padrão GPS)
- **Latitude**: -90° a +90° (positivo = Norte, negativo = Sul)
- **Longitude**: -180° a +180° (negativo = Oeste, positivo = Leste)

**Exemplos Brasil:**
- São Paulo: (-23.5505, -46.6333)
- Rio de Janeiro: (-22.9068, -43.1729)
- Recife: (-8.0476, -34.8770)
- Curitiba: (-25.4196, -49.2646)

---

### Resolução de Imagens

| Fonte | Resolução | Uso |
|-------|-----------|-----|
| Google Maps | 30 cm | Detecção geral |
| Sentinel-2 | 10 m | Cobertura regional |
| Imagens altas res. | 5-10 cm | Validação de telhados |

**Campo:** `resolucao_cm` armazena em centímetros/pixel

---

### Confiança de Detecção

| Range | Interpretação | Recomendação |
|-------|---------------|--------------|
| 0.0 - 0.5 | Baixa | Rejeitar, muito ruído |
| 0.5 - 0.7 | Média | Revisar manualmente |
| 0.7 - 0.85 | Boa | Usar com cautela |
| 0.85 - 1.0 | Muito alta | Usar com confiança |

---

## 🚀 Como Usar

### Criar Schema (Primeira Vez)

```bash
psql -U usuario -d banco_energia -f schema.sql (unificado)
```

### Verificar Tabelas Criadas

```sql
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY tablename;
```

### Ver Estrutura de uma Tabela

```sql
\d transformadores_aneel
\d telhados_detectados_transformador
```

### Listar Views Disponíveis

```sql
SELECT viewname 
FROM pg_views 
WHERE schemaname = 'public' 
ORDER BY viewname;
```

### Testar Inserção

```sql
-- Inserir subestação
INSERT INTO subestacoes_aneel (codigo, nome, distribuidora, latitude, longitude)
VALUES ('SUB_TEST_001', 'Subestação Teste', 'COPEL', -25.4196, -49.2646);

-- Inserir transformador
INSERT INTO transformadores_aneel (codigo, nome, distribuidora, subestacao_codigo, 
                                   potencia_kva, tensao_primaria_kv, tensao_secundaria_kv, 
                                   latitude, longitude, tipo_tensao)
VALUES ('TRAFO_TEST_001', 'Trafo Teste', 'COPEL', 'SUB_TEST_001',
        100, 13.8, 0.22, -25.4196, -49.2646, 'BT');

-- Inserir telhado
INSERT INTO telhados_detectados_transformador 
(transformador_id, subestacao_id, latitude, longitude, area_m2, confianca, fonte_imagem)
SELECT tr.id, s.id, -25.4196, -49.2646, 150.5, 0.87, 'google_maps'
FROM transformadores_aneel tr
JOIN subestacoes_aneel s ON tr.subestacao_codigo = s.codigo
WHERE tr.codigo = 'TRAFO_TEST_001' AND s.codigo = 'SUB_TEST_001';

-- Verificar dados
SELECT * FROM vw_telhados_completo;
SELECT * FROM vw_telhados_estatisticas;
```

---

## 📖 Referência Rápida

### Inserir Dados

```sql
-- Subestação
INSERT INTO subestacoes_aneel (codigo, nome, distribuidora, tensao_kv, latitude, longitude)
VALUES (...);

-- Transformador
INSERT INTO transformadores_aneel (codigo, nome, distribuidora, potencia_kva, tensao_primaria_kv,
                                   tensao_secundaria_kv, latitude, longitude)
VALUES (...);

-- Consumidor BT
INSERT INTO consumidores_bt_aneel (codigo, distribuidora, subestacao_codigo, 
                                   carga_instalada_kw, latitude, longitude)
VALUES (...);

-- Telhado
INSERT INTO telhados_detectados_transformador (transformador_id, subestacao_id, 
                                               latitude, longitude, area_m2, confianca)
VALUES (...);
```

### Buscar Dados

```sql
-- Por código
SELECT * FROM transformadores_aneel WHERE codigo = 'TRAFO_001';

-- Por distribuidora
SELECT * FROM subestacoes_aneel WHERE distribuidora = 'COPEL';

-- Por localização (buffer de 5km)
SELECT * FROM transformadores_aneel
WHERE ST_Distance(localizacao, ST_Point(-25.4196, -49.2646, 4326)) < 5000;

-- Telhados por transformador
SELECT * FROM vw_telhados_completo 
WHERE transformador_codigo = 'TRAFO_001';

-- Estatísticas
SELECT * FROM vw_telhados_estatisticas;
```

### Atualizar Dados

```sql
-- Atualizar transformador
UPDATE transformadores_aneel 
SET ativo = FALSE
WHERE codigo = 'TRAFO_001';

-- Atualizar confiança de telhado
UPDATE telhados_detectados_transformador
SET confianca = 0.92
WHERE id = 123;
```

### Deletar Dados

```sql
-- Subestação (cascata deleta telhados associados)
DELETE FROM subestacoes_aneel WHERE codigo = 'SUB_001';

-- Telhado específico
DELETE FROM telhados_detectados_transformador WHERE id = 123;
```

---

## ✅ Checklist de Implementação

- [ ] PostgreSQL 12+ com PostGIS instalado
- [ ] Extensões postgis e postgis_raster criadas
- [ ] Schema executado (schema.sql (unificado))
- [ ] Tabelas criadas (verificar com `\d`)
- [ ] Views criadas (verificar com SELECT FROM vw_*)
- [ ] Índices criados (verificar performance)
- [ ] Permissões de usuário configuradas
- [ ] Backup agendado
- [ ] Monitoramento de tamanho de tabela ativado
- [ ] Testes de inserção/query realizados

---

**Documentação**: Schema ANEEL BDGD  
**Versão**: 2.1  
**Data**: 2026-02-04  
**Status**: ✅ Produção

