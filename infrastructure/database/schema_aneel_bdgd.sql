-- ============================================================================
-- SCHEMA SQL PARA TABELAS ANEEL BDGD
-- ============================================================================
-- Arquivo: schema_aneel_bdgd.sql
-- Descrição: Cria tabelas para armazenar dados do BDGD (Base de Dados Geográficos)
-- Data: 2026-02-02
-- ============================================================================

-- ============================================================================
-- EXTENSÕES NECESSÁRIAS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;


-- ============================================================================
-- ENUMS PARA VALIDAÇÃO DE DADOS
-- ============================================================================

-- Enum para tipos de tensão
CREATE TYPE enum_tipo_tensao AS ENUM ('BT', 'MT', 'AT', 'DESCONHECIDO');

-- Enum para métodos de cálculo de área
CREATE TYPE enum_metodo_calculo AS ENUM ('convex_hull', 'buffer_500m', 'buffer_1km', 'buffer_2km', 'buffer_5km');

-- Enum para status de processamento
CREATE TYPE enum_status_processamento AS ENUM ('em_processamento', 'concluido', 'erro', 'parcial');


-- ============================================================================
-- FUNÇÕES DE VALIDAÇÃO (Constraints Check)
-- ============================================================================

-- Validar que coordenadas estejam dentro dos limites válidos
CREATE OR REPLACE FUNCTION fn_validar_coordenadas(lat DECIMAL, lon DECIMAL)
RETURNS BOOLEAN AS $$
BEGIN
    IF lat IS NULL OR lon IS NULL THEN
        RETURN TRUE;  -- NULL é válido (opcional)
    END IF;
    
    IF lat < -90 OR lat > 90 THEN
        RAISE EXCEPTION 'Latitude fora de range: %', lat;
    END IF;
    
    IF lon < -180 OR lon > 180 THEN
        RAISE EXCEPTION 'Longitude fora de range: %', lon;
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Validar que potência seja positiva
CREATE OR REPLACE FUNCTION fn_validar_potencia(potencia DECIMAL)
RETURNS BOOLEAN AS $$
BEGIN
    IF potencia IS NULL THEN
        RETURN TRUE;
    END IF;
    
    IF potencia <= 0 THEN
        RAISE EXCEPTION 'Potência deve ser positiva: %', potencia;
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Validar que tensão seja positiva
CREATE OR REPLACE FUNCTION fn_validar_tensao(tensao DECIMAL)
RETURNS BOOLEAN AS $$
BEGIN
    IF tensao IS NULL THEN
        RETURN TRUE;
    END IF;
    
    IF tensao <= 0 THEN
        RAISE EXCEPTION 'Tensão deve ser positiva: %', tensao;
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- TABELAS ANEEL BDGD
-- ============================================================================

-- Tabela de TRANSFORMADORES
-- Armazena dados de transformadores de distribuição obtidos da camada UNTRD do BDGD
-- Cada transformador está associado a uma subestação via campo subestacao_codigo
-- Relacionamento com subestacoes_aneel.codigo (não-enforçado para permitir dados incompletos)
CREATE TABLE IF NOT EXISTS transformadores_aneel (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    nome VARCHAR(200),
    distribuidora VARCHAR(100) NOT NULL,
    subestacao_codigo VARCHAR(50),
    potencia_kva DECIMAL(10, 2),
    tensao_primaria_kv DECIMAL(10, 2),
    tensao_secundaria_kv DECIMAL(10, 2),
    tipo_tensao VARCHAR(10),  -- 'BT', 'MT' ou 'AT' (classificado automaticamente)
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'aneel_bdgd',
    ativo BOOLEAN DEFAULT TRUE,
    observacoes TEXT,
    
    -- CONSTRAINTS DE VALIDAÇÃO
    CONSTRAINT chk_transformadores_potencia_positiva 
        CHECK (potencia_kva IS NULL OR potencia_kva > 0),
    CONSTRAINT chk_transformadores_tensao_positiva_pri 
        CHECK (tensao_primaria_kv IS NULL OR tensao_primaria_kv > 0),
    CONSTRAINT chk_transformadores_tensao_positiva_sec 
        CHECK (tensao_secundaria_kv IS NULL OR tensao_secundaria_kv > 0),
    CONSTRAINT chk_transformadores_coordenadas 
        CHECK (fn_validar_coordenadas(latitude, longitude)),
    CONSTRAINT chk_transformadores_tipo_tensao 
        CHECK (tipo_tensao IS NULL OR tipo_tensao IN ('BT', 'MT', 'AT'))
);

-- Índices para TRANSFORMADORES
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_codigo 
    ON transformadores_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_tipo_tensao 
    ON transformadores_aneel(tipo_tensao);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_distribuidora 
    ON transformadores_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_subestacao 
    ON transformadores_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_potencia 
    ON transformadores_aneel(potencia_kva DESC);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_tensao_pri 
    ON transformadores_aneel(tensao_primaria_kv);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_localizacao 
    ON transformadores_aneel USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_coordenadas 
    ON transformadores_aneel(latitude, longitude) 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_data 
    ON transformadores_aneel(data_criacao DESC, data_atualizacao DESC);


-- Tabela de SUBESTAÇÕES
-- Armazena dados de subestações de distribuição obtidos da camada CTMT (Barramentos) do BDGD
-- Múltiplos barramentos são agrupados por código de subestação (campo SUB)
-- É o ponto central de conexão da hierarquia: distribuidora → subestação → transformadores → consumidores
CREATE TABLE IF NOT EXISTS subestacoes_aneel (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    codigo_bdgd VARCHAR(50),
    barramento_cod_id VARCHAR(100),
    nome VARCHAR(200),
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(10),
    descricao TEXT,
    tensao_kv DECIMAL(10, 2),
    tensao_operacao_kv DECIMAL(10, 2),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'aneel_bdgd',
    fonte_camada VARCHAR(100) DEFAULT 'CTMT (Barramentos)',
    codigo_ons VARCHAR(100),
    ativo BOOLEAN DEFAULT TRUE,
    observacoes TEXT,
    
    -- CONSTRAINTS DE VALIDAÇÃO
    CONSTRAINT chk_subestacoes_tensao_positiva 
        CHECK (tensao_kv IS NULL OR tensao_kv > 0),
    CONSTRAINT chk_subestacoes_tensao_op_positiva 
        CHECK (tensao_operacao_kv IS NULL OR tensao_operacao_kv > 0),
    CONSTRAINT chk_subestacoes_coordenadas 
        CHECK (fn_validar_coordenadas(latitude, longitude))
);

-- Índices para SUBESTAÇÕES
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_codigo 
    ON subestacoes_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_distribuidora 
    ON subestacoes_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_tensao 
    ON subestacoes_aneel(tensao_kv);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_localizacao 
    ON subestacoes_aneel USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_codigo_ons 
    ON subestacoes_aneel(codigo_ons) WHERE codigo_ons IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_fonte 
    ON subestacoes_aneel(fonte_dados);


-- ============================================================================
-- TABELAS DE CONSUMIDORES (Unidades Consumidoras)
-- Separadas por nível de tensão: Baixa Tensão (BT), Média Tensão (MT), Alta Tensão (AT)
-- Dados obtidos das camadas UCBT, UCMT, UCAT do BDGD conforme especificação v2.1
-- ============================================================================

-- Tabela de CONSUMIDORES de BAIXA TENSÃO (UCBT)
-- Entidade: Unidade Consumidora de Baixa Tensão
-- Camada: UCBT (Ponto)
CREATE TABLE IF NOT EXISTS consumidores_bt_aneel (
    id BIGSERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(10),
    subestacao_codigo VARCHAR(50),
    circuito_mt_codigo VARCHAR(50),  -- CTMT
    transformador_mt_codigo VARCHAR(50),  -- UNI_TR_MT
    transformador_at_codigo VARCHAR(50),  -- UNI_TR_AT
    ramal_codigo VARCHAR(50),  -- RAMAL
    ponto_notavel_codigo VARCHAR(50),  -- PN_CON
    pac_codigo VARCHAR(50),
    conjunto_codigo VARCHAR(50),  -- CONJ
    municipio_codigo VARCHAR(10),  -- MUN (IBGE)
    geracao_distribuida_codigo VARCHAR(50),  -- CODGD
    logradouro VARCHAR(254),  -- LGRD
    bairro VARCHAR(254),  -- BRR
    cep VARCHAR(8),  -- CEP
    classe_subclasse_codigo VARCHAR(50),  -- CLAS_SUB
    cnae_codigo VARCHAR(20),
    curva_carga_codigo VARCHAR(50),  -- TIP_CC
    fases_conexao_codigo VARCHAR(50),  -- FAS_CON
    grupo_tensao_codigo VARCHAR(50),  -- GRU_TEN
    tensao_fornecimento_codigo VARCHAR(50),  -- TEN_FORN (código DDA)
    grupo_tarifario_codigo VARCHAR(50),  -- GRU_TAR
    situacao_ativacao_codigo VARCHAR(50),  -- SIT_ATIV
    data_conexao DATE,  -- DAT_CON
    carga_instalada_kw DECIMAL(12, 2),  -- CAR_INST
    consumidor_livre BOOLEAN,  -- LIV
    area_localizacao_codigo VARCHAR(50),  -- ARE_LOC
    -- Energia ativa medida 12 períodos (kWh)
    energia_01 DECIMAL(15, 2), energia_02 DECIMAL(15, 2), energia_03 DECIMAL(15, 2),
    energia_04 DECIMAL(15, 2), energia_05 DECIMAL(15, 2), energia_06 DECIMAL(15, 2),
    energia_07 DECIMAL(15, 2), energia_08 DECIMAL(15, 2), energia_09 DECIMAL(15, 2),
    energia_10 DECIMAL(15, 2), energia_11 DECIMAL(15, 2), energia_12 DECIMAL(15, 2),
    -- DIC (Duração de Interrupção por Consumidor) 12 períodos (horas)
    dic_01 DECIMAL(10, 2), dic_02 DECIMAL(10, 2), dic_03 DECIMAL(10, 2),
    dic_04 DECIMAL(10, 2), dic_05 DECIMAL(10, 2), dic_06 DECIMAL(10, 2),
    dic_07 DECIMAL(10, 2), dic_08 DECIMAL(10, 2), dic_09 DECIMAL(10, 2),
    dic_10 DECIMAL(10, 2), dic_11 DECIMAL(10, 2), dic_12 DECIMAL(10, 2),
    -- FIC (Frequência de Interrupção por Consumidor) 12 períodos (horas)
    fic_01 DECIMAL(10, 2), fic_02 DECIMAL(10, 2), fic_03 DECIMAL(10, 2),
    fic_04 DECIMAL(10, 2), fic_05 DECIMAL(10, 2), fic_06 DECIMAL(10, 2),
    fic_07 DECIMAL(10, 2), fic_08 DECIMAL(10, 2), fic_09 DECIMAL(10, 2),
    fic_10 DECIMAL(10, 2), fic_11 DECIMAL(10, 2), fic_12 DECIMAL(10, 2),
    sem_rede BOOLEAN,  -- SEMRED (1=sem rede BT, 0=com rede BT)
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    descricao VARCHAR(254),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'aneel_bdgd',
    ativo BOOLEAN DEFAULT TRUE
);

-- Índices para CONSUMIDORES BT
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_codigo ON consumidores_bt_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_distribuidora ON consumidores_bt_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_subestacao ON consumidores_bt_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_transformador_mt ON consumidores_bt_aneel(transformador_mt_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_localizacao ON consumidores_bt_aneel USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_carga ON consumidores_bt_aneel(carga_instalada_kw DESC);


-- Tabela de CONSUMIDORES de MÉDIA TENSÃO (UCMT)
-- Entidade: Unidade Consumidora de Média Tensão
-- Camada: UCMT (Ponto)
CREATE TABLE IF NOT EXISTS consumidores_mt_aneel (
    id BIGSERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(10),
    subestacao_codigo VARCHAR(50),
    circuito_mt_codigo VARCHAR(50),  -- CTMT
    transformador_at_codigo VARCHAR(50),  -- UNI_TR_AT
    ponto_notavel_codigo VARCHAR(50),  -- PN_CON
    pac_codigo VARCHAR(50),
    conjunto_codigo VARCHAR(50),  -- CONJ
    municipio_codigo VARCHAR(10),  -- MUN (IBGE)
    geracao_distribuida_codigo VARCHAR(50),  -- CODGD
    logradouro VARCHAR(254),  -- LGRD
    bairro VARCHAR(254),  -- BRR
    cep VARCHAR(8),  -- CEP
    classe_subclasse_codigo VARCHAR(50),  -- CLAS_SUB
    cnae_codigo VARCHAR(20),
    curva_carga_codigo VARCHAR(50),  -- TIP_CC
    fases_conexao_codigo VARCHAR(50),  -- FAS_CON
    grupo_tensao_codigo VARCHAR(50),  -- GRU_TEN
    tensao_fornecimento_codigo VARCHAR(50),  -- TEN_FORN (código DDA)
    grupo_tarifario_codigo VARCHAR(50),  -- GRU_TAR
    situacao_ativacao_codigo VARCHAR(50),  -- SIT_ATIV
    data_conexao DATE,  -- DAT_CON
    carga_instalada_kw DECIMAL(12, 2),  -- CAR_INST
    demanda_contratada_kw DECIMAL(12, 2),  -- DEM_CONT
    consumidor_livre BOOLEAN,  -- LIV
    area_localizacao_codigo VARCHAR(50),  -- ARE_LOC
    -- Demanda ativa máxima medida 12 períodos (kW)
    demanda_01 DECIMAL(12, 2), demanda_02 DECIMAL(12, 2), demanda_03 DECIMAL(12, 2),
    demanda_04 DECIMAL(12, 2), demanda_05 DECIMAL(12, 2), demanda_06 DECIMAL(12, 2),
    demanda_07 DECIMAL(12, 2), demanda_08 DECIMAL(12, 2), demanda_09 DECIMAL(12, 2),
    demanda_10 DECIMAL(12, 2), demanda_11 DECIMAL(12, 2), demanda_12 DECIMAL(12, 2),
    -- Energia ativa medida 12 períodos (kWh)
    energia_01 DECIMAL(15, 2), energia_02 DECIMAL(15, 2), energia_03 DECIMAL(15, 2),
    energia_04 DECIMAL(15, 2), energia_05 DECIMAL(15, 2), energia_06 DECIMAL(15, 2),
    energia_07 DECIMAL(15, 2), energia_08 DECIMAL(15, 2), energia_09 DECIMAL(15, 2),
    energia_10 DECIMAL(15, 2), energia_11 DECIMAL(15, 2), energia_12 DECIMAL(15, 2),
    -- DIC (Duração de Interrupção por Consumidor) 12 períodos (horas)
    dic_01 DECIMAL(10, 2), dic_02 DECIMAL(10, 2), dic_03 DECIMAL(10, 2),
    dic_04 DECIMAL(10, 2), dic_05 DECIMAL(10, 2), dic_06 DECIMAL(10, 2),
    dic_07 DECIMAL(10, 2), dic_08 DECIMAL(10, 2), dic_09 DECIMAL(10, 2),
    dic_10 DECIMAL(10, 2), dic_11 DECIMAL(10, 2), dic_12 DECIMAL(10, 2),
    -- FIC (Frequência de Interrupção por Consumidor) 12 períodos (horas)
    fic_01 DECIMAL(10, 2), fic_02 DECIMAL(10, 2), fic_03 DECIMAL(10, 2),
    fic_04 DECIMAL(10, 2), fic_05 DECIMAL(10, 2), fic_06 DECIMAL(10, 2),
    fic_07 DECIMAL(10, 2), fic_08 DECIMAL(10, 2), fic_09 DECIMAL(10, 2),
    fic_10 DECIMAL(10, 2), fic_11 DECIMAL(10, 2), fic_12 DECIMAL(10, 2),
    sem_rede BOOLEAN,  -- SEMRED (1=sem rede MT, 0=com rede MT)
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    descricao VARCHAR(254),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'aneel_bdgd',
    ativo BOOLEAN DEFAULT TRUE
);

-- Índices para CONSUMIDORES MT
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_codigo ON consumidores_mt_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_distribuidora ON consumidores_mt_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_subestacao ON consumidores_mt_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_circuito_mt ON consumidores_mt_aneel(circuito_mt_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_localizacao ON consumidores_mt_aneel USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_demanda ON consumidores_mt_aneel(demanda_contratada_kw DESC);


-- Tabela de CONSUMIDORES de ALTA TENSÃO (UCAT)
-- Entidade: Unidade Consumidora de Alta Tensão
-- Camada: UCAT (Ponto)
CREATE TABLE IF NOT EXISTS consumidores_at_aneel (
    id BIGSERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(10),
    subestacao_codigo VARCHAR(50),
    circuito_at_codigo VARCHAR(50),  -- CTAT
    ponto_notavel_codigo VARCHAR(50),  -- PN_CON
    pac_codigo VARCHAR(50),
    conjunto_codigo VARCHAR(50),  -- CONJ
    municipio_codigo VARCHAR(10),  -- MUN (IBGE)
    geracao_distribuida_codigo VARCHAR(50),  -- CODGD
    logradouro VARCHAR(254),  -- LGRD
    bairro VARCHAR(254),  -- BRR
    cep VARCHAR(8),  -- CEP
    classe_subclasse_codigo VARCHAR(50),  -- CLAS_SUB
    cnae_codigo VARCHAR(20),
    curva_carga_codigo VARCHAR(50),  -- TIP_CC
    fases_conexao_codigo VARCHAR(50),  -- FAS_CON
    grupo_tensao_codigo VARCHAR(50),  -- GRU_TEN
    tensao_fornecimento_codigo VARCHAR(50),  -- TEN_FORN (código DDA)
    grupo_tarifario_codigo VARCHAR(50),  -- GRU_TAR
    situacao_ativacao_codigo VARCHAR(50),  -- SIT_ATIV
    data_conexao DATE,  -- DAT_CON
    carga_instalada_kw DECIMAL(12, 2),  -- CAR_INST
    demanda_contratada_kw DECIMAL(12, 2),  -- DEM_CONT
    consumidor_livre BOOLEAN,  -- LIV
    area_localizacao_codigo VARCHAR(50),  -- ARE_LOC
    -- Demanda ativa máxima PONTA 12 períodos (kW)
    demanda_ponta_01 DECIMAL(12, 2), demanda_ponta_02 DECIMAL(12, 2), demanda_ponta_03 DECIMAL(12, 2),
    demanda_ponta_04 DECIMAL(12, 2), demanda_ponta_05 DECIMAL(12, 2), demanda_ponta_06 DECIMAL(12, 2),
    demanda_ponta_07 DECIMAL(12, 2), demanda_ponta_08 DECIMAL(12, 2), demanda_ponta_09 DECIMAL(12, 2),
    demanda_ponta_10 DECIMAL(12, 2), demanda_ponta_11 DECIMAL(12, 2), demanda_ponta_12 DECIMAL(12, 2),
    -- Demanda ativa máxima FORA PONTA 12 períodos (kW)
    demanda_fora_ponta_01 DECIMAL(12, 2), demanda_fora_ponta_02 DECIMAL(12, 2), demanda_fora_ponta_03 DECIMAL(12, 2),
    demanda_fora_ponta_04 DECIMAL(12, 2), demanda_fora_ponta_05 DECIMAL(12, 2), demanda_fora_ponta_06 DECIMAL(12, 2),
    demanda_fora_ponta_07 DECIMAL(12, 2), demanda_fora_ponta_08 DECIMAL(12, 2), demanda_fora_ponta_09 DECIMAL(12, 2),
    demanda_fora_ponta_10 DECIMAL(12, 2), demanda_fora_ponta_11 DECIMAL(12, 2), demanda_fora_ponta_12 DECIMAL(12, 2),
    -- Energia ativa PONTA 12 períodos (kWh)
    energia_ponta_01 DECIMAL(15, 2), energia_ponta_02 DECIMAL(15, 2), energia_ponta_03 DECIMAL(15, 2),
    energia_ponta_04 DECIMAL(15, 2), energia_ponta_05 DECIMAL(15, 2), energia_ponta_06 DECIMAL(15, 2),
    energia_ponta_07 DECIMAL(15, 2), energia_ponta_08 DECIMAL(15, 2), energia_ponta_09 DECIMAL(15, 2),
    energia_ponta_10 DECIMAL(15, 2), energia_ponta_11 DECIMAL(15, 2), energia_ponta_12 DECIMAL(15, 2),
    -- Energia ativa FORA PONTA 12 períodos (kWh)
    energia_fora_ponta_01 DECIMAL(15, 2), energia_fora_ponta_02 DECIMAL(15, 2), energia_fora_ponta_03 DECIMAL(15, 2),
    energia_fora_ponta_04 DECIMAL(15, 2), energia_fora_ponta_05 DECIMAL(15, 2), energia_fora_ponta_06 DECIMAL(15, 2),
    energia_fora_ponta_07 DECIMAL(15, 2), energia_fora_ponta_08 DECIMAL(15, 2), energia_fora_ponta_09 DECIMAL(15, 2),
    energia_fora_ponta_10 DECIMAL(15, 2), energia_fora_ponta_11 DECIMAL(15, 2), energia_fora_ponta_12 DECIMAL(15, 2),
    -- DIC (Duração de Interrupção por Consumidor) 12 períodos (horas)
    dic_01 DECIMAL(10, 2), dic_02 DECIMAL(10, 2), dic_03 DECIMAL(10, 2),
    dic_04 DECIMAL(10, 2), dic_05 DECIMAL(10, 2), dic_06 DECIMAL(10, 2),
    dic_07 DECIMAL(10, 2), dic_08 DECIMAL(10, 2), dic_09 DECIMAL(10, 2),
    dic_10 DECIMAL(10, 2), dic_11 DECIMAL(10, 2), dic_12 DECIMAL(10, 2),
    -- FIC (Frequência de Interrupção por Consumidor) 12 períodos (horas)
    fic_01 DECIMAL(10, 2), fic_02 DECIMAL(10, 2), fic_03 DECIMAL(10, 2),
    fic_04 DECIMAL(10, 2), fic_05 DECIMAL(10, 2), fic_06 DECIMAL(10, 2),
    fic_07 DECIMAL(10, 2), fic_08 DECIMAL(10, 2), fic_09 DECIMAL(10, 2),
    fic_10 DECIMAL(10, 2), fic_11 DECIMAL(10, 2), fic_12 DECIMAL(10, 2),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    descricao VARCHAR(254),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'aneel_bdgd',
    ativo BOOLEAN DEFAULT TRUE
);

-- Índices para CONSUMIDORES AT
CREATE INDEX IF NOT EXISTS idx_consumidores_at_codigo ON consumidores_at_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_distribuidora ON consumidores_at_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_subestacao ON consumidores_at_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_circuito_at ON consumidores_at_aneel(circuito_at_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_localizacao ON consumidores_at_aneel USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_demanda_ponta ON consumidores_at_aneel(demanda_contratada_kw DESC);


-- ============================================================================
-- TABELA DE DIMENSÃO: DISTRIBUIDORAS
-- ============================================================================

-- Tabela de DISTRIBUIDORAS ANEEL
-- Dimensão com informações sobre cada distribuidora
CREATE TABLE IF NOT EXISTS distribuidoras_aneel (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE NOT NULL,
    codigo_arquivo VARCHAR(200),
    estado VARCHAR(2),
    regiao VARCHAR(50),
    data_carregamento TIMESTAMP DEFAULT NOW(),
    total_transformadores INT DEFAULT 0,
    total_subestacoes INT DEFAULT 0,
    total_consumidores INT DEFAULT 0,
    potencia_total_kva DECIMAL(15, 2) DEFAULT 0,
    ativo BOOLEAN DEFAULT TRUE,
    observacoes TEXT
);

-- Índices para DISTRIBUIDORAS
CREATE INDEX IF NOT EXISTS idx_distribuidoras_aneel_nome 
    ON distribuidoras_aneel(nome);
CREATE INDEX IF NOT EXISTS idx_distribuidoras_aneel_codigo 
    ON distribuidoras_aneel(codigo_arquivo);
CREATE INDEX IF NOT EXISTS idx_distribuidoras_aneel_estado 
    ON distribuidoras_aneel(estado);

-- ============================================================================
-- TABELAS DE AUDITORIA E PROCESSAMENTO
-- ============================================================================

-- Tabela de LOG de PROCESSAMENTO
-- Rastreia execuções do ETL ANEEL BDGD
CREATE TABLE IF NOT EXISTS aneel_bdgd_processamento (
    id BIGSERIAL PRIMARY KEY,
    data_inicio TIMESTAMP DEFAULT NOW(),
    data_fim TIMESTAMP,
    tempo_total_segundos FLOAT,
    distribuidora_processada VARCHAR(100),
    transformadores_inseridos INTEGER DEFAULT 0,
    transformadores_atualizados INTEGER DEFAULT 0,
    subestacoes_inseridas INTEGER DEFAULT 0,
    subestacoes_atualizadas INTEGER DEFAULT 0,
    consumidores_inseridos INTEGER DEFAULT 0,
    consumidores_atualizados INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'em_processamento',
    mensagem_erro TEXT,
    parametros_execucao JSONB DEFAULT '{}',
    CONSTRAINT chk_status CHECK (status IN ('em_processamento', 'concluido', 'erro'))
);

-- Índices para LOG
CREATE INDEX IF NOT EXISTS idx_aneel_processamento_data 
    ON aneel_bdgd_processamento(data_inicio DESC);
CREATE INDEX IF NOT EXISTS idx_aneel_processamento_distribuidora 
    ON aneel_bdgd_processamento(distribuidora_processada);
CREATE INDEX IF NOT EXISTS idx_aneel_processamento_status 
    ON aneel_bdgd_processamento(status);


-- ============================================================================
-- VIEWS ÚTEIS
-- ============================================================================

-- VIEW: Resumo de Cobertura por Distribuidora
CREATE OR REPLACE VIEW v_aneel_cobertura_resumo AS
SELECT 
    'transformadores' as tipo,
    distribuidora,
    COUNT(*) as total,
    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as com_coordenadas,
    COUNT(CASE WHEN potencia_kva IS NOT NULL THEN 1 END) as com_dados_tecnicos,
    ROUND(AVG(CAST(potencia_kva AS DECIMAL))::NUMERIC, 2) as potencia_media_kva,
    ROUND((COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) as cobertura_geografica_pct
FROM transformadores_aneel
GROUP BY distribuidora

UNION ALL

SELECT 
    'subestacoes' as tipo,
    distribuidora,
    COUNT(*) as total,
    COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as com_coordenadas,
    COUNT(CASE WHEN tensao_kv IS NOT NULL THEN 1 END) as com_dados_tecnicos,
    ROUND(AVG(CAST(tensao_kv AS DECIMAL))::NUMERIC, 2) as potencia_media_kva,
    ROUND((COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) as cobertura_geografica_pct
FROM subestacoes_aneel
GROUP BY distribuidora

ORDER BY distribuidora, tipo;


-- VIEW: Distribuição de Tensões (Transformadores)
CREATE OR REPLACE VIEW v_aneel_transformadores_por_tensao AS
SELECT 
    distribuidora,
    tensao_primaria_kv,
    COUNT(*) as quantidade,
    ROUND(AVG(CAST(potencia_kva AS DECIMAL))::NUMERIC, 2) as potencia_media_kva,
    ROUND(SUM(CAST(potencia_kva AS DECIMAL))::NUMERIC, 2) as potencia_total_kva
FROM transformadores_aneel
WHERE ativo = TRUE
GROUP BY distribuidora, tensao_primaria_kv
ORDER BY distribuidora, tensao_primaria_kv DESC;


-- VIEW: Subestações com Transformadores Associados
CREATE OR REPLACE VIEW v_aneel_subestacoes_com_transformadores AS
SELECT 
    s.id,
    s.codigo,
    s.nome,
    s.distribuidora,
    s.tensao_kv,
    s.latitude,
    s.longitude,
    COUNT(t.id) as quantidade_transformadores,
    ROUND(SUM(CAST(t.potencia_kva AS DECIMAL))::NUMERIC, 2) as potencia_total_kva
FROM subestacoes_aneel s
LEFT JOIN transformadores_aneel t ON 
    ST_DWithin(s.localizacao, t.localizacao, 0.01) AND
    s.distribuidora = t.distribuidora
WHERE s.ativo = TRUE
GROUP BY s.id, s.codigo, s.nome, s.distribuidora, s.tensao_kv, s.latitude, s.longitude
ORDER BY s.distribuidora, quantidade_transformadores DESC;


-- ============================================================================
-- FUNÇÃO: Sincronizar dados ANEEL BDGD com tabelas de relacionamento
-- ============================================================================

-- Atualizar geometrias se lat/lon forem alteradas
CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_transformadores()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.localizacao := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.data_atualizacao := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aneel_transformadores_geometria
    BEFORE INSERT OR UPDATE ON transformadores_aneel
    FOR EACH ROW
    EXECUTE FUNCTION aneel_atualizar_geometria_transformadores();


CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_subestacoes()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.localizacao := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.data_atualizacao := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aneel_subestacoes_geometria
    BEFORE INSERT OR UPDATE ON subestacoes_aneel
    FOR EACH ROW
    EXECUTE FUNCTION aneel_atualizar_geometria_subestacoes();


-- ============================================================================
-- TRIGGERS PARA ATUALIZAÇÃO AUTOMÁTICA DE GEOMETRIA
-- ============================================================================

CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_consumidores_bt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.localizacao := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.data_atualizacao := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aneel_consumidores_bt_geometria
    BEFORE INSERT OR UPDATE ON consumidores_bt_aneel
    FOR EACH ROW
    EXECUTE FUNCTION aneel_atualizar_geometria_consumidores_bt();

CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_consumidores_mt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.localizacao := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.data_atualizacao := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aneel_consumidores_mt_geometria
    BEFORE INSERT OR UPDATE ON consumidores_mt_aneel
    FOR EACH ROW
    EXECUTE FUNCTION aneel_atualizar_geometria_consumidores_mt();

CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_consumidores_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.localizacao := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.data_atualizacao := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_aneel_consumidores_at_geometria
    BEFORE INSERT OR UPDATE ON consumidores_at_aneel
    FOR EACH ROW
    EXECUTE FUNCTION aneel_atualizar_geometria_consumidores_at();


-- ============================================================================
-- QUERIES ÚTEIS (Comentadas)
-- ============================================================================

/*
-- Total de dados carregados
SELECT 
    'transformadores' as tipo,
    COUNT(*) as total
FROM transformadores_aneel
UNION ALL
SELECT 
    'subestacoes',
    COUNT(*)
FROM subestacoes_aneel;

-- Dados com gaps geográficos
SELECT 
    distribuidora,
    COUNT(*) as sem_coordenadas
FROM transformadores_aneel
WHERE latitude IS NULL OR longitude IS NULL
GROUP BY distribuidora
HAVING COUNT(*) > 0;

-- Transformadores por classe de potência
SELECT 
    distribuidora,
    CASE 
        WHEN potencia_kva < 50 THEN '<50kVA'
        WHEN potencia_kva < 100 THEN '50-100kVA'
        WHEN potencia_kva < 300 THEN '100-300kVA'
        ELSE '>300kVA'
    END as classe_potencia,
    COUNT(*) as quantidade,
    ROUND(AVG(potencia_kva)::NUMERIC, 2) as media
FROM transformadores_aneel
GROUP BY distribuidora, classe_potencia
ORDER BY distribuidora, classe_potencia;

-- Detectar outliers (transformadores muito grandes/pequenos)
SELECT 
    codigo,
    nome,
    distribuidora,
    potencia_kva
FROM transformadores_aneel
WHERE potencia_kva > (SELECT AVG(potencia_kva) + 3 * STDDEV(potencia_kva) FROM transformadores_aneel)
   OR potencia_kva < (SELECT AVG(potencia_kva) - 3 * STDDEV(potencia_kva) FROM transformadores_aneel)
ORDER BY potencia_kva DESC;

-- Densidade de transformadores por subestação (áreas)
SELECT 
    s.codigo,
    s.nome,
    COUNT(t.id) as transformadores,
    (COUNT(t.id)::NUMERIC / COUNT(*) OVER (PARTITION BY s.distribuidora))::DECIMAL as percentual
FROM subestacoes_aneel s
LEFT JOIN transformadores_aneel t ON 
    ST_DWithin(s.localizacao, t.localizacao, 0.01)
WHERE s.ativo = TRUE
GROUP BY s.id, s.codigo, s.nome, s.distribuidora
ORDER BY transformadores DESC;
*/


-- ============================================================================
-- TABELA DE ÁREAS POLIGONAIS DOS TRANSFORMADORES
-- ============================================================================
-- Armazena área de cobertura (ConvexHull ou Buffer) para cada transformador
-- SEM REPETIÇÃO: Um transformador = Uma área = Um polígono
-- Calculada a partir dos consumidores conectados (UCBT, UCMT, UCAT)

CREATE TABLE IF NOT EXISTS transformador_area_cobertura (
    id SERIAL PRIMARY KEY,
    transformador_codigo VARCHAR(50) NOT NULL UNIQUE,  -- Chave única: sem repetição
    tipo_tensao VARCHAR(10) NOT NULL,  -- 'BT', 'MT', 'AT'
    distribuidora VARCHAR(100) NOT NULL,
    metodo_calculo VARCHAR(50) NOT NULL,  -- 'convex_hull' ou 'buffer_500m'/'buffer_1km'/'buffer_5km'
    geom GEOMETRY(Polygon, 4326),  -- Polígono da área
    area_m2 DECIMAL(15, 2),  -- Área em metros quadrados
    area_km2 DECIMAL(12, 4),  -- Área em quilômetros quadrados
    num_consumidores INT DEFAULT 0,  -- Quantos consumidores formam essa área
    num_vertices INT,  -- Número de vértices do polígono
    data_calculo TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    ativo BOOLEAN DEFAULT TRUE,
    observacoes TEXT,
    
    -- CONSTRAINTS DE VALIDAÇÃO
    CONSTRAINT chk_area_cobertura_tipo_tensao 
        CHECK (tipo_tensao IN ('BT', 'MT', 'AT')),
    CONSTRAINT chk_area_cobertura_metodo 
        CHECK (metodo_calculo IN ('convex_hull', 'buffer_500m', 'buffer_1km', 'buffer_2km', 'buffer_5km')),
    CONSTRAINT chk_area_cobertura_area_positiva 
        CHECK (area_m2 IS NULL OR area_m2 > 0),
    CONSTRAINT chk_area_cobertura_consumidores_positivos 
        CHECK (num_consumidores >= 0),
    CONSTRAINT chk_area_cobertura_vertices_positivos 
        CHECK (num_vertices IS NULL OR num_vertices > 0),
    CONSTRAINT fk_area_transformador 
        FOREIGN KEY (transformador_codigo) REFERENCES transformadores_aneel(codigo) ON DELETE CASCADE
);

-- Índices para TRANSFORMADOR_AREA_COBERTURA
CREATE INDEX IF NOT EXISTS idx_transformador_area_codigo 
    ON transformador_area_cobertura(transformador_codigo);
CREATE INDEX IF NOT EXISTS idx_transformador_area_tipo_tensao 
    ON transformador_area_cobertura(tipo_tensao);
CREATE INDEX IF NOT EXISTS idx_transformador_area_distribuidora 
    ON transformador_area_cobertura(distribuidora);
CREATE INDEX IF NOT EXISTS idx_transformador_area_metodo 
    ON transformador_area_cobertura(metodo_calculo);
CREATE INDEX IF NOT EXISTS idx_transformador_area_geom 
    ON transformador_area_cobertura USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_transformador_area_consumidores 
    ON transformador_area_cobertura(num_consumidores DESC);


-- ============================================================================
-- PROCEDURES SQL PARA PROCESSAMENTO E CÁLCULO DE ÁREAS
-- ============================================================================
-- Estas procedures consolidam a lógica de processamento que antes estava nos serviços Python
-- Objetivo: "Single Source of Truth" - SQL no schema, Python apenas executa

-- PROCEDURE: Calcular Áreas de Cobertura dos Transformadores (ConvexHull + Buffer)
-- Estratégia:
--   - ≥3 consumidores → ST_ConvexHull (polígono real dos consumidores)
--   - <3 consumidores → ST_Buffer (raio adaptado: BT=500m, MT=1km, AT=2km)
-- ============================================================================

CREATE OR REPLACE FUNCTION sp_calcular_area_transformadores(
    p_tipo_tensao VARCHAR DEFAULT NULL,  -- NULL = calcular todos (BT, MT, AT)
    p_distribuidora VARCHAR DEFAULT NULL,  -- NULL = calcular todas
    p_verbose BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(
    mensagem TEXT,
    tipo_tensao VARCHAR,
    transformadores_processados INT,
    areas_criadas INT,
    areas_atualizadas INT,
    tempo_ms INT
) AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_tipo VARCHAR;
    v_tabela_consumidores VARCHAR;
    v_campo_ref VARCHAR;
    v_raio_buffer INT;
    v_count_criadas INT := 0;
    v_count_atualizadas INT := 0;
    v_count_total INT := 0;
BEGIN
    v_start_time := CLOCK_TIMESTAMP();
    
    -- Determinar quais tipos de tensão processar
    FOR v_tipo IN 
        SELECT UNNEST(CASE WHEN p_tipo_tensao IS NOT NULL THEN ARRAY[p_tipo_tensao] 
                          ELSE ARRAY['BT', 'MT', 'AT'] END)
    LOOP
        -- Mapear tipo de tensão para tabela e raio de buffer
        CASE v_tipo
            WHEN 'BT' THEN
                v_tabela_consumidores := 'consumidores_bt_aneel';
                v_campo_ref := 'subestacao_codigo';
                v_raio_buffer := 500;  -- 500 metros
            WHEN 'MT' THEN
                v_tabela_consumidores := 'consumidores_mt_aneel';
                v_campo_ref := 'circuito_mt_codigo';
                v_raio_buffer := 1000;  -- 1 km
            WHEN 'AT' THEN
                v_tabela_consumidores := 'consumidores_at_aneel';
                v_campo_ref := 'circuito_at_codigo';
                v_raio_buffer := 2000;  -- 2 km
        END CASE;
        
        -- Executar cálculo via SQL dinâmico
        EXECUTE 'WITH consumidor_grupos AS (
            SELECT 
                ' || v_campo_ref || ' as transformador_codigo,
                COUNT(*) as num_consumidores,
                ST_Collect(localizacao) as geom_coletada
            FROM ' || v_tabela_consumidores || '
            WHERE distribuidora = COALESCE($2, distribuidora)
              AND localizacao IS NOT NULL
            GROUP BY ' || v_campo_ref || '
        ),
        areas_calculadas AS (
            SELECT 
                transformador_codigo,
                num_consumidores,
                CASE 
                    WHEN num_consumidores >= 3 THEN 
                        ST_ConvexHull(geom_coletada)
                    ELSE 
                        ST_Buffer(
                            ST_Centroid(geom_coletada)::GEOGRAPHY,
                            $3
                        )::GEOMETRY
                END as geom,
                CASE 
                    WHEN num_consumidores >= 3 THEN ''convex_hull''
                    ELSE ''buffer_'' || $3 || ''m''
                END as metodo,
                ST_Area(
                    CASE 
                        WHEN num_consumidores >= 3 THEN 
                            ST_ConvexHull(geom_coletada)
                        ELSE 
                            ST_Buffer(
                                ST_Centroid(geom_coletada)::GEOGRAPHY,
                                $3
                            )::GEOMETRY
                    END ::GEOGRAPHY
                ) as area_m2
            FROM consumidor_grupos
        )
        INSERT INTO transformador_area_cobertura 
        (transformador_codigo, tipo_tensao, distribuidora, metodo_calculo, geom, area_m2, area_km2, num_consumidores, num_vertices, data_calculo)
        SELECT 
            transformador_codigo,
            $1,
            COALESCE($2, (SELECT distribuidora FROM ' || v_tabela_consumidores || ' WHERE ' || v_campo_ref || ' = transformador_codigo LIMIT 1)),
            metodo,
            geom,
            area_m2,
            area_m2 / 1000000,
            num_consumidores,
            ST_NPoints(geom),
            NOW()
        FROM areas_calculadas
        ON CONFLICT (transformador_codigo) DO UPDATE SET
            geom = EXCLUDED.geom,
            area_m2 = EXCLUDED.area_m2,
            area_km2 = EXCLUDED.area_km2,
            num_consumidores = EXCLUDED.num_consumidores,
            num_vertices = EXCLUDED.num_vertices,
            metodo_calculo = EXCLUDED.metodo_calculo,
            data_atualizacao = NOW()
        WHERE transformador_area_cobertura.tipo_tensao = $1'
        USING v_tipo, p_distribuidora, v_raio_buffer;
        
        -- Contar registros processados
        SELECT COUNT(*) INTO v_count_total 
        FROM transformador_area_cobertura 
        WHERE tipo_tensao = v_tipo
          AND (p_distribuidora IS NULL OR distribuidora = p_distribuidora);
        
        IF p_verbose THEN
            RAISE NOTICE '[%] Tensão % processada. Total: %', v_tipo, v_tipo, v_count_total;
        END IF;
        
    END LOOP;
    
    -- Retornar resultado
    RETURN QUERY SELECT 
        'Cálculo de áreas completado com sucesso'::TEXT as mensagem,
        p_tipo_tensao::VARCHAR,
        v_count_total,
        v_count_criadas,
        v_count_atualizadas,
        EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP() - v_start_time))::INT * 1000;

EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT 
        'ERRO: ' || SQLERRM,
        p_tipo_tensao::VARCHAR,
        0, 0, 0, 0;
END;
$$ LANGUAGE plpgsql;

-- Comentário de uso
COMMENT ON FUNCTION sp_calcular_area_transformadores IS 
'Calcula áreas de cobertura dos transformadores usando ConvexHull ou Buffer
Uso: SELECT * FROM sp_calcular_area_transformadores();
     SELECT * FROM sp_calcular_area_transformadores(''BT'', ''IENERGIA_87'');
     SELECT * FROM sp_calcular_area_transformadores(NULL, NULL, TRUE);';


-- ============================================================================
-- PROCEDURE: Atualizar Tabela de Distribuidoras com Estatísticas
-- ============================================================================

CREATE OR REPLACE FUNCTION sp_atualizar_distribuidoras()
RETURNS TABLE(
    distribuidora VARCHAR,
    total_transformadores INT,
    total_subestacoes INT,
    total_consumidores INT,
    potencia_total_kva DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO distribuidoras_aneel 
    (nome, total_transformadores, total_subestacoes, total_consumidores, potencia_total_kva, data_carregamento)
    SELECT 
        d.distribuidora,
        COUNT(DISTINCT t.id),
        COUNT(DISTINCT s.id),
        COUNT(DISTINCT COALESCE(c_bt.id, c_mt.id, c_at.id)),
        ROUND(COALESCE(SUM(t.potencia_kva), 0), 2)
    FROM (
        SELECT DISTINCT distribuidora FROM transformadores_aneel
        UNION
        SELECT DISTINCT distribuidora FROM subestacoes_aneel
        UNION
        SELECT DISTINCT distribuidora FROM consumidores_bt_aneel
        UNION
        SELECT DISTINCT distribuidora FROM consumidores_mt_aneel
        UNION
        SELECT DISTINCT distribuidora FROM consumidores_at_aneel
    ) d
    LEFT JOIN transformadores_aneel t ON d.distribuidora = t.distribuidora
    LEFT JOIN subestacoes_aneel s ON d.distribuidora = s.distribuidora
    LEFT JOIN consumidores_bt_aneel c_bt ON d.distribuidora = c_bt.distribuidora
    LEFT JOIN consumidores_mt_aneel c_mt ON d.distribuidora = c_mt.distribuidora
    LEFT JOIN consumidores_at_aneel c_at ON d.distribuidora = c_at.distribuidora
    GROUP BY d.distribuidora
    ON CONFLICT (nome) DO UPDATE SET
        total_transformadores = EXCLUDED.total_transformadores,
        total_subestacoes = EXCLUDED.total_subestacoes,
        total_consumidores = EXCLUDED.total_consumidores,
        potencia_total_kva = EXCLUDED.potencia_total_kva,
        data_carregamento = NOW();

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Erro ao atualizar distribuidoras: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sp_atualizar_distribuidoras IS 
'Atualiza tabela distribuidoras_aneel com estatísticas consolidadas
Uso: SELECT * FROM sp_atualizar_distribuidoras();';


-- ============================================================================
-- PROCEDURE: Validar Integridade dos Dados
-- ============================================================================

CREATE OR REPLACE FUNCTION sp_validar_integridade()
RETURNS TABLE(
    validacao TEXT,
    status VARCHAR,
    quantidade INT,
    detalhes TEXT
) AS $$
BEGIN
    -- Validação 1: Transformadores sem coordenadas
    RETURN QUERY SELECT 
        'Transformadores sem coordenadas'::TEXT,
        'AVISO'::VARCHAR,
        COUNT(*)::INT,
        'Transformadores que não possuem latitude/longitude'::TEXT
    FROM transformadores_aneel
    WHERE latitude IS NULL OR longitude IS NULL;
    
    -- Validação 2: Transformadores sem tipo de tensão classificado
    RETURN QUERY SELECT 
        'Transformadores sem tipo_tensao'::TEXT,
        'AVISO'::VARCHAR,
        COUNT(*)::INT,
        'Transformadores que não foram classificados como BT/MT/AT'::TEXT
    FROM transformadores_aneel
    WHERE tipo_tensao IS NULL;
    
    -- Validação 3: Consumidores orfãos (sem transformador associado)
    RETURN QUERY SELECT 
        'Consumidores BT sem transformador'::TEXT,
        'AVISO'::VARCHAR,
        COUNT(*)::INT,
        'Consumidores BT que não têm subestacao_codigo definida'::TEXT
    FROM consumidores_bt_aneel
    WHERE subestacao_codigo IS NULL;
    
    -- Validação 4: Distribuições sem dados
    RETURN QUERY SELECT 
        'Distribuidoras vazias'::TEXT,
        'CRITICA'::VARCHAR,
        COUNT(*)::INT,
        'Distribuidoras registradas mas sem transformadores associados'::TEXT
    FROM distribuidoras_aneel d
    WHERE NOT EXISTS (SELECT 1 FROM transformadores_aneel t WHERE t.distribuidora = d.nome);
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sp_validar_integridade IS 
'Valida integridade dos dados ANEEL BDGD
Uso: SELECT * FROM sp_validar_integridade();';


-- ============================================================================
-- PROCEDURE: Limpar Dados Obsoletos e Inválidos
-- ============================================================================

CREATE OR REPLACE FUNCTION sp_limpar_dados_obsoletos(
    p_dias_inatividade INT DEFAULT 90,
    p_dry_run BOOLEAN DEFAULT TRUE
)
RETURNS TABLE(
    operacao TEXT,
    tabela VARCHAR,
    registros_afetados INT,
    acao TEXT
) AS $$
DECLARE
    v_data_limite DATE;
    v_count INT;
BEGIN
    v_data_limite := CURRENT_DATE - p_dias_inatividade;
    
    -- Limpar transformadores inativos
    SELECT COUNT(*) INTO v_count 
    FROM transformadores_aneel 
    WHERE ativo = FALSE 
      AND data_atualizacao < v_data_limite;
    
    IF NOT p_dry_run AND v_count > 0 THEN
        DELETE FROM transformadores_aneel 
        WHERE ativo = FALSE 
          AND data_atualizacao < v_data_limite;
        RETURN QUERY SELECT 'DELETE'::TEXT, 'transformadores_aneel'::VARCHAR, v_count, 'Registros inativos removidos'::TEXT;
    ELSE
        RETURN QUERY SELECT 'DRY_RUN'::TEXT, 'transformadores_aneel'::VARCHAR, v_count, 'Seria removidos (DRY RUN)'::TEXT;
    END IF;
    
    -- Limpar áreas inválidas (sem geometria)
    SELECT COUNT(*) INTO v_count 
    FROM transformador_area_cobertura 
    WHERE geom IS NULL;
    
    IF NOT p_dry_run AND v_count > 0 THEN
        DELETE FROM transformador_area_cobertura 
        WHERE geom IS NULL;
        RETURN QUERY SELECT 'DELETE'::TEXT, 'transformador_area_cobertura'::VARCHAR, v_count, 'Áreas inválidas removidas'::TEXT;
    ELSE
        RETURN QUERY SELECT 'DRY_RUN'::TEXT, 'transformador_area_cobertura'::VARCHAR, v_count, 'Seria removidas (DRY RUN)'::TEXT;
    END IF;

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sp_limpar_dados_obsoletos IS 
'Remove dados obsoletos e inválidos
Uso: SELECT * FROM sp_limpar_dados_obsoletos(90, TRUE);  -- dry run
     SELECT * FROM sp_limpar_dados_obsoletos(90, FALSE); -- executar de verdade';


-- ============================================================================
-- TABELA: Telhados Detectados por Transformador
-- ============================================================================
-- Objetivo: Armazenar telhados detectados usando chaves estrangeiras
--           das tabelas ANEEL BDGD (transformadores_aneel, subestacoes_aneel)
-- Data: 2026-02-04
-- ============================================================================

CREATE TABLE IF NOT EXISTS telhados_detectados_transformador (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores_aneel(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_aneel(id),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    area_m2 DOUBLE PRECISION NOT NULL,
    confianca DOUBLE PRECISION NOT NULL CHECK (confianca >= 0 AND confianca <= 1),
    bbox_json JSONB,
    fonte_imagem VARCHAR(50) DEFAULT 'google_maps',
    resolucao_cm DOUBLE PRECISION DEFAULT 30.0,
    timestamp_deteccao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW(),
    url_imagem_origem TEXT
);

-- Índices para telhados_detectados_transformador
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_transformador 
    ON telhados_detectados_transformador(transformador_id);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_subestacao 
    ON telhados_detectados_transformador(subestacao_id);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_timestamp 
    ON telhados_detectados_transformador(timestamp_deteccao DESC);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_confianca 
    ON telhados_detectados_transformador(confianca DESC);


-- ============================================================================
-- VIEWS: Telhados com Contexto ANEEL BDGD
-- ============================================================================

-- View para ver telhados com dados de transformador e subestação
DROP VIEW IF EXISTS vw_telhados_completo CASCADE;
CREATE OR REPLACE VIEW vw_telhados_completo AS
SELECT 
    t.id as telhado_id,
    t.transformador_id,
    t.subestacao_id,
    t.latitude,
    t.longitude,
    t.area_m2,
    t.confianca,
    t.fonte_imagem,
    t.timestamp_deteccao,
    tr.codigo as transformador_codigo,
    tr.nome as transformador_nome,
    tr.potencia_kva,
    tr.distribuidora,
    s.codigo as subestacao_codigo,
    s.nome as subestacao_nome
FROM telhados_detectados_transformador t
LEFT JOIN transformadores_aneel tr ON t.transformador_id = tr.id
LEFT JOIN subestacoes_aneel s ON t.subestacao_id = s.id
ORDER BY t.timestamp_deteccao DESC;

-- View com estatísticas gerais
DROP VIEW IF EXISTS vw_telhados_estatisticas CASCADE;
CREATE OR REPLACE VIEW vw_telhados_estatisticas AS
SELECT 
    COUNT(DISTINCT t.id) as total_telhados,
    COUNT(DISTINCT t.transformador_id) as total_transformadores,
    COUNT(DISTINCT t.subestacao_id) as total_subestacoes,
    ROUND(AVG(t.area_m2)::NUMERIC, 2) as area_media_m2,
    ROUND(SUM(t.area_m2)::NUMERIC, 2) as area_total_m2,
    ROUND(AVG(t.confianca)::NUMERIC, 3) as confianca_media,
    MIN(t.confianca) as confianca_minima,
    MAX(t.confianca) as confianca_maxima,
    MIN(t.timestamp_deteccao) as primeira_deteccao,
    MAX(t.timestamp_deteccao) as ultima_deteccao
FROM telhados_detectados_transformador t;

-- View com estatísticas por subestação
DROP VIEW IF EXISTS vw_telhados_por_subestacao CASCADE;
CREATE OR REPLACE VIEW vw_telhados_por_subestacao AS
SELECT 
    s.id as subestacao_id,
    s.codigo as subestacao_codigo,
    s.nome as subestacao_nome,
    COUNT(DISTINCT t.id) as total_telhados,
    COUNT(DISTINCT t.transformador_id) as total_transformadores,
    ROUND(AVG(t.area_m2)::NUMERIC, 2) as area_media_m2,
    ROUND(SUM(t.area_m2)::NUMERIC, 2) as area_total_m2,
    ROUND(AVG(t.confianca)::NUMERIC, 3) as confianca_media,
    MAX(t.timestamp_deteccao) as ultima_deteccao
FROM subestacoes_aneel s
LEFT JOIN telhados_detectados_transformador t ON s.id = t.subestacao_id
GROUP BY s.id, s.codigo, s.nome
ORDER BY total_telhados DESC;

-- View com estatísticas por transformador
DROP VIEW IF EXISTS vw_telhados_por_transformador CASCADE;
CREATE OR REPLACE VIEW vw_telhados_por_transformador AS
SELECT 
    tr.id as transformador_id,
    tr.codigo as transformador_codigo,
    tr.nome as transformador_nome,
    tr.potencia_kva,
    s.codigo as subestacao_codigo,
    COUNT(DISTINCT t.id) as total_telhados,
    ROUND(AVG(t.area_m2)::NUMERIC, 2) as area_media_m2,
    ROUND(SUM(t.area_m2)::NUMERIC, 2) as area_total_m2,
    ROUND(AVG(t.confianca)::NUMERIC, 3) as confianca_media,
    MAX(t.timestamp_deteccao) as ultima_deteccao
FROM transformadores_aneel tr
LEFT JOIN subestacoes_aneel s ON tr.subestacao_codigo = s.codigo
LEFT JOIN telhados_detectados_transformador t ON tr.id = t.transformador_id
GROUP BY tr.id, tr.codigo, tr.nome, tr.potencia_kva, s.codigo
ORDER BY total_telhados DESC;


-- ============================================================================
-- FIM DO SCHEMA
-- ============================================================================
