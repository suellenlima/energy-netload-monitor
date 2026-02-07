-- ============================================================================
-- ENERGY NETLOAD MONITOR - SCHEMA UNIFICADO (APENAS TABELAS UTILIZADAS)
-- ============================================================================
-- Data: 2026-02-07 (Atualizado)
-- Descrição: Schema unificado contendo TODAS as tabelas realmente utilizadas
-- Consolidação: schema.sql + schema_aneel_bdgd.sql mesclados em um único arquivo
-- Adicionado: Tabelas de consumidores ANEEL (BT/MT/AT) com UPSERT support
-- Removidas: 30+ tabelas não referenciadas + 4 tabelas ANEEL não utilizadas
-- ============================================================================

-- ============================================================================
-- EXTENSÕES
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================================
-- TIPOS ENUM
-- ============================================================================

DO $$ BEGIN
    CREATE TYPE tipo_estabelecimento AS ENUM (
        'residencia',
        'predio_residencial',
        'comercio',
        'predio_comercial',
        'industria',
        'outro'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE enum_tipo_tensao AS ENUM ('BT', 'MT', 'AT', 'DESCONHECIDO');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE enum_metodo_calculo AS ENUM ('convex_hull', 'buffer_500m', 'buffer_1km', 'buffer_2km', 'buffer_5km');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE enum_status_processamento AS ENUM ('em_processamento', 'concluido', 'erro', 'parcial');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ============================================================================
-- FUNÇÕES DE VALIDAÇÃO
-- ============================================================================

CREATE OR REPLACE FUNCTION fn_validar_coordenadas(lat DECIMAL, lon DECIMAL)
RETURNS BOOLEAN AS $$
BEGIN
    IF lat IS NULL OR lon IS NULL THEN
        RETURN TRUE;
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

-- ============================================================================
-- PARTE 1: DADOS ANEEL - TRANSFORMADORES E SUBESTAÇÕES
-- ============================================================================

-- Tabela de Transformadores ANEEL (BDGD)
CREATE SEQUENCE IF NOT EXISTS transformadores_aneel_id_seq;

CREATE TABLE IF NOT EXISTS transformadores_aneel (
    id INTEGER PRIMARY KEY DEFAULT nextval('transformadores_aneel_id_seq'),
    codigo VARCHAR(100) UNIQUE NOT NULL,
    nome VARCHAR(255),
    subestacao_id INTEGER,
    subestacao_codigo VARCHAR(100),
    tensao_primaria_kv NUMERIC(10,2),
    tensao_secundaria_kv NUMERIC(10,2),
    tensao_secundaria_v NUMERIC(10,2),
    potencia_nominal_kva NUMERIC(10,2),
    potencia_kva NUMERIC(10,2),
    impedancia_percentual NUMERIC(10,2),
    tipo_tensao VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    geom GEOMETRY(Point, 4326),
    distribuidora VARCHAR(255),
    dist_codigo VARCHAR(50),
    status VARCHAR(50) DEFAULT 'ativo',
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_importacao TIMESTAMP DEFAULT NOW(),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    ativo BOOLEAN DEFAULT TRUE
);

-- Garantir que a sequência pertence à coluna
ALTER SEQUENCE transformadores_aneel_id_seq OWNED BY transformadores_aneel.id;

-- Tabela de Subestações ANEEL (BDGD)
CREATE SEQUENCE IF NOT EXISTS subestacoes_aneel_id_seq;

CREATE TABLE IF NOT EXISTS subestacoes_aneel (
    id INTEGER PRIMARY KEY DEFAULT nextval('subestacoes_aneel_id_seq'),
    codigo VARCHAR(100) UNIQUE,
    nome VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    geom GEOMETRY(Point, 4326),
    tensao_kv NUMERIC(10,2),
    tensao_operacao_kv NUMERIC(10,2),
    codigo_ons VARCHAR(50),
    distribuidora VARCHAR(255),
    dist_codigo VARCHAR(50),
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL'
);

-- Garantir que a sequência pertence à coluna
ALTER SEQUENCE subestacoes_aneel_id_seq OWNED BY subestacoes_aneel.id;

-- ============================================================================
-- TABELAS DE CONSUMIDORES ANEEL (UNIDADES CONSUMIDORAS - BDGD)
-- ============================================================================

-- Consumidores de Baixa Tensão (BT)
CREATE TABLE IF NOT EXISTS consumidores_bt_aneel (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(50),
    subestacao_codigo VARCHAR(50),
    classe_subclasse_codigo VARCHAR(20),
    tensao_fornecimento_codigo VARCHAR(20),
    carga_instalada_kw DECIMAL(10, 2),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumidores_bt_distribuidora ON consumidores_bt_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_subestacao ON consumidores_bt_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_bt_coords ON consumidores_bt_aneel(latitude, longitude);

-- Consumidores de Média Tensão (MT)
CREATE TABLE IF NOT EXISTS consumidores_mt_aneel (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(50),
    subestacao_codigo VARCHAR(50),
    circuito_mt_codigo VARCHAR(50),
    classe_subclasse_codigo VARCHAR(20),
    tensao_fornecimento_codigo VARCHAR(20),
    carga_instalada_kw DECIMAL(10, 2),
    demanda_contratada_kw DECIMAL(10, 2),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumidores_mt_distribuidora ON consumidores_mt_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_subestacao ON consumidores_mt_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_mt_coords ON consumidores_mt_aneel(latitude, longitude);

-- Consumidores de Alta Tensão (AT)
CREATE TABLE IF NOT EXISTS consumidores_at_aneel (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    dist_codigo VARCHAR(50),
    subestacao_codigo VARCHAR(50),
    circuito_at_codigo VARCHAR(50),
    classe_subclasse_codigo VARCHAR(20),
    tensao_fornecimento_codigo VARCHAR(20),
    carga_instalada_kw DECIMAL(10, 2),
    demanda_contratada_kw DECIMAL(10, 2),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumidores_at_distribuidora ON consumidores_at_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_subestacao ON consumidores_at_aneel(subestacao_codigo);
CREATE INDEX IF NOT EXISTS idx_consumidores_at_coords ON consumidores_at_aneel(latitude, longitude);

-- ============================================================================
-- PROCESSAMENTO E CONTROLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS aneel_bdgd_processamento (
    id SERIAL PRIMARY KEY,
    tipo_processamento VARCHAR(100) NOT NULL,
    data_inicio TIMESTAMP DEFAULT NOW(),
    data_fim TIMESTAMP,
    status enum_status_processamento DEFAULT 'em_processamento',
    transformadores_processados INTEGER DEFAULT 0,
    subestacoes_processadas INTEGER DEFAULT 0,
    consumidores_processados INTEGER DEFAULT 0,
    erros_count INTEGER DEFAULT 0,
    observacoes TEXT,
    resultado_json JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS transformador_area_cobertura (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER UNIQUE NOT NULL REFERENCES transformadores_aneel(id),
    area_cobertura GEOMETRY(Polygon, 4326),
    metodo_definicao VARCHAR(100),
    area_km2 NUMERIC(10,2),
    raio_aproximado_m NUMERIC(10,2),
    total_consumidores INTEGER,
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    observacoes TEXT
);

-- ============================================================================
-- PARTE 2: CARGAS REAIS (DADOS EM TEMPO REAL)
-- ============================================================================

CREATE TABLE IF NOT EXISTS carga_ons (
    time TIMESTAMPTZ NOT NULL,
    subsistema TEXT,
    carga_mw DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS irradiancia_solar (
    time TIMESTAMPTZ NOT NULL,
    subsistema VARCHAR(50),
    irradiancia_wm2 DOUBLE PRECISION,
    temperatura_c DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS geracao_mmgd (
    time TIMESTAMPTZ NOT NULL,
    subsistema VARCHAR(50),
    geracao_mw DOUBLE PRECISION
);

-- Tabela de MMGD por Distribuidora e Subestação (Dados ANEEL)
CREATE TABLE IF NOT EXISTS geracao_mmgd_distribuidora (
    id BIGSERIAL PRIMARY KEY,
    distribuidora TEXT NOT NULL,
    distribuidora_normalizada TEXT,
    subsistema TEXT,
    subestacao TEXT,
    fonte_geracao TEXT,  -- 'Solar', 'Eólica', 'Hidro', 'Biomassa', etc
    potencia_total_kw FLOAT8 NOT NULL,
    quantidade_empreendimentos INT DEFAULT 0,
    data_medicao TIMESTAMP WITH TIME ZONE,
    data_insercao TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_mmgd_distribuidor_subestacao_fonte 
        UNIQUE (distribuidora, subestacao, fonte_geracao, data_medicao)
);

CREATE INDEX IF NOT EXISTS idx_geracao_mmgd_distribuidora_name 
ON geracao_mmgd_distribuidora(distribuidora);

CREATE INDEX IF NOT EXISTS idx_geracao_mmgd_distribuidora_normalized
ON geracao_mmgd_distribuidora(distribuidora_normalizada);

CREATE INDEX IF NOT EXISTS idx_geracao_mmgd_distribuidora_subsistema
ON geracao_mmgd_distribuidora(subsistema);

CREATE INDEX IF NOT EXISTS idx_geracao_mmgd_distribuidora_subestacao
ON geracao_mmgd_distribuidora(subestacao);

-- Tabela de GD Granular (dados detalhados por estabelecimento MMGD)
CREATE TABLE IF NOT EXISTS gd_granular (
    id BIGSERIAL PRIMARY KEY,
    distribuidora TEXT NOT NULL,
    distribuidora_normalizada TEXT,
    classe_consumo TEXT,
    tipo_consumidor VARCHAR(10),
    subgrupo_tarifario TEXT,
    qtd_unidades INT DEFAULT 1,
    sigla_uf VARCHAR(2),
    fonte_geracao TEXT,
    potencia_kw FLOAT8,
    tipo_estabelecimento TEXT,
    data_insercao TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gd_granular_distribuidora 
ON gd_granular(distribuidora);

CREATE INDEX IF NOT EXISTS idx_gd_granular_distribuidora_normalized
ON gd_granular(distribuidora_normalizada);

CREATE INDEX IF NOT EXISTS idx_gd_granular_tipo_estabelecimento
ON gd_granular(tipo_estabelecimento);

CREATE TABLE IF NOT EXISTS consumidor (
    id SERIAL PRIMARY KEY,
    codigo_cliente VARCHAR(50) UNIQUE,
    transformador_id INTEGER,
    distribuidora VARCHAR(255),
    classe_consumo VARCHAR(50),
    consumo_kwh NUMERIC,
    data_referencia DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Hypertables para dados em série temporal
SELECT create_hypertable('carga_ons', 'time', if_not_exists => TRUE);
SELECT create_hypertable('irradiancia_solar', 'time', if_not_exists => TRUE);
SELECT create_hypertable('geracao_mmgd', 'time', if_not_exists => TRUE);

-- ============================================================================
-- PARTE 3: REAL-TIME ESTIMATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS estado_sistema_realtime (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    subsistema VARCHAR(50),
    carga_mw DOUBLE PRECISION,
    geracao_mmgd_mw DOUBLE PRECISION,
    irradiancia_wm2 DOUBLE PRECISION,
    previsao_carga_mw DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS previsoes_carga (
    id SERIAL PRIMARY KEY,
    subsistema VARCHAR(50),
    hora_inicial TIMESTAMP,
    hora_final TIMESTAMP,
    carga_prevista_mw DOUBLE PRECISION,
    confiabilidade NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PARTE 4: LOAD CALCULATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS perfis_carga_classe (
    id SERIAL PRIMARY KEY,
    classe_consumo VARCHAR(50),
    hora INTEGER,
    percentual_carga NUMERIC,
    tipo_dia VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS consumo_granular_classe (
    id SERIAL PRIMARY KEY,
    distribuidora VARCHAR(255),
    classe_consumo VARCHAR(50),
    consumo_kwh NUMERIC,
    data_medicao TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(distribuidora, classe_consumo, data_medicao)
);

CREATE TABLE IF NOT EXISTS mmgd_subsistema (
    id SERIAL PRIMARY KEY,
    subsistema VARCHAR(50),
    geracao_mw NUMERIC,
    data_referencia DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS calibracao_parametros (
    id SERIAL PRIMARY KEY,
    parametro_nome VARCHAR(100),
    valor_parametro NUMERIC,
    subsistema VARCHAR(50),
    data_criacao TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cargas_calculadas (
    id SERIAL PRIMARY KEY,
    subsistema VARCHAR(50),
    distribuidora VARCHAR(255),
    carga_calculada_mw NUMERIC,
    timestamp_calculo TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PARTE 5: TELHADOS E PAINÉIS SOLARES
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
    url_imagem_origem TEXT,
    codigo VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS paineis_solares_detectados (
    id SERIAL PRIMARY KEY,
    telhado_id INTEGER NOT NULL REFERENCES telhados_detectados_transformador(id),
    transformador_id INTEGER NOT NULL REFERENCES transformadores_aneel(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_aneel(id),
    bbox_json JSONB NOT NULL,
    centroide_json JSONB NOT NULL,
    area_pixeis INTEGER NOT NULL,
    area_m2 DOUBLE PRECISION NOT NULL,
    confianca DOUBLE PRECISION NOT NULL CHECK (confianca >= 0 AND confianca <= 1),
    tipo_painel VARCHAR(50) DEFAULT 'desconhecido',
    potencia_w DOUBLE PRECISION,
    timestamp_deteccao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS potencia_telhados (
    id SERIAL PRIMARY KEY,
    telhado_id INTEGER NOT NULL UNIQUE REFERENCES telhados_detectados_transformador(id),
    transformador_id INTEGER NOT NULL REFERENCES transformadores_aneel(id),
    num_paineis INTEGER DEFAULT 0,
    area_total_m2 DOUBLE PRECISION DEFAULT 0,
    potencia_instalada_kw DOUBLE PRECISION DEFAULT 0,
    producao_diaria_kwh DOUBLE PRECISION DEFAULT 0,
    producao_anual_kwh DOUBLE PRECISION DEFAULT 0,
    economia_anual_brl DOUBLE PRECISION DEFAULT 0,
    potencia_por_m2 DOUBLE PRECISION DEFAULT 150.0,
    fator_capacidade DOUBLE PRECISION DEFAULT 0.15,
    insolacao_media_kwh_m2_dia DOUBLE PRECISION DEFAULT 4.5,
    tarifa_brl_kwh DOUBLE PRECISION DEFAULT 0.80,
    timestamp_calculo TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- PARTE 6: SATÉLITE (APENAS CBERS4A UTILIZADO)
-- ============================================================================

CREATE TABLE IF NOT EXISTS requisicoes_satelite_cbers4a (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER REFERENCES subestacoes_aneel(id) ON DELETE CASCADE,
    transformador_id INTEGER REFERENCES transformadores_aneel(id) ON DELETE CASCADE,
    data_requisicao TIMESTAMPTZ DEFAULT NOW(),
    tipo_requisicao VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'sucesso',
    data_imagem TIMESTAMPTZ,
    cobertura_nuvem_percentual DOUBLE PRECISION,
    resolucao_metros DOUBLE PRECISION DEFAULT 2.0,
    bbox_min_lat DOUBLE PRECISION,
    bbox_min_lon DOUBLE PRECISION,
    bbox_max_lat DOUBLE PRECISION,
    bbox_max_lon DOUBLE PRECISION,
    imagem_id VARCHAR(255),
    url_download TEXT,
    tamanho_mb DOUBLE PRECISION,
    observacoes TEXT,
    fonte_satelite VARCHAR(50),
    custo_usd_estimado DOUBLE PRECISION,
    tempo_requisicao_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- ÍNDICES - ANEEL
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_geom ON transformadores_aneel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_distribuidora ON transformadores_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_status ON transformadores_aneel(status);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_distribuida_status ON transformadores_aneel(distribuidora, status);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_geom ON subestacoes_aneel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_codigo ON subestacoes_aneel(codigo);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_distribuidora ON subestacoes_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_tensao ON subestacoes_aneel(tensao_kv);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_localizacao ON subestacoes_aneel(localizacao) WHERE localizacao IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transformador_area_geom ON transformador_area_cobertura USING GIST(area_cobertura);
CREATE INDEX IF NOT EXISTS idx_transformador_area_id ON transformador_area_cobertura(transformador_id);

-- ============================================================================
-- ÍNDICES - CARGAS EM TEMPO REAL
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_carga_ons_time ON carga_ons (time);
CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema ON carga_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema_time ON carga_ons (subsistema, time DESC);
CREATE INDEX IF NOT EXISTS idx_irradiancia_solar_subsistema_time ON irradiancia_solar (subsistema, time);
CREATE INDEX IF NOT EXISTS idx_geracao_mmgd_subsistema_time ON geracao_mmgd (subsistema, time DESC);

-- ============================================================================
-- ÍNDICES - REAL-TIME ESTIMATION
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_estado_sistema_time ON estado_sistema_realtime(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_estado_sistema_subsistema ON estado_sistema_realtime(subsistema);
CREATE INDEX IF NOT EXISTS idx_previsoes_carga_subsistema ON previsoes_carga(subsistema);
CREATE INDEX IF NOT EXISTS idx_previsoes_carga_hora ON previsoes_carga(hora_inicial);

-- ============================================================================
-- ÍNDICES - LOAD CALCULATION
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_perfis_carga_classe ON perfis_carga_classe(classe_consumo, hora);
CREATE INDEX IF NOT EXISTS idx_consumo_granular_dist ON consumo_granular_classe(distribuidora);
CREATE INDEX IF NOT EXISTS idx_mmgd_subsistema ON mmgd_subsistema(subsistema);
CREATE INDEX IF NOT EXISTS idx_cargas_calculadas_timestamp ON cargas_calculadas(timestamp_calculo DESC);

-- ============================================================================
-- ÍNDICES - TELHADOS E PAINÉIS
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_transformador ON telhados_detectados_transformador(transformador_id);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_subestacao ON telhados_detectados_transformador(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_timestamp ON telhados_detectados_transformador(timestamp_deteccao DESC);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_confianca ON telhados_detectados_transformador(confianca DESC);
CREATE INDEX IF NOT EXISTS idx_paineis_telhado ON paineis_solares_detectados(telhado_id);
CREATE INDEX IF NOT EXISTS idx_paineis_transformador ON paineis_solares_detectados(transformador_id);
CREATE INDEX IF NOT EXISTS idx_paineis_subestacao ON paineis_solares_detectados(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_paineis_timestamp ON paineis_solares_detectados(timestamp_deteccao DESC);
CREATE INDEX IF NOT EXISTS idx_paineis_confianca ON paineis_solares_detectados(confianca DESC);
CREATE INDEX IF NOT EXISTS idx_potencia_transformador ON potencia_telhados(transformador_id);
CREATE INDEX IF NOT EXISTS idx_potencia_timestamp ON potencia_telhados(timestamp_atualizacao DESC);

-- ============================================================================
-- ÍNDICES - SATÉLITE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_subestacao ON requisicoes_satelite_cbers4a(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_data ON requisicoes_satelite_cbers4a(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_status ON requisicoes_satelite_cbers4a(status);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers4a_transformador_id ON requisicoes_satelite_cbers4a(transformador_id);

-- ============================================================================
-- TRIGGERS PARA ATUALIZAÇÃO AUTOMÁTICA DE GEOMETRIA
-- ============================================================================

CREATE OR REPLACE FUNCTION aneel_atualizar_geometria_transformadores()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
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
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
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
-- VIEWS
-- ============================================================================

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
    ROUND(SUM(CAST(t.potencia_nominal_kva AS DECIMAL))::NUMERIC, 2) as potencia_total_kva
FROM subestacoes_aneel s
LEFT JOIN transformadores_aneel t ON t.subestacao_codigo = s.codigo AND s.distribuidora = t.distribuidora
WHERE s.ativo = TRUE
GROUP BY s.id, s.codigo, s.nome, s.distribuidora, s.tensao_kv, s.latitude, s.longitude
ORDER BY s.distribuidora, quantidade_transformadores DESC;

-- ============================================================================
-- ESTATÍSTICAS E ANÁLISE
-- ============================================================================

ANALYZE transformadores_aneel;
ANALYZE subestacoes_aneel;
ANALYZE carga_ons;
ANALYZE irradiancia_solar;
ANALYZE geracao_mmgd;
ANALYZE estado_sistema_realtime;
ANALYZE telhados_detectados_transformador;
ANALYZE paineis_solares_detectados;
ANALYZE potencia_telhados;
ANALYZE requisicoes_satelite_cbers4a;

-- ============================================================================
-- ONS SUBSISTEMA - SCHEMA NORMALIZADO (3NF)
-- ============================================================================
-- Modelo normalizado para evitar duplicação de dados:
-- - subsistema_ons: Fatos (time series) - time, subsistema, carga_mw
-- - subsistema_ons_regiao: Dimensão - subsistema, regiao, codigo, nome_completo
-- Relacionamento 1:N através de FK

-- 1. Tabela de Dimensão de Subsistemas e Regiões
-- ============================================================================
CREATE TABLE IF NOT EXISTS subsistema_ons_regiao (
    subsistema TEXT PRIMARY KEY,
    subsistema_codigo VARCHAR(10) UNIQUE NOT NULL,
    regiao TEXT NOT NULL,
    nome_completo TEXT NOT NULL,
    descricao TEXT,
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE subsistema_ons_regiao IS 'Dimensão de subsistemas ONS com informações geográficas e de região';
COMMENT ON COLUMN subsistema_ons_regiao.subsistema IS 'Nome normalizado do subsistema (PK) - ex: norte, nordeste, sudeste/centro-oeste, sul';
COMMENT ON COLUMN subsistema_ons_regiao.subsistema_codigo IS 'Código único do subsistema - NO, NE, SE/CO, S';
COMMENT ON COLUMN subsistema_ons_regiao.regiao IS 'Região geográfica do Brasil';
COMMENT ON COLUMN subsistema_ons_regiao.nome_completo IS 'Nome completo e descritivo do subsistema';

-- 2. Renomear tabela carga_ons para subsistema_ons (se ainda não foi feita)
-- ============================================================================
-- Backup da tabela antiga se existir
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='carga_ons') THEN
        -- Criar tabela nova se não existir
        CREATE TABLE IF NOT EXISTS subsistema_ons (
            time TIMESTAMPTZ NOT NULL,
            subsistema TEXT,
            carga_mw DOUBLE PRECISION
        );
        
        -- Copiar dados se ainda não foram copiados
        IF (SELECT COUNT(*) FROM subsistema_ons) = 0 THEN
            INSERT INTO subsistema_ons SELECT time, subsistema, carga_mw FROM carga_ons;
            RAISE NOTICE 'Dados migrados de carga_ons para subsistema_ons';
        END IF;
        
        -- Renomear tabela antiga para backup
        ALTER TABLE carga_ons RENAME TO carga_ons_backup;
        RAISE NOTICE 'Tabela carga_ons renomeada para carga_ons_backup';
    ELSE
        -- Criar tabela nova se carga_ons não existe
        CREATE TABLE IF NOT EXISTS subsistema_ons (
            time TIMESTAMPTZ NOT NULL,
            subsistema TEXT,
            carga_mw DOUBLE PRECISION
        );
    END IF;
END $$;

-- 3. Criar tabela subsistema_ons como TimescaleDB hypertable
-- ============================================================================
CREATE TABLE IF NOT EXISTS subsistema_ons (
    time TIMESTAMPTZ NOT NULL,
    subsistema TEXT NOT NULL,
    carga_mw DOUBLE PRECISION NOT NULL
);

COMMENT ON TABLE subsistema_ons IS 'Fatos de carga por subsistema - série temporal otimizada';
COMMENT ON COLUMN subsistema_ons.time IS 'Timestamp UTC da medição de carga';
COMMENT ON COLUMN subsistema_ons.subsistema IS 'Chave estrangeira para subsistema_ons_regiao';
COMMENT ON COLUMN subsistema_ons.carga_mw IS 'Valor de carga em MW';

-- 4. Adicionar constraint de integridade referencial
-- ============================================================================
ALTER TABLE subsistema_ons
DROP CONSTRAINT IF EXISTS fk_subsistema_ons_regiao;

ALTER TABLE subsistema_ons
ADD CONSTRAINT fk_subsistema_ons_regiao 
FOREIGN KEY (subsistema) REFERENCES subsistema_ons_regiao(subsistema)
ON UPDATE CASCADE ON DELETE RESTRICT;

-- 5. Criar TimescaleDB hypertable
-- ============================================================================
SELECT create_hypertable('subsistema_ons', 'time', if_not_exists => TRUE);

-- 6. Criar índices para performance
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_subsistema_ons_subsistema 
ON subsistema_ons (subsistema);

CREATE INDEX IF NOT EXISTS idx_subsistema_ons_subsistema_time 
ON subsistema_ons (subsistema, time DESC);

CREATE INDEX IF NOT EXISTS idx_subsistema_ons_time 
ON subsistema_ons (time DESC);

CREATE INDEX IF NOT EXISTS idx_subsistema_ons_regiao_ativo 
ON subsistema_ons_regiao (ativo) WHERE ativo = TRUE;

-- 7. Criar view para facilitar análises (JOIN automático)
-- ============================================================================
CREATE OR REPLACE VIEW v_subsistema_ons_detalhado AS
SELECT 
    c.time,
    c.subsistema,
    c.carga_mw,
    r.subsistema_codigo,
    r.regiao,
    r.nome_completo,
    r.descricao,
    DATE_TRUNC('hour', c.time) as hora_medida,
    EXTRACT(YEAR FROM c.time)::INT as ano,
    EXTRACT(MONTH FROM c.time)::INT as mes,
    EXTRACT(DAY FROM c.time)::INT as dia,
    EXTRACT(DOW FROM c.time)::INT as dia_semana
FROM subsistema_ons c
LEFT JOIN subsistema_ons_regiao r ON c.subsistema = r.subsistema;

COMMENT ON VIEW v_subsistema_ons_detalhado IS 'View detalhada com JOIN automático entre fatos e dimensão';

-- 8. Criar view de agregação por região (evita JOIN em queries)
-- ============================================================================
CREATE OR REPLACE VIEW v_subsistema_ons_por_regiao AS
SELECT 
    r.regiao,
    r.subsistema_codigo,
    r.subsistema,
    c.time,
    c.carga_mw,
    AVG(c.carga_mw) OVER (PARTITION BY c.subsistema ORDER BY c.time ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) as carga_media_12h
FROM subsistema_ons c
LEFT JOIN subsistema_ons_regiao r ON c.subsistema = r.subsistema;

COMMENT ON VIEW v_subsistema_ons_por_regiao IS 'View com agregação por região e média móvel 12h';

-- 9. Inserir dados de referência de subsistemas e regiões
-- ============================================================================
INSERT INTO subsistema_ons_regiao (subsistema, subsistema_codigo, regiao, nome_completo, descricao, ativo)
VALUES 
    ('norte', 'NO', 'Norte', 'Subsistema Norte', 'Abrange principalmente os estados do Amazonas, Amapá, Pará e Roraima', TRUE),
    ('nordeste', 'NE', 'Nordeste', 'Subsistema Nordeste', 'Abrange os estados de Alagoas, Bahia, Ceará, Maranhão, Paraíba, Pernambuco, Piauí, Rio Grande do Norte e Sergipe', TRUE),
    ('sudeste/centro-oeste', 'SE/CO', 'Sudeste/Centro-Oeste', 'Subsistema Sudeste/Centro-Oeste', 'Abrange São Paulo, Minas Gerais, Rio de Janeiro, Espírito Santo, Mato Grosso, Mato Grosso do Sul, Goiás e Distrito Federal', TRUE),
    ('sul', 'S', 'Sul', 'Subsistema Sul', 'Abrange Rio Grande do Sul, Santa Catarina e Paraná', TRUE)
ON CONFLICT (subsistema) DO UPDATE SET
    regiao = EXCLUDED.regiao,
    nome_completo = EXCLUDED.nome_completo,
    subsistema_codigo = EXCLUDED.subsistema_codigo,
    data_atualizacao = NOW();

-- ============================================================================
-- PARTE 10: CARGA DAS DISTRIBUIDORAS (TEMPO REAL)
-- ============================================================================

CREATE TABLE IF NOT EXISTS carga_distribuidoras (
    id SERIAL PRIMARY KEY,
    distribuidora VARCHAR(255) NOT NULL,
    subsistema VARCHAR(50),
    carga_liquida_mw FLOAT NOT NULL,
    carga_estimada_total_mw FLOAT,
    data_medicao TIMESTAMP NOT NULL,
    data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(distribuidora, data_medicao)
);

CREATE INDEX IF NOT EXISTS idx_carga_dist_data 
    ON carga_distribuidoras(distribuidora, data_medicao DESC);

CREATE INDEX IF NOT EXISTS idx_carga_dist_subsistema 
    ON carga_distribuidoras(subsistema, data_medicao DESC);

-- ============================================================================
-- PARTE 11: CARGA DO ONS - DADOS REAIS EM TEMPO REAL
-- ============================================================================

CREATE TABLE IF NOT EXISTS carga_ons_realtime (
    id SERIAL PRIMARY KEY,
    data_medicao TIMESTAMP NOT NULL,
    subsistema VARCHAR(50) NOT NULL,
    distribuidora VARCHAR(255),
    carga_mw FLOAT NOT NULL,
    percentual FLOAT,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_medicao, subsistema, distribuidora)
);

CREATE INDEX IF NOT EXISTS idx_carga_ons_data 
    ON carga_ons_realtime(data_medicao DESC);

CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema 
    ON carga_ons_realtime(subsistema, data_medicao DESC);

CREATE INDEX IF NOT EXISTS idx_carga_ons_distribuidora 
    ON carga_ons_realtime(distribuidora, data_medicao DESC);

-- ============================================================================
-- PARTE 12: DISTRIBUIDORAS ANEEL - CADASTRO
-- ============================================================================

CREATE TABLE IF NOT EXISTS distribuidoras_aneel (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL UNIQUE,
    sigla VARCHAR(50),
    regiao VARCHAR(50),
    subsistema VARCHAR(50),
    potencia_total_kva FLOAT,
    total_transformadores INTEGER DEFAULT 0,
    total_subestacoes INTEGER DEFAULT 0,
    total_consumidores INTEGER DEFAULT 0,
    data_carregamento TIMESTAMP,
    ativo BOOLEAN DEFAULT TRUE,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dist_aneel_nome 
    ON distribuidoras_aneel(nome);

CREATE INDEX IF NOT EXISTS idx_dist_aneel_regiao 
    ON distribuidoras_aneel(regiao, ativo);

CREATE INDEX IF NOT EXISTS idx_dist_aneel_subsistema 
    ON distribuidoras_aneel(subsistema, ativo);

INSERT INTO distribuidoras_aneel (nome, sigla, regiao, subsistema, potencia_total_kva, ativo)
VALUES
    ('LIGHT', 'LIGHT', 'Sudeste', 'SUDESTE', 25000000, TRUE),
    ('ENEL', 'ENEL', 'Sudeste', 'SUDESTE', 28000000, TRUE),
    ('CPFL', 'CPFL', 'Sudeste', 'SUDESTE', 22000000, TRUE),
    ('ELEKTRO', 'ELEKTRO', 'Sudeste', 'SUDESTE', 18000000, TRUE),
    ('CEMIG', 'CEMIG', 'Sudeste', 'SUDESTE', 35000000, TRUE),
    ('AES', 'AES', 'Sul', 'SUL', 15000000, TRUE),
    ('COPEL', 'COPEL', 'Sul', 'SUL', 32000000, TRUE),
    ('RGE', 'RGE', 'Sul', 'SUL', 12000000, TRUE),
    ('NEOENERGIA', 'NEOENERGIA', 'Nordeste', 'NORDESTE', 20000000, TRUE),
    ('EQUATORIAL', 'EQUATORIAL', 'Nordeste', 'NORDESTE', 18000000, TRUE),
    ('COSERN', 'COSERN', 'Nordeste', 'NORDESTE', 10000000, TRUE),
    ('AMPERE', 'AMPERE', 'Norte', 'NORTE', 8000000, TRUE)
ON CONFLICT (nome) DO NOTHING;

-- ============================================================================
-- ALTERAÇÕES EM TABELAS EXISTENTES (Backward Compatibility)
-- ============================================================================

-- Garantir que gd_granular tem coluna distribuidora_normalizada
ALTER TABLE IF EXISTS gd_granular 
ADD COLUMN IF NOT EXISTS distribuidora_normalizada TEXT;

-- ============================================================================
-- STORED PROCEDURES
-- ============================================================================

CREATE OR REPLACE FUNCTION sp_calcular_area_transformadores(
    p_tipo_tensao VARCHAR,
    p_distribuidora VARCHAR,
    p_apenas_ativos BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(
    transformador_id INTEGER,
    area_m2 NUMERIC,
    total_consumidores INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        COALESCE(ST_Area(tc.area_cobertura)::NUMERIC, 0) as area_m2,
        tc.total_consumidores
    FROM transformadores_aneel t
    LEFT JOIN transformador_area_cobertura tc ON t.id = tc.transformador_id
    WHERE (p_tipo_tensao IS NULL OR t.tipo_tensao = p_tipo_tensao)
        AND (p_distribuidora IS NULL OR t.distribuidora = p_distribuidora)
        AND (NOT p_apenas_ativos OR t.ativo = TRUE);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- RESUMO FINAL
-- ============================================================================

SELECT 
        'Schema Unificado criado com sucesso!' as status,
        NOW() as data_execucao;

SELECT 
        'Total de tabelas criadas:' as info,
        COUNT(*) as total
FROM information_schema.tables 
WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';

-- ============================================================================
-- FIM DO SCHEMA UNIFICADO
-- ============================================================================
