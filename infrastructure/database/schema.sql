CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS usinas_siga (
    ceg TEXT,
    nome TEXT,
    fonte TEXT,
    combustivel TEXT,
    potencia_kw DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS carga_ons (
    time TIMESTAMPTZ NOT NULL,
    subsistema TEXT,
    carga_mw DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS clima_real (
    time TIMESTAMPTZ NOT NULL,
    subsistema VARCHAR(20),
    irradiancia_wm2 DOUBLE PRECISION,
    temperatura_c DOUBLE PRECISION,
    CONSTRAINT clima_real_unique UNIQUE (time, subsistema)
);

CREATE TABLE IF NOT EXISTS gd_detalhada (
    distribuidora TEXT,
    classe TEXT,
    sigla_uf TEXT,
    fonte TEXT,
    potencia_mw DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS auditoria_visual (
    id BIGSERIAL PRIMARY KEY,
    data_inspecao TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distribuidora TEXT,
    classe_estimada_ia TEXT,
    diferenca_fraude_kw DOUBLE PRECISION,
    potencia_oficial_kw DOUBLE PRECISION,
    status TEXT
);

CREATE TABLE IF NOT EXISTS subestacoes_ons (
    id SERIAL PRIMARY KEY,
    nome TEXT UNIQUE,
    sigla_se TEXT,
    tensao_kv DOUBLE PRECISION,
    subsistema TEXT,
    distribuidora TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    fonte_dados TEXT,
    geom geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS subestacoes_detectadas (
    id SERIAL PRIMARY KEY,
    cluster_id INTEGER,
    nome TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distribuidora TEXT,
    subsistema TEXT,
    quantidade_gd INTEGER,
    potencia_total_mw DOUBLE PRECISION,
    raio_deteccao_km DOUBLE PRECISION,
    data_deteccao TIMESTAMPTZ DEFAULT NOW(),
    geom geometry(Point, 4326)
);

CREATE INDEX IF NOT EXISTS idx_carga_ons_time ON carga_ons (time);
CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema ON carga_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_gd_detalhada_distribuidora ON gd_detalhada (distribuidora);
CREATE INDEX IF NOT EXISTS idx_auditoria_visual_distribuidora ON auditoria_visual (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_ons_distribuidora ON subestacoes_ons (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_ons_subsistema ON subestacoes_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_subestacoes_detectadas_distribuidora ON subestacoes_detectadas (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_detectadas_cluster ON subestacoes_detectadas (cluster_id);

SELECT create_hypertable('carga_ons', 'time', if_not_exists => TRUE);
SELECT create_hypertable('clima_real', 'time', if_not_exists => TRUE);

-- ENUM para tipos de estabelecimento
CREATE TYPE tipo_estabelecimento AS ENUM (
    'residencia',
    'predio_residencial',
    'comercio',
    'predio_comercial',
    'industria',
    'outro'
);

-- Tabela granular com dados detalhados para contabilização de unidades
CREATE TABLE IF NOT EXISTS gd_granular (
    id BIGSERIAL PRIMARY KEY,
    distribuidora TEXT NOT NULL,
    classe_consumo TEXT NOT NULL,
    tipo_consumidor TEXT,
    subgrupo_tarifario TEXT,
    qtd_unidades INTEGER NOT NULL DEFAULT 1,
    sigla_uf TEXT NOT NULL,
    fonte_geracao TEXT NOT NULL,
    potencia_kw DOUBLE PRECISION NOT NULL,
    tipo_estabelecimento tipo_estabelecimento NOT NULL,
    data_carga TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_gd_granular_distribuidora ON gd_granular (distribuidora);
CREATE INDEX IF NOT EXISTS idx_gd_granular_tipo_estab ON gd_granular (tipo_estabelecimento);
CREATE INDEX IF NOT EXISTS idx_gd_granular_composite ON gd_granular (distribuidora, tipo_estabelecimento);
CREATE INDEX IF NOT EXISTS idx_gd_granular_uf ON gd_granular (sigla_uf);