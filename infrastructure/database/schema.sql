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

CREATE INDEX IF NOT EXISTS idx_carga_ons_time ON carga_ons (time);
CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema ON carga_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_gd_detalhada_distribuidora ON gd_detalhada (distribuidora);
CREATE INDEX IF NOT EXISTS idx_auditoria_visual_distribuidora ON auditoria_visual (distribuidora);

SELECT create_hypertable('carga_ons', 'time', if_not_exists => TRUE);
SELECT create_hypertable('clima_real', 'time', if_not_exists => TRUE);