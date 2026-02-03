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

-- Tabela BDGD: Base de Dados Geográfica da Distribuidora
-- Fonte: ANEEL - https://dadosabertos.aneel.gov.br/dataset/bdgd
CREATE TABLE IF NOT EXISTS bdgd_unidades_consumidoras (
    id BIGSERIAL PRIMARY KEY,
    cod_id VARCHAR(50),
    distribuidora VARCHAR(100) NOT NULL,
    classe_consumo VARCHAR(50) NOT NULL,  -- Residencial, Comercial, Industrial, Rural, Poder Público
    subclasse VARCHAR(50),                 -- Baixa Renda, Normal, etc.
    energia_kwh_mes NUMERIC,               -- Consumo mensal médio em kWh
    tensao_fornecimento VARCHAR(20),       -- AT, MT, BT
    municipio VARCHAR(100),
    bairro VARCHAR(100),
    geom GEOMETRY(Point, 4326),            -- Coordenadas geográficas
    data_referencia DATE,                  -- Mês/ano de referência dos dados
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT bdgd_uc_energia_check CHECK (energia_kwh_mes >= 0)
);

-- Índices para performance na BDGD
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_classe ON bdgd_unidades_consumidoras(classe_consumo);
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_geom ON bdgd_unidades_consumidoras USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_distribuidora ON bdgd_unidades_consumidoras(distribuidora);
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_municipio ON bdgd_unidades_consumidoras(municipio);
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_composite ON bdgd_unidades_consumidoras(distribuidora, classe_consumo);
CREATE INDEX IF NOT EXISTS idx_bdgd_uc_data_ref ON bdgd_unidades_consumidoras(data_referencia);

-- Comentários explicativos para tabela BDGD
COMMENT ON TABLE bdgd_unidades_consumidoras IS 'Unidades Consumidoras da Base de Dados Geográfica da Distribuidora (BDGD) - ANEEL';
COMMENT ON COLUMN bdgd_unidades_consumidoras.energia_kwh_mes IS 'Consumo médio mensal em kWh (dados mensais agregados)';
COMMENT ON COLUMN bdgd_unidades_consumidoras.classe_consumo IS 'Classe de consumo segundo ANEEL (Residencial, Comercial, Industrial, Rural, Poder Público)';
COMMENT ON COLUMN bdgd_unidades_consumidoras.geom IS 'Localização geográfica da UC para associação espacial com subestações';

-- Comentários explicativos para tabela usinas_siga
COMMENT ON TABLE usinas_siga IS 'Usinas geradoras cadastradas no SIGA/ANEEL';
COMMENT ON COLUMN usinas_siga.potencia_kw IS 'Potência instalada da usina (kW)';
COMMENT ON COLUMN usinas_siga.geom IS 'Localização geográfica da usina';

-- Comentários explicativos para tabela carga_ons
COMMENT ON TABLE carga_ons IS 'Histórico de carga líquida medida pelo ONS (Operador Nacional do Sistema)';
COMMENT ON COLUMN carga_ons.carga_mw IS 'Carga líquida medida pelo ONS nos pontos de entrega (MW) - potência média horária';
COMMENT ON COLUMN carga_ons.subsistema IS 'Subsistema elétrico (SUDESTE, SUL, NORDESTE, NORTE)';

-- Comentários explicativos para tabela clima_real
COMMENT ON TABLE clima_real IS 'Dados climáticos reais para estimativa de geração solar';
COMMENT ON COLUMN clima_real.irradiancia_wm2 IS 'Irradiância solar medida (W/m²) - usado para calcular geração fotovoltaica';
COMMENT ON COLUMN clima_real.temperatura_c IS 'Temperatura ambiente (°C) - afeta eficiência dos painéis solares';

-- Comentários explicativos para tabela gd_detalhada
COMMENT ON TABLE gd_detalhada IS 'Geração distribuída (micro e minigeração) agregada por distribuidora e classe';
COMMENT ON COLUMN gd_detalhada.potencia_mw IS 'Potência instalada de geração distribuída (MW)';
COMMENT ON COLUMN gd_detalhada.classe IS 'Classe de consumidor (Residencial, Comercial, Industrial, Rural)';

-- Comentários explicativos para tabela auditoria_visual
COMMENT ON TABLE auditoria_visual IS 'Resultados de auditoria visual com IA para detecção de fraudes';
COMMENT ON COLUMN auditoria_visual.diferenca_fraude_kw IS 'Diferença de potência entre estimativa IA e valor oficial (kW) - indica possível fraude';
COMMENT ON COLUMN auditoria_visual.potencia_oficial_kw IS 'Potência oficialmente registrada pela distribuidora (kW)';

-- Comentários explicativos para tabela subestacoes_ons
COMMENT ON TABLE subestacoes_ons IS 'Subestações oficiais do sistema elétrico nacional (fonte: ONS)';
COMMENT ON COLUMN subestacoes_ons.tensao_kv IS 'Tensão nominal da subestação (kV) - AT: 230/500/765, MT: 69/138';
COMMENT ON COLUMN subestacoes_ons.geom IS 'Localização geográfica da subestação';

-- Comentários explicativos para tabela subestacoes_detectadas
COMMENT ON TABLE subestacoes_detectadas IS 'Subestações detectadas por clustering geoespacial de GDs';
COMMENT ON COLUMN subestacoes_detectadas.potencia_total_mw IS 'Potência total agregada das GDs próximas (MW) - soma das instalações no cluster';
COMMENT ON COLUMN subestacoes_detectadas.raio_deteccao_km IS 'Raio de detecção usado no clustering geoespacial (km) - geralmente 5-15 km';
COMMENT ON COLUMN subestacoes_detectadas.quantidade_gd IS 'Quantidade de instalações de geração distribuída no cluster';
COMMENT ON COLUMN subestacoes_detectadas.geom IS 'Centroide do cluster (coordenadas médias das GDs agrupadas)';

-- Comentários explicativos para tabela gd_granular
COMMENT ON TABLE gd_granular IS 'Dados granulares de geração distribuída (nível de instalação individual)';
COMMENT ON COLUMN gd_granular.potencia_kw IS 'Potência instalada da geração distribuída (kW)';
COMMENT ON COLUMN gd_granular.qtd_unidades IS 'Número de unidades consumidoras atendidas por esta instalação de GD';
COMMENT ON COLUMN gd_granular.tipo_estabelecimento IS 'Tipo do estabelecimento (residência, comércio, indústria, etc.)';
COMMENT ON COLUMN gd_granular.classe_consumo IS 'Classe de consumo ANEEL (Residencial, Comercial, Industrial, Rural, Poder Público)';