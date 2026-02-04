-- ============================================================================
-- ENERGY NETLOAD MONITOR - SCHEMA COMPLETO CONSOLIDADO
-- ============================================================================
-- Arquivo: schema_completo.sql
-- Data: 2026-02-01
-- Descrição: Schema completo consolidado de todos os arquivos SQL
-- Fonte: Consolidação de 15 arquivos SQL em infrastructure/database
-- ============================================================================

-- ============================================================================
-- EXTENSÕES
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- TIPOS ENUM
-- ============================================================================

-- ENUM para tipos de estabelecimento
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

-- ============================================================================
-- PARTE 1: TABELAS BASE (schema.sql)
-- ============================================================================

-- Tabela de usinas SIGA
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

-- Tabela de carga ONS
CREATE TABLE IF NOT EXISTS carga_ons (
    time TIMESTAMPTZ NOT NULL,
    subsistema TEXT,
    carga_mw DOUBLE PRECISION
);

-- Tabela de clima real
CREATE TABLE IF NOT EXISTS clima_real (
    time TIMESTAMPTZ NOT NULL,
    subsistema VARCHAR(20),
    irradiancia_wm2 DOUBLE PRECISION,
    temperatura_c DOUBLE PRECISION,
    CONSTRAINT clima_real_unique UNIQUE (time, subsistema)
);

-- Tabela de geração distribuída detalhada
CREATE TABLE IF NOT EXISTS gd_detalhada (
    distribuidora TEXT,
    classe TEXT,
    sigla_uf TEXT,
    fonte TEXT,
    potencia_mw DOUBLE PRECISION
);

-- Tabela de auditoria visual
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

-- Tabela de subestações ONS
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
    geometry geometry(Point, 4326)
);

-- Tabela de subestações detectadas
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
    geom geometry(Point, 4326),
    localizacao geometry(Point, 4326),
    fonte_dados VARCHAR(50) DEFAULT 'manual',
    codigo_ons VARCHAR(100)
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

-- Índices tabelas base
CREATE INDEX IF NOT EXISTS idx_carga_ons_time ON carga_ons (time);
CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema ON carga_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_gd_detalhada_distribuidora ON gd_detalhada (distribuidora);
CREATE INDEX IF NOT EXISTS idx_auditoria_visual_distribuidora ON auditoria_visual (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_ons_distribuidora ON subestacoes_ons (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_ons_subsistema ON subestacoes_ons (subsistema);
CREATE INDEX IF NOT EXISTS idx_subestacoes_detectadas_distribuidora ON subestacoes_detectadas (distribuidora);
CREATE INDEX IF NOT EXISTS idx_subestacoes_detectadas_cluster ON subestacoes_detectadas (cluster_id);
CREATE INDEX IF NOT EXISTS idx_gd_granular_distribuidora ON gd_granular (distribuidora);
CREATE INDEX IF NOT EXISTS idx_gd_granular_tipo_estab ON gd_granular (tipo_estabelecimento);
CREATE INDEX IF NOT EXISTS idx_gd_granular_composite ON gd_granular (distribuidora, tipo_estabelecimento);
CREATE INDEX IF NOT EXISTS idx_gd_granular_uf ON gd_granular (sigla_uf);
CREATE INDEX IF NOT EXISTS idx_subestacoes_fonte ON subestacoes_detectadas(fonte_dados);
CREATE INDEX IF NOT EXISTS idx_subestacoes_codigo_ons ON subestacoes_detectadas(codigo_ons);

-- Criar hypertables
SELECT create_hypertable('carga_ons', 'time', if_not_exists => TRUE);
SELECT create_hypertable('clima_real', 'time', if_not_exists => TRUE);

-- ============================================================================
-- PARTE 2: TABELAS DE SATÉLITE (001_satelite_tables.sql)
-- ============================================================================

-- Tabela principal para metadados de imagens
CREATE TABLE IF NOT EXISTS satelite_imagens (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    sensor VARCHAR(50) NOT NULL,
    data_aquisicao TIMESTAMPTZ NOT NULL,
    resolucao_m INTEGER,
    cobertura_nuvem_pct FLOAT CHECK (cobertura_nuvem_pct >= 0 AND cobertura_nuvem_pct <= 100),
    url TEXT,
    bbox_json JSONB,
    propriedades_json JSONB,
    url_google_maps_satellite TEXT,
    url_google_maps_hybrid TEXT,
    data_registro TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT satelite_imagens_unique UNIQUE (subestacao_id, sensor, data_aquisicao)
);

-- Tabela para armazenar bandas espectrais individuais de cada imagem
CREATE TABLE IF NOT EXISTS satelite_bandas (
    id SERIAL PRIMARY KEY,
    imagem_id INTEGER NOT NULL REFERENCES satelite_imagens(id) ON DELETE CASCADE,
    numero_banda INTEGER NOT NULL CHECK (numero_banda >= 0 AND numero_banda <= 4),
    nome_banda VARCHAR(20) NOT NULL,
    url TEXT NOT NULL,
    resolucao_m INTEGER,
    data_registro TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT satelite_bandas_unique UNIQUE(imagem_id, numero_banda),
    CHECK (nome_banda IN ('blue', 'green', 'red', 'nir', 'swir'))
);

-- Tabela para rastrear consultas/buscas realizadas
CREATE TABLE IF NOT EXISTS satelite_consultas (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    tipo_consulta VARCHAR(50),
    data_inicio TIMESTAMPTZ,
    data_fim TIMESTAMPTZ,
    raio_km FLOAT,
    sensores_consultados TEXT[],
    quantidade_resultados INTEGER,
    tempo_execucao_ms INTEGER,
    status VARCHAR(20),
    mensagem_erro TEXT,
    data_consulta TIMESTAMPTZ DEFAULT NOW()
);

-- Tabela para cache de resultados STAC
CREATE TABLE IF NOT EXISTS satelite_cache_stac (
    id BIGSERIAL PRIMARY KEY,
    bbox_hash VARCHAR(64) NOT NULL,
    min_lat FLOAT,
    max_lat FLOAT,
    min_lon FLOAT,
    max_lon FLOAT,
    data_inicio TIMESTAMPTZ,
    data_fim TIMESTAMPTZ,
    sensor VARCHAR(50),
    resultado_json JSONB,
    data_cache TIMESTAMPTZ DEFAULT NOW(),
    validade_ate TIMESTAMPTZ,
    CONSTRAINT satelite_cache_stac_unique UNIQUE (bbox_hash, sensor, data_inicio, data_fim)
);

-- Tabela para estatísticas de cobertura
CREATE TABLE IF NOT EXISTS satelite_cobertura_stats (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    sensor VARCHAR(50),
    mes_ano DATE,
    media_nuvem_pct FLOAT,
    total_cenas INTEGER,
    cenas_baixa_nuvem INTEGER,
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT satelite_stats_unique UNIQUE (subestacao_id, sensor, mes_ano)
);

-- Índices para satélite
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_subestacao ON satelite_imagens(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_data ON satelite_imagens(data_aquisicao DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_sensor ON satelite_imagens(sensor);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_nuvem ON satelite_imagens(cobertura_nuvem_pct);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_composite ON satelite_imagens(subestacao_id, data_aquisicao DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_bandas_imagem ON satelite_bandas(imagem_id);
CREATE INDEX IF NOT EXISTS idx_satelite_bandas_nome ON satelite_bandas(nome_banda);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_subestacao ON satelite_consultas(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_tipo ON satelite_consultas(tipo_consulta);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_data ON satelite_consultas(data_consulta DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_status ON satelite_consultas(status);
CREATE INDEX IF NOT EXISTS idx_satelite_cache_hash ON satelite_cache_stac(bbox_hash);
CREATE INDEX IF NOT EXISTS idx_satelite_cache_validade ON satelite_cache_stac(validade_ate);
CREATE INDEX IF NOT EXISTS idx_satelite_cache_sensor ON satelite_cache_stac(sensor);
CREATE INDEX IF NOT EXISTS idx_satelite_stats_subestacao ON satelite_cobertura_stats(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_stats_sensor ON satelite_cobertura_stats(sensor);
CREATE INDEX IF NOT EXISTS idx_satelite_stats_mes ON satelite_cobertura_stats(mes_ano DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_google_maps_satellite ON satelite_imagens(url_google_maps_satellite) WHERE url_google_maps_satellite IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_satelite_google_maps_hybrid ON satelite_imagens(url_google_maps_hybrid) WHERE url_google_maps_hybrid IS NOT NULL;

-- ============================================================================
-- PARTE 3: TABELAS DE TELHADOS (002_telhado_tables.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS telhado_deteccoes (
    id_deteccao SERIAL PRIMARY KEY,
    id_telhado VARCHAR(100) NOT NULL UNIQUE,
    id_subestacao INTEGER,
    id_imagem_satelite INTEGER,
    bbox_x INTEGER NOT NULL,
    bbox_y INTEGER NOT NULL,
    bbox_largura INTEGER NOT NULL,
    bbox_altura INTEGER NOT NULL,
    bbox_x_norm FLOAT NOT NULL,
    bbox_y_norm FLOAT NOT NULL,
    bbox_w_norm FLOAT NOT NULL,
    bbox_h_norm FLOAT NOT NULL,
    centroide_x FLOAT NOT NULL,
    centroide_y FLOAT NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    area_pixeis INTEGER NOT NULL,
    area_m2 FLOAT,
    confianca_deteccao FLOAT NOT NULL CHECK (confianca_deteccao >= 0 AND confianca_deteccao <= 1),
    tipo_edificio VARCHAR(50) DEFAULT 'desconhecido',
    percentual_cobertura FLOAT CHECK (percentual_cobertura >= 0 AND percentual_cobertura <= 100),
    indice_qualidade FLOAT CHECK (indice_qualidade >= 0 AND indice_qualidade <= 1),
    mascara_contorno BYTEA,
    modelo_deteccao VARCHAR(100) DEFAULT 'yolov8n-seg',
    timestamp_deteccao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    propriedades_json JSONB DEFAULT '{}',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT TRUE,
    CONSTRAINT fk_subestacao FOREIGN KEY (id_subestacao) REFERENCES subestacoes_detectadas(id),
    CONSTRAINT fk_imagem_satelite FOREIGN KEY (id_imagem_satelite) REFERENCES satelite_imagens(id)
);

CREATE TABLE IF NOT EXISTS telhado_rois (
    id_roi SERIAL PRIMARY KEY,
    id_deteccao INTEGER NOT NULL,
    tamanho_altura INTEGER NOT NULL,
    tamanho_largura INTEGER NOT NULL,
    resolucao_m_por_pixel FLOAT NOT NULL,
    area_aproximada_m2 FLOAT,
    percentual_cobertura FLOAT CHECK (percentual_cobertura >= 0 AND percentual_cobertura <= 100),
    indice_qualidade_roi FLOAT CHECK (indice_qualidade_roi >= 0 AND indice_qualidade_roi <= 1),
    caminho_arquivo_local VARCHAR(500),
    url_storage_s3 VARCHAR(500),
    tamanho_arquivo_kb FLOAT,
    hash_arquivo VARCHAR(64),
    timestamp_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_expiracao TIMESTAMP,
    processada BOOLEAN DEFAULT FALSE,
    CONSTRAINT fk_deteccao FOREIGN KEY (id_deteccao) REFERENCES telhado_deteccoes(id_deteccao) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS telhado_processamento_yolo (
    id_processamento SERIAL PRIMARY KEY,
    id_roi INTEGER NOT NULL,
    modelo_yolo_id VARCHAR(100) NOT NULL,
    modelo_yolo_versao VARCHAR(20),
    tipo_deteccao VARCHAR(50),
    numero_objetos_detectados INTEGER DEFAULT 0,
    numero_paineis_solares_detectados INTEGER DEFAULT 0,
    confianca_media FLOAT CHECK (confianca_media >= 0 AND confianca_media <= 1),
    area_coberta_percentual FLOAT,
    tempo_inferencia_ms FLOAT,
    timestamp_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deteccoes_json JSONB DEFAULT '[]',
    propriedades_calculadas JSONB DEFAULT '{}',
    sucesso BOOLEAN DEFAULT TRUE,
    mensagem_erro VARCHAR(500),
    CONSTRAINT fk_roi FOREIGN KEY (id_roi) REFERENCES telhado_rois(id_roi) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS telhado_modelos_yolo (
    id_modelo SERIAL PRIMARY KEY,
    modelo_id VARCHAR(100) NOT NULL UNIQUE,
    nome_modelo VARCHAR(200) NOT NULL,
    descricao TEXT,
    caminho_arquivo VARCHAR(500) NOT NULL,
    url_download VARCHAR(500),
    tipo_deteccao VARCHAR(50) NOT NULL,
    versao VARCHAR(20) NOT NULL,
    framework VARCHAR(50) DEFAULT 'ultralytics',
    map50 FLOAT,
    map75 FLOAT,
    f1_score FLOAT,
    precisao FLOAT,
    recall FLOAT,
    ativo BOOLEAN DEFAULT TRUE,
    timestamp_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metricas_json JSONB DEFAULT '{}',
    notas TEXT
);

CREATE TABLE IF NOT EXISTS telhado_processamento_lotes (
    id_lote SERIAL PRIMARY KEY,
    identificador_lote VARCHAR(100) UNIQUE,
    timestamp_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_fim TIMESTAMP,
    tempo_total_segundos FLOAT,
    subestacoes_processadas INTEGER DEFAULT 0,
    subestacoes_com_sucesso INTEGER DEFAULT 0,
    subestacoes_com_erro INTEGER DEFAULT 0,
    telhados_detectados_total INTEGER DEFAULT 0,
    telhados_segmentados_total INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'em_processamento',
    taxa_sucesso_percentual FLOAT,
    parametros_json JSONB DEFAULT '{}',
    erros_json JSONB DEFAULT '[]',
    CONSTRAINT chk_status CHECK (status IN ('em_processamento', 'concluido', 'erro'))
);

CREATE TABLE IF NOT EXISTS telhado_cache_segmentacao (
    id_cache SERIAL PRIMARY KEY,
    hash_requisicao VARCHAR(64) NOT NULL UNIQUE,
    url_imagem VARCHAR(500),
    parametros_json JSONB,
    resultado_json JSONB NOT NULL,
    timestamp_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_expiracao TIMESTAMP,
    valido BOOLEAN DEFAULT TRUE,
    numero_acessos INTEGER DEFAULT 0,
    CONSTRAINT chk_ttl CHECK (timestamp_expiracao > timestamp_criacao)
);

CREATE TABLE IF NOT EXISTS telhado_estatisticas_diarias (
    id_stat SERIAL PRIMARY KEY,
    data_dia DATE NOT NULL UNIQUE,
    total_subestacoes_processadas INTEGER DEFAULT 0,
    total_telhados_detectados INTEGER DEFAULT 0,
    total_telhados_segmentados INTEGER DEFAULT 0,
    total_imagens_processadas INTEGER DEFAULT 0,
    total_rois_extraidas INTEGER DEFAULT 0,
    media_telhados_por_subestacao FLOAT,
    media_confianca_deteccao FLOAT,
    media_indice_qualidade FLOAT,
    media_area_telhado_m2 FLOAT,
    tempo_medio_processamento_segundos FLOAT,
    tempo_total_processamento_segundos FLOAT,
    taxa_sucesso_percentual FLOAT,
    numero_erros INTEGER DEFAULT 0,
    timestamp_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices telhados
CREATE INDEX idx_telhado_deteccoes_subestacao ON telhado_deteccoes(id_subestacao);
CREATE INDEX idx_telhado_deteccoes_imagem ON telhado_deteccoes(id_imagem_satelite);
CREATE INDEX idx_telhado_deteccoes_timestamp ON telhado_deteccoes(timestamp_deteccao DESC);
CREATE INDEX idx_telhado_deteccoes_confianca ON telhado_deteccoes(confianca_deteccao DESC);
CREATE INDEX idx_telhado_deteccoes_tipo_edificio ON telhado_deteccoes(tipo_edificio);
CREATE INDEX idx_telhado_deteccoes_geo ON telhado_deteccoes(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX idx_telhado_rois_deteccao ON telhado_rois(id_deteccao);
CREATE INDEX idx_telhado_rois_processada ON telhado_rois(processada);
CREATE INDEX idx_telhado_rois_hash ON telhado_rois(hash_arquivo);
CREATE INDEX idx_yolo_processamento_roi ON telhado_processamento_yolo(id_roi);
CREATE INDEX idx_yolo_processamento_modelo ON telhado_processamento_yolo(modelo_yolo_id);
CREATE INDEX idx_yolo_processamento_tipo ON telhado_processamento_yolo(tipo_deteccao);
CREATE INDEX idx_yolo_processamento_timestamp ON telhado_processamento_yolo(timestamp_processamento DESC);
CREATE INDEX idx_yolo_modelo_id ON telhado_modelos_yolo(modelo_id);
CREATE INDEX idx_yolo_tipo_deteccao ON telhado_modelos_yolo(tipo_deteccao);
CREATE INDEX idx_yolo_ativo ON telhado_modelos_yolo(ativo);
CREATE INDEX idx_lote_timestamp_inicio ON telhado_processamento_lotes(timestamp_inicio DESC);
CREATE INDEX idx_lote_status ON telhado_processamento_lotes(status);
CREATE INDEX idx_cache_hash ON telhado_cache_segmentacao(hash_requisicao);
CREATE INDEX idx_cache_expiracao ON telhado_cache_segmentacao(timestamp_expiracao) WHERE valido = TRUE;
CREATE INDEX idx_stats_data ON telhado_estatisticas_diarias(data_dia DESC);

-- ============================================================================
-- PARTE 4: TRANSFORMADORES E CONSUMIDORES (004_area_cobertura_real.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS transformadores (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    subestacao_id INTEGER NOT NULL,
    nome VARCHAR(200),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    potencia_kva DECIMAL(10, 2) NOT NULL,
    tipo VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ativo',
    tensao_primaria_kv DECIMAL(10, 2),
    tensao_secundaria_v INTEGER,
    fabricante VARCHAR(100),
    ano_instalacao INTEGER,
    data_ultima_inspecao DATE,
    observacoes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    osm_id BIGINT,
    fonte_dados VARCHAR(50) DEFAULT 'manual',
    area_poligonal_km DECIMAL(10, 2) DEFAULT 1.0,
    FOREIGN KEY (subestacao_id) REFERENCES subestacoes_detectadas(id)
);

CREATE TABLE IF NOT EXISTS consumidores (
    id SERIAL PRIMARY KEY,
    codigo_cliente VARCHAR(50) UNIQUE NOT NULL,
    transformador_id INTEGER NOT NULL,
    nome VARCHAR(200),
    documento VARCHAR(20),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    tipo_cliente VARCHAR(50),
    classe_consumo VARCHAR(50),
    grupo_tarifario VARCHAR(10),
    demanda_contratada_kw DECIMAL(10, 2),
    consumo_medio_mensal_kwh DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'ativo',
    endereco TEXT,
    cep VARCHAR(10),
    cidade VARCHAR(100),
    estado VARCHAR(2),
    data_ligacao DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    codigo_unidade_consumidora VARCHAR(50),
    medidor_numero VARCHAR(50),
    fonte_dados VARCHAR(50) DEFAULT 'manual',
    FOREIGN KEY (transformador_id) REFERENCES transformadores(id)
);

CREATE TABLE IF NOT EXISTS subestacoes_area_cobertura (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL,
    area_cobertura GEOMETRY(Polygon, 4326),
    metodo_definicao VARCHAR(100) NOT NULL,
    area_km2 DECIMAL(10, 2),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    observacoes TEXT,
    fonte_dados VARCHAR(50) DEFAULT 'calculado',
    confiabilidade INTEGER CHECK (confiabilidade BETWEEN 1 AND 5),
    FOREIGN KEY (subestacao_id) REFERENCES subestacoes_detectadas(id)
);

CREATE TABLE IF NOT EXISTS transformadores_area_cobertura (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL UNIQUE,
    area_cobertura GEOMETRY(Polygon, 4326),
    metodo_definicao VARCHAR(100) NOT NULL,
    area_km2 DECIMAL(10, 2),
    raio_aproximado_m DECIMAL(10, 2),
    total_consumidores INTEGER,
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    observacoes TEXT,
    area_poligonal_km DECIMAL(10, 2) DEFAULT 1.0,
    FOREIGN KEY (transformador_id) REFERENCES transformadores(id)
);

-- Índices para transformadores e consumidores
CREATE INDEX IF NOT EXISTS idx_transformadores_geom ON transformadores USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_transformadores_subestacao ON transformadores(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_transformadores_osm ON transformadores(osm_id);
CREATE INDEX IF NOT EXISTS idx_transformadores_fonte ON transformadores(fonte_dados);
CREATE INDEX IF NOT EXISTS idx_transformadores_subestacao_status ON transformadores(subestacao_id, status);
CREATE INDEX IF NOT EXISTS idx_transformadores_updated ON transformadores(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_transformadores_area_poligonal ON transformadores(area_poligonal_km);
CREATE INDEX IF NOT EXISTS idx_consumidores_geom ON consumidores USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_consumidores_transformador ON consumidores(transformador_id);
CREATE INDEX IF NOT EXISTS idx_consumidores_unidade ON consumidores(codigo_unidade_consumidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_medidor ON consumidores(medidor_numero);
CREATE INDEX IF NOT EXISTS idx_consumidores_fonte ON consumidores(fonte_dados);
CREATE INDEX IF NOT EXISTS idx_consumidores_transformador_status ON consumidores(transformador_id, status);
CREATE INDEX IF NOT EXISTS idx_consumidores_updated ON consumidores(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_area_cobertura_geom ON subestacoes_area_cobertura USING GIST(area_cobertura);
CREATE INDEX IF NOT EXISTS idx_transformador_area_geom ON transformadores_area_cobertura USING GIST(area_cobertura);
CREATE INDEX IF NOT EXISTS idx_transformador_area_id ON transformadores_area_cobertura(transformador_id);

-- ============================================================================
-- PARTE 5: USINAS DE GERAÇÃO (005_schema_dados_reais.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS usinas_geracao (
    id SERIAL PRIMARY KEY,
    codigo_ceg VARCHAR(50) UNIQUE,
    nome VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(11, 7) NOT NULL,
    localizacao GEOMETRY(Point, 4326),
    tipo_geracao VARCHAR(50),
    fonte_energia VARCHAR(100),
    combustivel VARCHAR(100),
    potencia_outorgada_kw DECIMAL(12, 2),
    potencia_fiscalizada_kw DECIMAL(12, 2),
    municipio VARCHAR(100),
    estado VARCHAR(2),
    bacia_hidrografica VARCHAR(100),
    situacao VARCHAR(50),
    data_operacao DATE,
    proprietario VARCHAR(200),
    cpf_cnpj VARCHAR(20),
    subestacao_conectada_id INTEGER,
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_ultima_atualizacao DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (subestacao_conectada_id) REFERENCES subestacoes_detectadas(id)
);

-- Índices para usinas
CREATE INDEX IF NOT EXISTS idx_usinas_geom ON usinas_geracao USING GIST(localizacao);
CREATE INDEX IF NOT EXISTS idx_usinas_tipo ON usinas_geracao(tipo_geracao);
CREATE INDEX IF NOT EXISTS idx_usinas_estado ON usinas_geracao(estado);
CREATE INDEX IF NOT EXISTS idx_usinas_situacao ON usinas_geracao(situacao);
CREATE INDEX IF NOT EXISTS idx_usinas_subestacao ON usinas_geracao(subestacao_conectada_id);

-- ============================================================================
-- PARTE 6: TRACKING DE SATÉLITES (satelite_tracking.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS requisicoes_satelite_google (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    data_requisicao TIMESTAMPTZ DEFAULT NOW(),
    ano_mes TEXT NOT NULL,
    tipo_requisicao VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'sucesso',
    bbox_min_lat DOUBLE PRECISION,
    bbox_min_lon DOUBLE PRECISION,
    bbox_max_lat DOUBLE PRECISION,
    bbox_max_lon DOUBLE PRECISION,
    observacoes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS requisicoes_satelite_cbers4a (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    transformador_id INTEGER REFERENCES transformadores(id) ON DELETE CASCADE,
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

CREATE TABLE IF NOT EXISTS quota_satelite_google_mes (
    id SERIAL PRIMARY KEY,
    ano_mes TEXT NOT NULL UNIQUE,
    total_requisicoes INTEGER DEFAULT 0,
    requisicoes_sucesso INTEGER DEFAULT 0,
    requisicoes_erro INTEGER DEFAULT 0,
    requisicoes_canceladas INTEGER DEFAULT 0,
    limite_maximo INTEGER DEFAULT 25000,
    percentual_uso DOUBLE PRECISION DEFAULT 0.0,
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    observacoes TEXT
);

CREATE TABLE IF NOT EXISTS preferencia_satelite_subestacao (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL UNIQUE REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    satelite_preferido VARCHAR(50) NOT NULL DEFAULT 'CBERS-4A',
    usar_google_maps_se_necessario BOOLEAN DEFAULT TRUE,
    raio_busca_km DOUBLE PRECISION DEFAULT 5.0,
    cobertura_nuvem_max DOUBLE PRECISION DEFAULT 30.0,
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    observacoes TEXT
);

CREATE TABLE IF NOT EXISTS satelite_requisicoes_google_maps (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    tipo_requisicao VARCHAR(50) NOT NULL DEFAULT 'transformador',
    url_satellite TEXT,
    url_hybrid TEXT,
    zoom INTEGER DEFAULT 18,
    tamanho_pixels VARCHAR(20) DEFAULT '640x640',
    data_requisicao TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'registrada',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS satelite_erros_transformador (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    tipo_busca VARCHAR(50) NOT NULL,
    erro_mensagem TEXT,
    data_erro TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Índices para tracking
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_ano_mes ON requisicoes_satelite_google(ano_mes);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_subestacao ON requisicoes_satelite_google(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_data ON requisicoes_satelite_google(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_status ON requisicoes_satelite_google(status);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_subestacao ON requisicoes_satelite_cbers4a(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_data ON requisicoes_satelite_cbers4a(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_status ON requisicoes_satelite_cbers4a(status);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers4a_transformador_id ON requisicoes_satelite_cbers4a(transformador_id);
CREATE INDEX IF NOT EXISTS idx_quota_ano_mes ON quota_satelite_google_mes(ano_mes);
CREATE INDEX IF NOT EXISTS idx_preferencia_subestacao ON preferencia_satelite_subestacao(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_gmaps_req_transformador ON satelite_requisicoes_google_maps(transformador_id);
CREATE INDEX IF NOT EXISTS idx_gmaps_req_data ON satelite_requisicoes_google_maps(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_gmaps_req_mes ON satelite_requisicoes_google_maps(DATE_TRUNC('month', data_requisicao));
CREATE INDEX IF NOT EXISTS idx_erros_transformador ON satelite_erros_transformador(transformador_id);
CREATE INDEX IF NOT EXISTS idx_erros_data ON satelite_erros_transformador(data_erro);

-- ============================================================================
-- PARTE 7: TELHADOS POR TRANSFORMADOR (telhados_transformador.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS telhados_detectados_transformador (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id),
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

-- Índices telhados transformador
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_transformador ON telhados_detectados_transformador(transformador_id);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_subestacao ON telhados_detectados_transformador(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_timestamp ON telhados_detectados_transformador(timestamp_deteccao DESC);
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_confianca ON telhados_detectados_transformador(confianca DESC);

-- ============================================================================
-- PARTE 8: PAINÉIS SOLARES (migrations/create_paineis_solares_tables.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS paineis_solares_detectados (
    id SERIAL PRIMARY KEY,
    telhado_id INTEGER NOT NULL REFERENCES telhados_detectados_transformador(id),
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id),
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
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
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

-- Índices painéis solares
CREATE INDEX IF NOT EXISTS idx_paineis_telhado ON paineis_solares_detectados(telhado_id);
CREATE INDEX IF NOT EXISTS idx_paineis_transformador ON paineis_solares_detectados(transformador_id);
CREATE INDEX IF NOT EXISTS idx_paineis_subestacao ON paineis_solares_detectados(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_paineis_timestamp ON paineis_solares_detectados(timestamp_deteccao DESC);
CREATE INDEX IF NOT EXISTS idx_paineis_confianca ON paineis_solares_detectados(confianca DESC);
CREATE INDEX IF NOT EXISTS idx_potencia_transformador ON potencia_telhados(transformador_id);
CREATE INDEX IF NOT EXISTS idx_potencia_timestamp ON potencia_telhados(timestamp_atualizacao DESC);

-- ============================================================================
-- PARTE 9: LOG DE ETL (005_schema_dados_reais.sql)
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_execucao_log (
    id SERIAL PRIMARY KEY,
    tipo_etl VARCHAR(50) NOT NULL,
    fonte_dados VARCHAR(50) NOT NULL,
    data_execucao TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) NOT NULL,
    registros_extraidos INTEGER,
    registros_inseridos INTEGER,
    registros_atualizados INTEGER,
    registros_falhados INTEGER,
    duracao_segundos DECIMAL(10, 2),
    mensagem TEXT,
    erro TEXT,
    usuario VARCHAR(100),
    host VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_etl_log_tipo ON etl_execucao_log(tipo_etl);
CREATE INDEX IF NOT EXISTS idx_etl_log_data ON etl_execucao_log(data_execucao DESC);
CREATE INDEX IF NOT EXISTS idx_etl_log_status ON etl_execucao_log(status);

-- ============================================================================
-- PARTE 10: VIEWS
-- ============================================================================

-- View: Últimas imagens por subestação (satélite)
CREATE OR REPLACE VIEW v_satelite_ultimas_imagens AS
SELECT 
    sd.id as subestacao_id,
    sd.nome as subestacao_nome,
    si.sensor,
    si.data_aquisicao,
    si.resolucao_m,
    si.cobertura_nuvem_pct,
    si.url,
    ROW_NUMBER() OVER (PARTITION BY sd.id, si.sensor ORDER BY si.data_aquisicao DESC) as rank
FROM subestacoes_detectadas sd
LEFT JOIN satelite_imagens si ON sd.id = si.subestacao_id
WHERE rank = 1;

-- View: Resumo de cobertura por subestação (satélite)
CREATE OR REPLACE VIEW v_satelite_resumo_cobertura AS
SELECT 
    sd.id as subestacao_id,
    sd.nome as subestacao_nome,
    COUNT(DISTINCT si.sensor) as sensores_disponiveis,
    COUNT(*) as total_imagens,
    MIN(si.data_aquisicao) as primeira_imagem,
    MAX(si.data_aquisicao) as ultima_imagem,
    ROUND(AVG(si.cobertura_nuvem_pct)::numeric, 2) as media_nuvem_pct,
    ROUND((COUNT(*) FILTER (WHERE si.cobertura_nuvem_pct < 20))::numeric / COUNT(*) * 100, 2) as pct_baixa_nuvem
FROM subestacoes_detectadas sd
LEFT JOIN satelite_imagens si ON sd.id = si.subestacao_id
GROUP BY sd.id, sd.nome;

-- View: Últimos telhados processados
CREATE OR REPLACE VIEW v_telhado_ultimos_processados AS
SELECT 
    td.id_deteccao,
    td.id_telhado,
    td.id_subestacao,
    td.tipo_edificio,
    td.confianca_deteccao,
    td.area_m2,
    td.timestamp_deteccao,
    COUNT(tr.id_roi) as numero_rois,
    SUM(CASE WHEN tr.processada THEN 1 ELSE 0 END) as rois_processadas
FROM telhado_deteccoes td
LEFT JOIN telhado_rois tr ON td.id_deteccao = tr.id_deteccao
WHERE td.ativo = TRUE
GROUP BY td.id_deteccao, td.id_telhado, td.id_subestacao, td.tipo_edificio, 
         td.confianca_deteccao, td.area_m2, td.timestamp_deteccao
ORDER BY td.timestamp_deteccao DESC
LIMIT 1000;

-- View: Telhados com painéis solares detectados
CREATE OR REPLACE VIEW v_telhado_com_paineis_solares AS
SELECT 
    td.id_telhado,
    td.id_subestacao,
    td.area_m2,
    ty.numero_paineis_solares_detectados,
    ty.area_coberta_percentual,
    ty.confianca_media,
    ty.propriedades_calculadas->>'potencial_mw' as potencial_mw_estimado,
    ty.timestamp_processamento
FROM telhado_deteccoes td
INNER JOIN telhado_rois tr ON td.id_deteccao = tr.id_deteccao
INNER JOIN telhado_processamento_yolo ty ON tr.id_roi = ty.id_roi
WHERE ty.numero_paineis_solares_detectados > 0
  AND ty.sucesso = TRUE
ORDER BY ty.numero_paineis_solares_detectados DESC;

-- View: Resumo por subestação (telhados)
CREATE OR REPLACE VIEW v_telhado_resumo_subestacao AS
SELECT 
    s.id,
    s.nome as nome_subestacao,
    COUNT(DISTINCT td.id_deteccao) as total_telhados,
    SUM(CASE WHEN td.tipo_edificio = 'residencial' THEN 1 ELSE 0 END) as telhados_residenciais,
    SUM(CASE WHEN td.tipo_edificio = 'comercial' THEN 1 ELSE 0 END) as telhados_comerciais,
    SUM(CASE WHEN td.tipo_edificio = 'industrial' THEN 1 ELSE 0 END) as telhados_industriais,
    AVG(td.confianca_deteccao) as confianca_media,
    SUM(td.area_m2) as area_total_m2,
    MAX(td.timestamp_deteccao) as ultima_deteccao
FROM subestacoes_detectadas s
LEFT JOIN telhado_deteccoes td ON s.id = td.id_subestacao AND td.ativo = TRUE
GROUP BY s.id, s.nome;

-- View: Telhados por transformador
CREATE OR REPLACE VIEW v_telhados_por_transformador AS
SELECT 
    transformador_id,
    subestacao_id,
    COUNT(*) as total_telhados,
    SUM(area_m2) as area_total_m2,
    AVG(area_m2) as area_media_m2,
    MIN(confianca) as confianca_minima,
    AVG(confianca) as confianca_media,
    MAX(confianca) as confianca_maxima,
    COUNT(DISTINCT fonte_imagem) as fontes_utilizadas,
    MAX(timestamp_deteccao) as ultima_deteccao
FROM telhados_detectados_transformador
GROUP BY transformador_id, subestacao_id;

-- View: Telhados por subestação
CREATE OR REPLACE VIEW v_telhados_por_subestacao AS
SELECT 
    subestacao_id,
    COUNT(DISTINCT transformador_id) as transformadores_com_telhados,
    COUNT(DISTINCT id) as total_telhados,
    SUM(area_m2) as area_total_m2,
    AVG(area_m2) as area_media_m2,
    AVG(confianca) as confianca_media,
    MAX(timestamp_deteccao) as ultima_atualizacao
FROM telhados_detectados_transformador
GROUP BY subestacao_id;

-- View: Potência por transformador (painéis solares)
CREATE OR REPLACE VIEW v_potencia_por_transformador AS
SELECT 
    t.transformador_id,
    t.id as telhado_id,
    COUNT(DISTINCT ps.id) as total_paineis,
    SUM(ps.area_m2) as area_total_m2,
    SUM(COALESCE(ps.potencia_w, 0)) / 1000 as potencia_instalada_kw,
    SUM(COALESCE(ps.potencia_w, 0)) / 1000 * 4.5 / 1000 as producao_diaria_kwh,
    SUM(COALESCE(ps.potencia_w, 0)) / 1000 * 4.5 * 365 / 1000 as producao_anual_kwh,
    SUM(COALESCE(ps.potencia_w, 0)) / 1000 * 4.5 * 365 / 1000 * 0.80 as economia_anual_brl
FROM telhados_detectados_transformador t
LEFT JOIN paineis_solares_detectados ps ON t.id = ps.telhado_id
GROUP BY t.transformador_id, t.id;

-- View: Requisições Google Maps por mês
CREATE OR REPLACE VIEW view_quota_google_mes AS
SELECT 
    DATE_TRUNC('month', data_requisicao)::TEXT as ano_mes,
    COUNT(*) as total_requisicoes,
    SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucesso,
    SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as erro,
    SUM(CASE WHEN status = 'cancelado' THEN 1 ELSE 0 END) as cancelado,
    ROUND(100.0 * COUNT(*) / 25000, 2) as percentual_limite
FROM requisicoes_satelite_google
GROUP BY DATE_TRUNC('month', data_requisicao)
ORDER BY ano_mes DESC;

-- View: CBERS-4A por subestação
CREATE OR REPLACE VIEW view_cbers4a_por_subestacao AS
SELECT 
    sd.id,
    sd.nome,
    COUNT(*) as total_imagens,
    SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as imagens_sucesso,
    MIN(cobertura_nuvem_percentual) as min_nuvem_percentual,
    MAX(data_imagem) as imagem_mais_recente,
    SUM(CASE WHEN status = 'sem_cobertura' THEN 1 ELSE 0 END) as sem_cobertura
FROM subestacoes_detectadas sd
LEFT JOIN requisicoes_satelite_cbers4a rcb ON sd.id = rcb.subestacao_id
GROUP BY sd.id, sd.nome
ORDER BY total_imagens DESC;

-- View: Status geral de requisições
CREATE OR REPLACE VIEW view_status_requisicoes_satelite AS
SELECT 
    'Google Maps' as satelite,
    DATE_TRUNC('month', NOW())::TEXT as mes_atual,
    COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_google 
              WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM')), 0) as requisicoes_mes,
    25000 as limite_mes,
    ROUND(100.0 * COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_google 
                           WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM')), 0) / 25000, 2) as percentual_uso
UNION ALL
SELECT 
    'CBERS-4A' as satelite,
    DATE_TRUNC('month', NOW())::TEXT as mes_atual,
    COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_cbers4a 
              WHERE DATE_TRUNC('month', data_requisicao) = DATE_TRUNC('month', NOW())), 0) as requisicoes_mes,
    NULL as limite_mes,
    NULL as percentual_uso;

-- View: Quota mensal do Google Maps
CREATE OR REPLACE VIEW v_google_maps_quota_mensal AS
SELECT 
    DATE_TRUNC('month', data_requisicao)::date as mes,
    COUNT(*) as requisicoes,
    COUNT(DISTINCT transformador_id) as transformadores_unicos,
    25000 - COUNT(*) as quota_disponivel,
    ROUND(100.0 * COUNT(*) / 25000, 2) as percentual_uso,
    MIN(data_requisicao) as primeira_requisicao,
    MAX(data_requisicao) as ultima_requisicao
FROM satelite_requisicoes_google_maps
GROUP BY DATE_TRUNC('month', data_requisicao)
ORDER BY mes DESC;

-- View: Histórico diário
CREATE OR REPLACE VIEW v_google_maps_historico_diario AS
SELECT 
    DATE_TRUNC('day', data_requisicao)::date as dia,
    COUNT(*) as requisicoes,
    COUNT(DISTINCT transformador_id) as transformadores,
    AVG(zoom) as zoom_medio
FROM satelite_requisicoes_google_maps
GROUP BY DATE_TRUNC('day', data_requisicao)
ORDER BY dia DESC;

-- View: Estatísticas de erros
CREATE OR REPLACE VIEW v_google_maps_erros AS
SELECT 
    DATE_TRUNC('day', data_erro)::date as dia,
    tipo_busca,
    COUNT(*) as total_erros,
    COUNT(DISTINCT transformador_id) as transformadores_afetados
FROM satelite_erros_transformador
GROUP BY DATE_TRUNC('day', data_erro), tipo_busca
ORDER BY dia DESC, total_erros DESC;

-- View: Qualidade dos dados
CREATE OR REPLACE VIEW vw_qualidade_dados AS
SELECT 
    'subestacoes' as tabela,
    fonte_dados,
    COUNT(*) as total_registros,
    COUNT(CASE WHEN localizacao IS NULL AND geom IS NULL THEN 1 END) as sem_coordenadas,
    COUNT(CASE WHEN nome IS NULL OR nome = '' THEN 1 END) as sem_nome,
    ROUND(
        100.0 * COUNT(CASE WHEN (localizacao IS NOT NULL OR geom IS NOT NULL) AND nome IS NOT NULL THEN 1 END) / COUNT(*),
        2
    ) as qualidade_percentual
FROM subestacoes_detectadas
GROUP BY fonte_dados

UNION ALL

SELECT 
    'transformadores' as tabela,
    fonte_dados,
    COUNT(*) as total_registros,
    COUNT(CASE WHEN localizacao IS NULL THEN 1 END) as sem_coordenadas,
    COUNT(CASE WHEN nome IS NULL OR nome = '' THEN 1 END) as sem_nome,
    ROUND(
        100.0 * COUNT(CASE WHEN localizacao IS NOT NULL AND status = 'ativo' THEN 1 END) / COUNT(*),
        2
    ) as qualidade_percentual
FROM transformadores
GROUP BY fonte_dados

UNION ALL

SELECT 
    'usinas' as tabela,
    fonte_dados,
    COUNT(*) as total_registros,
    COUNT(CASE WHEN localizacao IS NULL THEN 1 END) as sem_coordenadas,
    COUNT(CASE WHEN nome IS NULL OR nome = '' THEN 1 END) as sem_nome,
    ROUND(
        100.0 * COUNT(CASE WHEN localizacao IS NOT NULL AND situacao = 'Operação' THEN 1 END) / COUNT(*),
        2
    ) as qualidade_percentual
FROM usinas_geracao
GROUP BY fonte_dados

UNION ALL

SELECT 
    'consumidores' as tabela,
    fonte_dados,
    COUNT(*) as total_registros,
    COUNT(CASE WHEN localizacao IS NULL THEN 1 END) as sem_coordenadas,
    COUNT(CASE WHEN nome IS NULL OR nome = '' THEN 1 END) as sem_nome,
    ROUND(
        100.0 * COUNT(CASE WHEN localizacao IS NOT NULL AND status = 'ativo' THEN 1 END) / COUNT(*),
        2
    ) as qualidade_percentual
FROM consumidores
GROUP BY fonte_dados;

-- View: Cobertura por subestação
CREATE OR REPLACE VIEW vw_cobertura_subestacoes AS
SELECT 
    se.id,
    se.nome,
    se.fonte_dados as fonte_subestacao,
    se.subsistema,
    se.latitude,
    se.longitude,
    ac.area_km2,
    ac.metodo_definicao as metodo_area,
    ac.confiabilidade,
    ac.fonte_dados as fonte_area,
    COUNT(DISTINCT t.id) as total_transformadores,
    COUNT(DISTINCT CASE WHEN t.status = 'ativo' THEN t.id END) as transformadores_ativos,
    SUM(t.potencia_kva) as potencia_total_kva,
    COUNT(DISTINCT c.id) as total_consumidores,
    COUNT(DISTINCT CASE WHEN c.status = 'ativo' THEN c.id END) as consumidores_ativos,
    SUM(c.consumo_medio_mensal_kwh) as consumo_total_kwh,
    COUNT(DISTINCT u.id) as usinas_conectadas,
    SUM(u.potencia_outorgada_kw) as geracao_distribuida_kw,
    ac.data_atualizacao as ultima_atualizacao
FROM subestacoes_detectadas se
LEFT JOIN subestacoes_area_cobertura ac ON ac.subestacao_id = se.id
LEFT JOIN transformadores t ON t.subestacao_id = se.id
LEFT JOIN consumidores c ON c.transformador_id = t.id
LEFT JOIN usinas_geracao u ON u.subestacao_conectada_id = se.id
GROUP BY 
    se.id, se.nome, se.fonte_dados, se.subsistema, se.latitude, se.longitude,
    ac.area_km2, ac.metodo_definicao, ac.confiabilidade, ac.fonte_dados, ac.data_atualizacao
ORDER BY se.nome;

-- ============================================================================
-- PARTE 11: FUNÇÕES
-- ============================================================================

-- Função: Limpar cache expirado (satélite)
CREATE OR REPLACE FUNCTION satelite_limpar_cache_expirado()
RETURNS void AS $$
BEGIN
    DELETE FROM satelite_cache_stac 
    WHERE validade_ate < NOW();
    
    RAISE NOTICE 'Cache expirado removido com sucesso';
END;
$$ LANGUAGE plpgsql;

-- Função: Atualizar estatísticas (satélite)
CREATE OR REPLACE FUNCTION satelite_atualizar_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO satelite_cobertura_stats 
    (subestacao_id, sensor, mes_ano, media_nuvem_pct, total_cenas, cenas_baixa_nuvem)
    SELECT 
        NEW.subestacao_id,
        NEW.sensor,
        DATE_TRUNC('month', NEW.data_aquisicao)::date,
        AVG(si.cobertura_nuvem_pct),
        COUNT(*),
        COUNT(*) FILTER (WHERE si.cobertura_nuvem_pct < 20)
    FROM satelite_imagens si
    WHERE si.subestacao_id = NEW.subestacao_id 
      AND si.sensor = NEW.sensor
      AND DATE_TRUNC('month', si.data_aquisicao) = DATE_TRUNC('month', NEW.data_aquisicao)
    GROUP BY NEW.subestacao_id, NEW.sensor, DATE_TRUNC('month', NEW.data_aquisicao)
    ON CONFLICT (subestacao_id, sensor, mes_ano) DO UPDATE
    SET 
        media_nuvem_pct = EXCLUDED.media_nuvem_pct,
        total_cenas = EXCLUDED.total_cenas,
        cenas_baixa_nuvem = EXCLUDED.cenas_baixa_nuvem,
        data_atualizacao = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Função: Manutenção periódica (satélite)
CREATE OR REPLACE FUNCTION satelite_manutencao_periodica()
RETURNS void AS $$
BEGIN
    PERFORM satelite_limpar_cache_expirado();
    RAISE NOTICE 'Manutenção de satélite completada';
END;
$$ LANGUAGE plpgsql;

-- Função: Limpar cache expirado (telhados)
CREATE OR REPLACE FUNCTION telhado_limpar_cache_expirado()
RETURNS void AS $$
BEGIN
    UPDATE telhado_cache_segmentacao 
    SET valido = FALSE 
    WHERE timestamp_expiracao < CURRENT_TIMESTAMP 
      AND valido = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Função: Calcular potencial solar por subestação
CREATE OR REPLACE FUNCTION telhado_potencial_solar_subestacao(p_id_subestacao INTEGER)
RETURNS TABLE (
    tipo_edificio VARCHAR,
    numero_telhados INTEGER,
    area_total_m2 FLOAT,
    telhados_com_paineis INTEGER,
    potencial_mw_estimado FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        td.tipo_edificio,
        COUNT(DISTINCT td.id_deteccao)::INTEGER,
        SUM(td.area_m2)::FLOAT,
        COUNT(DISTINCT CASE WHEN ty.numero_paineis_solares_detectados > 0 THEN td.id_deteccao END)::INTEGER,
        SUM(CAST(ty.propriedades_calculadas->>'potencial_mw' AS FLOAT))::FLOAT
    FROM telhado_deteccoes td
    LEFT JOIN telhado_rois tr ON td.id_deteccao = tr.id_deteccao
    LEFT JOIN telhado_processamento_yolo ty ON tr.id_roi = ty.id_roi
    WHERE td.id_subestacao = p_id_subestacao AND td.ativo = TRUE
    GROUP BY td.tipo_edificio;
END;
$$ LANGUAGE plpgsql;

-- Função: Verificar se pode usar Google Maps este mês
CREATE OR REPLACE FUNCTION pode_usar_google_maps_este_mes()
RETURNS TABLE(pode_usar BOOLEAN, requisicoes_usadas INTEGER, limite INTEGER, percentual_uso DOUBLE PRECISION) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_google 
                 WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM') AND status = 'sucesso'), 0) < 25000,
        COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_google 
                 WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM') AND status = 'sucesso'), 0)::INTEGER,
        25000,
        ROUND(100.0 * COALESCE((SELECT COUNT(*) FROM requisicoes_satelite_google 
                               WHERE ano_mes = TO_CHAR(NOW(), 'YYYY-MM') AND status = 'sucesso'), 0) / 25000, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Função: Registrar requisição Google Maps
CREATE OR REPLACE FUNCTION registrar_requisicao_google_maps(
    p_subestacao_id INTEGER,
    p_tipo_requisicao VARCHAR,
    p_status VARCHAR,
    p_bbox_min_lat DOUBLE PRECISION,
    p_bbox_min_lon DOUBLE PRECISION,
    p_bbox_max_lat DOUBLE PRECISION,
    p_bbox_max_lon DOUBLE PRECISION,
    p_observacoes TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
    v_ano_mes TEXT;
BEGIN
    v_ano_mes := TO_CHAR(NOW(), 'YYYY-MM');
    
    INSERT INTO requisicoes_satelite_google (
        subestacao_id, ano_mes, tipo_requisicao, status, 
        bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, observacoes
    ) VALUES (
        p_subestacao_id, v_ano_mes, p_tipo_requisicao, p_status,
        p_bbox_min_lat, p_bbox_min_lon, p_bbox_max_lat, p_bbox_max_lon, p_observacoes
    )
    RETURNING id INTO v_id;
    
    INSERT INTO quota_satelite_google_mes (ano_mes, total_requisicoes, requisicoes_sucesso)
    VALUES (v_ano_mes, 1, CASE WHEN p_status = 'sucesso' THEN 1 ELSE 0 END)
    ON CONFLICT (ano_mes) DO UPDATE SET
        total_requisicoes = quota_satelite_google_mes.total_requisicoes + 1,
        requisicoes_sucesso = quota_satelite_google_mes.requisicoes_sucesso + 
                              CASE WHEN p_status = 'sucesso' THEN 1 ELSE 0 END,
        data_atualizacao = NOW(),
        percentual_uso = ROUND(100.0 * (quota_satelite_google_mes.total_requisicoes + 1) / 25000, 2);
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- Função: Registrar requisição CBERS-4A
CREATE OR REPLACE FUNCTION registrar_requisicao_cbers4a(
    p_subestacao_id INTEGER,
    p_tipo_requisicao VARCHAR,
    p_status VARCHAR,
    p_data_imagem TIMESTAMPTZ,
    p_cobertura_nuvem DOUBLE PRECISION,
    p_bbox_min_lat DOUBLE PRECISION,
    p_bbox_min_lon DOUBLE PRECISION,
    p_bbox_max_lat DOUBLE PRECISION,
    p_bbox_max_lon DOUBLE PRECISION,
    p_imagem_id VARCHAR DEFAULT NULL,
    p_url_download TEXT DEFAULT NULL,
    p_tamanho_mb DOUBLE PRECISION DEFAULT NULL,
    p_observacoes TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    INSERT INTO requisicoes_satelite_cbers4a (
        subestacao_id, tipo_requisicao, status, data_imagem, cobertura_nuvem_percentual,
        bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon,
        imagem_id, url_download, tamanho_mb, observacoes
    ) VALUES (
        p_subestacao_id, p_tipo_requisicao, p_status, p_data_imagem, p_cobertura_nuvem,
        p_bbox_min_lat, p_bbox_min_lon, p_bbox_max_lat, p_bbox_max_lon,
        p_imagem_id, p_url_download, p_tamanho_mb, p_observacoes
    )
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- Função: Calcular confiabilidade da área
CREATE OR REPLACE FUNCTION calcular_confiabilidade_area(p_subestacao_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    v_metodo VARCHAR(100);
    v_fonte VARCHAR(50);
    v_total_trans INTEGER;
    v_confiabilidade INTEGER;
BEGIN
    SELECT metodo_definicao, fonte_dados
    INTO v_metodo, v_fonte
    FROM subestacoes_area_cobertura
    WHERE subestacao_id = p_subestacao_id;
    
    SELECT COUNT(*)
    INTO v_total_trans
    FROM transformadores
    WHERE subestacao_id = p_subestacao_id
      AND status = 'ativo';
    
    IF v_metodo = 'cadastro_oficial' THEN
        v_confiabilidade := 5;
    ELSIF v_fonte = 'OSM' AND v_total_trans >= 10 THEN
        v_confiabilidade := 4;
    ELSIF v_metodo = 'analise_topologica' AND v_total_trans >= 5 THEN
        v_confiabilidade := 3;
    ELSIF v_total_trans >= 3 THEN
        v_confiabilidade := 2;
    ELSE
        v_confiabilidade := 1;
    END IF;
    
    UPDATE subestacoes_area_cobertura
    SET confiabilidade = v_confiabilidade
    WHERE subestacao_id = p_subestacao_id;
    
    RETURN v_confiabilidade;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PARTE 12: TRIGGERS
-- ============================================================================

-- Trigger: Atualizar estatísticas (satélite)
DROP TRIGGER IF EXISTS trigger_satelite_stats ON satelite_imagens;
CREATE TRIGGER trigger_satelite_stats
AFTER INSERT ON satelite_imagens
FOR EACH ROW
EXECUTE FUNCTION satelite_atualizar_stats();

-- Trigger: Atualizar data_atualizacao em telhado_deteccoes
CREATE OR REPLACE FUNCTION trigger_atualizar_telhado_deteccoes()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_atualizar_telhado_deteccoes ON telhado_deteccoes;
CREATE TRIGGER trg_atualizar_telhado_deteccoes
BEFORE UPDATE ON telhado_deteccoes
FOR EACH ROW
EXECUTE FUNCTION trigger_atualizar_telhado_deteccoes();

-- Trigger: Marcar ROI como processada
CREATE OR REPLACE FUNCTION trigger_marcar_roi_processada()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE telhado_rois 
    SET processada = TRUE
    WHERE id_roi = NEW.id_roi;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_marcar_roi_processada ON telhado_processamento_yolo;
CREATE TRIGGER trg_marcar_roi_processada
AFTER INSERT ON telhado_processamento_yolo
FOR EACH ROW
EXECUTE FUNCTION trigger_marcar_roi_processada();

-- Trigger: Atualizar estatísticas diárias
CREATE OR REPLACE FUNCTION trigger_atualizar_stats_diarias()
RETURNS TRIGGER AS $$
DECLARE
    data_dia DATE := CURRENT_DATE;
BEGIN
    INSERT INTO telhado_estatisticas_diarias (
        data_dia, total_telhados_detectados, timestamp_atualizacao
    ) VALUES (data_dia, 1, CURRENT_TIMESTAMP)
    ON CONFLICT (data_dia) DO UPDATE SET
        total_telhados_detectados = total_telhados_detectados + 1,
        timestamp_atualizacao = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_stats_diarias_insert ON telhado_deteccoes;
CREATE TRIGGER trg_stats_diarias_insert
AFTER INSERT ON telhado_deteccoes
FOR EACH ROW
EXECUTE FUNCTION trigger_atualizar_stats_diarias();

-- ============================================================================
-- PARTE 13: COMENTÁRIOS
-- ============================================================================

COMMENT ON DATABASE energy_monitor IS 'Energy Netload Monitor - Dados REAIS de ONS, ANEEL e OpenStreetMap';

-- Comentários satélite
COMMENT ON TABLE satelite_imagens IS 'Armazena metadados de imagens de satélite de subestações (Sentinel-2, Landsat, MODIS, etc.)';
COMMENT ON TABLE satelite_bandas IS 'Armazena informações individuais de bandas espectrais para cada imagem (RGB, NIR, SWIR, etc.)';
COMMENT ON TABLE satelite_consultas IS 'Rastreia todas as consultas realizadas às APIs STAC e WMS para auditoria e otimização';
COMMENT ON TABLE satelite_cache_stac IS 'Cache de resultados STAC para otimizar buscas frequentes na mesma área';
COMMENT ON TABLE satelite_cobertura_stats IS 'Estatísticas mensais de cobertura e disponibilidade de imagens por sensor';
COMMENT ON COLUMN satelite_imagens.sensor IS 'Sensor/satélite: Sentinel-2, Landsat-8, Landsat-9, MODIS, etc.';
COMMENT ON COLUMN satelite_imagens.resolucao_m IS 'Resolução espacial em metros. Sentinel-2: 10-60m, Landsat: 30m, MODIS: 250-1000m';
COMMENT ON COLUMN satelite_imagens.cobertura_nuvem_pct IS 'Percentual de cobertura de nuvem (0-100). Importante para qualidade da imagem.';
COMMENT ON COLUMN satelite_imagens.propriedades_json IS 'Propriedades customizadas: tile MGRS/UTM, nível de processamento, missão, etc.';
COMMENT ON COLUMN satelite_imagens.url_google_maps_satellite IS 'URL da imagem satellite do Google Maps Static API';
COMMENT ON COLUMN satelite_imagens.url_google_maps_hybrid IS 'URL da imagem hybrid do Google Maps Static API';
COMMENT ON COLUMN satelite_bandas.numero_banda IS 'Número da banda espectral (0-4). Ex: 0=blue, 1=green, 2=red, 3=nir, 4=swir';
COMMENT ON COLUMN satelite_bandas.nome_banda IS 'Nome da banda: blue, green, red, nir (infravermelho próximo), swir (infravermelho curto)';

-- Comentários telhados
COMMENT ON TABLE telhado_deteccoes IS 'Telhados/edifícios detectados em imagens de satélite via YOLOv8';
COMMENT ON TABLE telhado_rois IS 'ROIs (imagens recortadas) de telhados para processamento posterior';
COMMENT ON TABLE telhado_processamento_yolo IS 'Resultados do processamento com modelos YOLO (painéis solares, cobertura, etc)';
COMMENT ON TABLE telhado_modelos_yolo IS 'Registro de modelos YOLO disponíveis na plataforma';
COMMENT ON COLUMN telhado_deteccoes.confianca_deteccao IS 'Confiança da detecção (0-1), do modelo YOLOv8';
COMMENT ON COLUMN telhado_deteccoes.tipo_edificio IS 'Classificação do tipo de edifício: residencial, comercial, industrial, desconhecido';
COMMENT ON COLUMN telhado_rois.url_storage_s3 IS 'URL em storage cloud (S3/Azure/GCP) para acesso remoto';
COMMENT ON COLUMN telhado_processamento_yolo.propriedades_calculadas IS 'Propriedades derivadas como potencial_mw, orientacao_media, etc';

-- Comentários transformadores
COMMENT ON COLUMN transformadores.osm_id IS 'ID do objeto no OpenStreetMap';
COMMENT ON COLUMN transformadores.fonte_dados IS 'Origem: OSM, ANEEL, SCADA, manual';
COMMENT ON COLUMN transformadores.area_poligonal_km IS 'Dimensão da área poligonal em km (bounding box) para busca de imagens - padrão 1.0 km';
COMMENT ON COLUMN transformadores_area_cobertura.area_poligonal_km IS 'Dimensão da área poligonal em km para este transformador - baseada em bbox';

-- Comentários consumidores
COMMENT ON COLUMN consumidores.codigo_unidade_consumidora IS 'Código da UC na distribuidora';
COMMENT ON COLUMN consumidores.medidor_numero IS 'Número do medidor de energia';
COMMENT ON COLUMN consumidores.fonte_dados IS 'Origem: distribuidora, ANEEL, manual';

-- Comentários áreas
COMMENT ON COLUMN subestacoes_area_cobertura.fonte_dados IS 'Origem: oficial, OSM, calculado, satelite';
COMMENT ON COLUMN subestacoes_area_cobertura.confiabilidade IS 'Nível de confiança: 1 (baixo) a 5 (alto)';

-- Comentários usinas
COMMENT ON TABLE usinas_geracao IS 'Usinas de geração distribuída (dados ANEEL SIGA)';
COMMENT ON COLUMN usinas_geracao.codigo_ceg IS 'Código CEG (Cadastro de Empreendimentos de Geração)';
COMMENT ON COLUMN usinas_geracao.tipo_geracao IS 'Sigla ANEEL: UFV, EOL, UHE, PCH, UTE, CGH';
COMMENT ON COLUMN usinas_geracao.potencia_outorgada_kw IS 'Potência autorizada pela ANEEL';
COMMENT ON COLUMN usinas_geracao.potencia_fiscalizada_kw IS 'Potência verificada em campo';

-- Comentários ETL
COMMENT ON TABLE etl_execucao_log IS 'Log de execuções do ETL para auditoria';
COMMENT ON COLUMN etl_execucao_log.tipo_etl IS 'Tipo: ons, aneel, osm, scada, satelite';

-- Comentários subestações
COMMENT ON COLUMN subestacoes_detectadas.fonte_dados IS 'Origem dos dados: ONS, ANEEL, OSM, satelite, manual';
COMMENT ON COLUMN subestacoes_detectadas.codigo_ons IS 'Código oficial da subestação no ONS';
COMMENT ON COLUMN subestacoes_detectadas.subsistema IS 'Subsistema: Norte, Nordeste, Sudeste/Centro-Oeste, Sul';

-- Comentários telhados transformador
COMMENT ON TABLE telhados_detectados_transformador IS 'Armazena telhados/edifícios detectados em imagens de transformadores individuais via YOLO';
COMMENT ON COLUMN telhados_detectados_transformador.confianca IS 'Confiança da detecção YOLO (0-1)';
COMMENT ON COLUMN telhados_detectados_transformador.fonte_imagem IS 'Fonte da imagem: google_maps (0.3m/px) ou cbers4a (2m/px)';
COMMENT ON COLUMN telhados_detectados_transformador.url_imagem_origem IS 'URL da imagem original (Google Maps grid) onde o telhado foi detectado. Use com bbox_json para cortar a região do telhado.';
COMMENT ON VIEW v_telhados_por_transformador IS 'Agregações de telhados detectados por transformador';
COMMENT ON VIEW v_telhados_por_subestacao IS 'Agregações de telhados detectados por subestação';

-- Comentários painéis solares
COMMENT ON TABLE paineis_solares_detectados IS 'Painéis solares individuais detectados por YOLO em cada telhado';
COMMENT ON TABLE potencia_telhados IS 'Resumo desnormalizado de potência e produção por telhado';
COMMENT ON COLUMN paineis_solares_detectados.bbox_json IS 'Bounding box do painel na imagem ROI: {x, y, w, h}';
COMMENT ON COLUMN paineis_solares_detectados.potencia_w IS 'Potência estimada do painel (W/m² × área)';
COMMENT ON COLUMN potencia_telhados.producao_anual_kwh IS 'Estimativa de produção anual em kWh baseada em 4.5 kWh/m²/dia (padrão Brasil)';

-- Comentários tabelas ANEEL
COMMENT ON TABLE transformadores_aneel IS 'Transformadores de distribuição da ANEEL importados via API ArcGIS';
COMMENT ON COLUMN transformadores_aneel.codigo_aneel IS 'Código único do transformador na ANEEL';
COMMENT ON COLUMN transformadores_aneel.distribuidora IS 'Nome da distribuidora (ex: RGE SUL, Equatorial, etc)';
COMMENT ON COLUMN transformadores_aneel.fonte_dados IS 'Origem: ANEEL (importação automática)';

COMMENT ON TABLE consumidores_aneel IS 'Unidades Consumidoras (UCs) da ANEEL importadas via API ArcGIS';
COMMENT ON COLUMN consumidores_aneel.codigo_uc IS 'Código da Unidade Consumidora na ANEEL';
COMMENT ON COLUMN consumidores_aneel.tipo_cliente IS 'Tipo: residencial, comercial, industrial, etc';
COMMENT ON COLUMN consumidores_aneel.distribuidora IS 'Distribuidora de energia responsável';

COMMENT ON TABLE subestacoes_aneel IS 'Subestações de distribuição da ANEEL importadas via API ArcGIS';
COMMENT ON COLUMN subestacoes_aneel.codigo_aneel IS 'Código único da subestação na ANEEL';
COMMENT ON COLUMN subestacoes_aneel.distribuidora IS 'Distribuidora responsável pela subestação';

-- ============================================================================
-- PARTE 13: TABELAS ANEEL - DISTRIBUIÇÃO
-- ============================================================================

-- Tabela de transformadores ANEEL (distribuição)
CREATE TABLE IF NOT EXISTS transformadores_aneel (
    id SERIAL PRIMARY KEY,
    codigo_aneel VARCHAR(50) UNIQUE,
    nome VARCHAR(200) NOT NULL,
    potencia_kva DECIMAL(10, 2),
    tensao_primaria_kv DECIMAL(10, 2),
    tensao_secundaria_v INTEGER,
    tipo VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ativo',
    distribuidora VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    geom GEOMETRY(Point, 4326),
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_importacao TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT transformadores_aneel_unique_codigo UNIQUE (codigo_aneel, distribuidora)
);

-- Tabela de consumidores ANEEL (UCs)
CREATE TABLE IF NOT EXISTS consumidores_aneel (
    id SERIAL PRIMARY KEY,
    codigo_uc VARCHAR(50) NOT NULL,
    nome VARCHAR(200),
    tipo_cliente VARCHAR(50),
    distribuidora VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    geom GEOMETRY(Point, 4326),
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_importacao TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT consumidores_aneel_unique_codigo UNIQUE (codigo_uc, distribuidora)
);

-- Tabela de subestações ANEEL (distribuição)
CREATE TABLE IF NOT EXISTS subestacoes_aneel (
    id SERIAL PRIMARY KEY,
    codigo_aneel VARCHAR(50) UNIQUE,
    nome VARCHAR(200) NOT NULL,
    distribuidora VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    geom GEOMETRY(Point, 4326),
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_importacao TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para tabelas ANEEL
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_geom ON transformadores_aneel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_distribuidora ON transformadores_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_status ON transformadores_aneel(status);
CREATE INDEX IF NOT EXISTS idx_transformadores_aneel_distribuida_status ON transformadores_aneel(distribuidora, status);

CREATE INDEX IF NOT EXISTS idx_consumidores_aneel_geom ON consumidores_aneel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_consumidores_aneel_distribuidora ON consumidores_aneel(distribuidora);
CREATE INDEX IF NOT EXISTS idx_consumidores_aneel_tipo ON consumidores_aneel(tipo_cliente);

CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_geom ON subestacoes_aneel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_subestacoes_aneel_distribuidora ON subestacoes_aneel(distribuidora);

-- ============================================================================
-- PARTE 14: DADOS DE INICIALIZAÇÃO
-- ============================================================================

-- Inicializar preferências de satélites (todas SEs preferem CBERS-4A)
INSERT INTO preferencia_satelite_subestacao (subestacao_id, satelite_preferido)
SELECT id, 'CBERS-4A' 
FROM subestacoes_detectadas
ON CONFLICT (subestacao_id) DO NOTHING;

-- ============================================================================
-- FIM DO SCHEMA COMPLETO
-- ============================================================================

-- Mostrar resumo
SELECT 
    'Schema completo criado com sucesso!' as status,
    NOW() as data_execucao;

SELECT 
    'Total de tabelas criadas:' as info,
    COUNT(*) as total
FROM information_schema.tables 
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';

SELECT 
    'Total de views criadas:' as info,
    COUNT(*) as total
FROM information_schema.views 
WHERE table_schema = 'public';

SELECT 
    'Total de funções criadas:' as info,
    COUNT(*) as total
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'public'
  AND p.prokind = 'f';
