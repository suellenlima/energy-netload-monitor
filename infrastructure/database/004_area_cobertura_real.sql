-- Script SQL: Criação de tabelas e dados de exemplo para área de cobertura real
-- Autor: Energy Netload Monitor
-- Data: 2026-01-30

-- ============================================================================
-- 1. CRIAR TABELAS DE ÁREA DE COBERTURA
-- ============================================================================

-- Tabela de áreas de cobertura (polígonos)
CREATE TABLE IF NOT EXISTS subestacoes_area_cobertura (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL,
    area_cobertura GEOMETRY(Polygon, 4326),
    metodo_definicao VARCHAR(100) NOT NULL, -- 'cadastro_oficial', 'analise_topologica', 'aproximacao'
    area_km2 DECIMAL(10, 2),
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    observacoes TEXT,
    FOREIGN KEY (subestacao_id) REFERENCES subestacoes_detectadas(id)
);

-- Índice espacial
CREATE INDEX IF NOT EXISTS idx_area_cobertura_geom 
ON subestacoes_area_cobertura USING GIST(area_cobertura);


-- Tabela de áreas de cobertura dos transformadores
CREATE TABLE IF NOT EXISTS transformadores_area_cobertura (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL UNIQUE,
    area_cobertura GEOMETRY(Polygon, 4326),
    metodo_definicao VARCHAR(100) NOT NULL, -- 'convex_hull_consumidores', 'raio_fixo', 'calculo_topologico'
    area_km2 DECIMAL(10, 2),
    raio_aproximado_m DECIMAL(10, 2),
    total_consumidores INTEGER,
    data_atualizacao TIMESTAMP DEFAULT NOW(),
    observacoes TEXT,
    FOREIGN KEY (transformador_id) REFERENCES transformadores(id)
);

-- Índice espacial
CREATE INDEX IF NOT EXISTS idx_transformador_area_geom 
ON transformadores_area_cobertura USING GIST(area_cobertura);

-- Índice por transformador
CREATE INDEX IF NOT EXISTS idx_transformador_area_id 
ON transformadores_area_cobertura(transformador_id);


-- Tabela de transformadores de distribuição
CREATE TABLE IF NOT EXISTS transformadores (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    subestacao_id INTEGER NOT NULL,
    nome VARCHAR(200),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    potencia_kva DECIMAL(10, 2) NOT NULL,
    tipo VARCHAR(50), -- 'aereo', 'pedestal', 'subterraneo'
    status VARCHAR(20) DEFAULT 'ativo', -- 'ativo', 'inativo', 'manutencao'
    tensao_primaria_kv DECIMAL(10, 2),
    tensao_secundaria_v INTEGER,
    fabricante VARCHAR(100),
    ano_instalacao INTEGER,
    data_ultima_inspecao DATE,
    observacoes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (subestacao_id) REFERENCES subestacoes_detectadas(id)
);

-- Índice espacial
CREATE INDEX IF NOT EXISTS idx_transformadores_geom 
ON transformadores USING GIST(localizacao);

-- Índice por subestação
CREATE INDEX IF NOT EXISTS idx_transformadores_subestacao 
ON transformadores(subestacao_id);


-- Tabela de consumidores (medidores)
CREATE TABLE IF NOT EXISTS consumidores (
    id SERIAL PRIMARY KEY,
    codigo_cliente VARCHAR(50) UNIQUE NOT NULL,
    transformador_id INTEGER NOT NULL,
    nome VARCHAR(200),
    documento VARCHAR(20), -- CPF/CNPJ
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    localizacao GEOMETRY(Point, 4326),
    tipo_cliente VARCHAR(50), -- 'residencial', 'comercial', 'industrial', 'rural', 'publico'
    classe_consumo VARCHAR(50), -- 'baixa_tensao', 'media_tensao'
    grupo_tarifario VARCHAR(10), -- 'A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4'
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
    FOREIGN KEY (transformador_id) REFERENCES transformadores(id)
);

-- Índice espacial
CREATE INDEX IF NOT EXISTS idx_consumidores_geom 
ON consumidores USING GIST(localizacao);

-- Índice por transformador
CREATE INDEX IF NOT EXISTS idx_consumidores_transformador 
ON consumidores(transformador_id);


-- ============================================================================
-- 2. INSERIR DADOS DE EXEMPLO - SE BRASÍLIA SUL
-- ============================================================================

-- Inserir transformadores de exemplo ao redor da SE Brasília Sul
-- Distribuídos em um raio de 5km

INSERT INTO transformadores (codigo, subestacao_id, nome, latitude, longitude, potencia_kva, tipo, status, tensao_primaria_kv, tensao_secundaria_v) VALUES
-- Região Norte (Asa Norte)
('TR-BSB-N-001', 1, 'Transformador Asa Norte 1', -15.7800, -47.8900, 300.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-N-002', 1, 'Transformador Asa Norte 2', -15.7850, -47.8950, 225.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-N-003', 1, 'Transformador Asa Norte 3', -15.7900, -47.9000, 150.0, 'pedestal', 'ativo', 13.8, 380),
('TR-BSB-N-004', 1, 'Transformador Asa Norte 4', -15.7950, -47.9050, 300.0, 'aereo', 'ativo', 13.8, 220),

-- Região Sul (Asa Sul)
('TR-BSB-S-001', 1, 'Transformador Asa Sul 1', -15.8500, -47.9100, 300.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-S-002', 1, 'Transformador Asa Sul 2', -15.8550, -47.9150, 225.0, 'pedestal', 'ativo', 13.8, 220),
('TR-BSB-S-003', 1, 'Transformador Asa Sul 3', -15.8600, -47.9200, 300.0, 'aereo', 'ativo', 13.8, 380),
('TR-BSB-S-004', 1, 'Transformador Asa Sul 4', -15.8650, -47.9250, 150.0, 'aereo', 'ativo', 13.8, 220),

-- Região Leste (Lago Sul)
('TR-BSB-E-001', 1, 'Transformador Lago Sul 1', -15.8300, -47.8700, 450.0, 'subterraneo', 'ativo', 13.8, 380),
('TR-BSB-E-002', 1, 'Transformador Lago Sul 2', -15.8350, -47.8750, 300.0, 'pedestal', 'ativo', 13.8, 220),
('TR-BSB-E-003', 1, 'Transformador Lago Sul 3', -15.8400, -47.8800, 225.0, 'aereo', 'ativo', 13.8, 220),

-- Região Oeste (Cruzeiro)
('TR-BSB-W-001', 1, 'Transformador Cruzeiro 1', -15.8200, -47.9500, 300.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-W-002', 1, 'Transformador Cruzeiro 2', -15.8250, -47.9550, 225.0, 'pedestal', 'ativo', 13.8, 220),
('TR-BSB-W-003', 1, 'Transformador Cruzeiro 3', -15.8300, -47.9600, 300.0, 'aereo', 'ativo', 13.8, 380),

-- Região Central
('TR-BSB-C-001', 1, 'Transformador Centro 1', -15.8100, -47.9100, 450.0, 'subterraneo', 'ativo', 13.8, 380),
('TR-BSB-C-002', 1, 'Transformador Centro 2', -15.8150, -47.9150, 300.0, 'pedestal', 'ativo', 13.8, 220),
('TR-BSB-C-003', 1, 'Transformador Centro 3', -15.8200, -47.9200, 225.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-C-004', 1, 'Transformador Centro 4', -15.8250, -47.9250, 300.0, 'aereo', 'ativo', 13.8, 220),
('TR-BSB-C-005', 1, 'Transformador Centro 5', -15.8300, -47.9300, 150.0, 'pedestal', 'ativo', 13.8, 220),
('TR-BSB-C-006', 1, 'Transformador Centro 6', -15.8350, -47.9350, 300.0, 'aereo', 'ativo', 13.8, 380);

-- Atualizar geometria dos transformadores
UPDATE transformadores 
SET localizacao = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE localizacao IS NULL;


-- ============================================================================
-- 3. INSERIR CONSUMIDORES DE EXEMPLO
-- ============================================================================

-- Inserir consumidores residenciais no Transformador Asa Sul 1
INSERT INTO consumidores (codigo_cliente, transformador_id, nome, latitude, longitude, tipo_cliente, classe_consumo, grupo_tarifario, consumo_medio_mensal_kwh, status, endereco, cidade, estado) VALUES
-- Transformador TR-BSB-S-001 (Asa Sul 1)
('CLI-001', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-S-001'), 'Residência 1 - Asa Sul', -15.8505, -47.9105, 'residencial', 'baixa_tensao', 'B1', 350, 'ativo', 'SQS 308 Bloco A Apto 101', 'Brasília', 'DF'),
('CLI-002', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-S-001'), 'Residência 2 - Asa Sul', -15.8510, -47.9110, 'residencial', 'baixa_tensao', 'B1', 420, 'ativo', 'SQS 308 Bloco A Apto 201', 'Brasília', 'DF'),
('CLI-003', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-S-001'), 'Residência 3 - Asa Sul', -15.8515, -47.9115, 'residencial', 'baixa_tensao', 'B1', 280, 'ativo', 'SQS 308 Bloco B Apto 101', 'Brasília', 'DF'),
('CLI-004', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-S-001'), 'Comércio 1 - Asa Sul', -15.8495, -47.9095, 'comercial', 'baixa_tensao', 'B3', 1200, 'ativo', 'CLS 308 Loja 15', 'Brasília', 'DF'),

-- Transformador TR-BSB-N-001 (Asa Norte 1)
('CLI-005', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-N-001'), 'Residência 4 - Asa Norte', -15.7805, -47.8905, 'residencial', 'baixa_tensao', 'B1', 380, 'ativo', 'SQN 208 Bloco A Apto 301', 'Brasília', 'DF'),
('CLI-006', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-N-001'), 'Residência 5 - Asa Norte', -15.7810, -47.8910, 'residencial', 'baixa_tensao', 'B1', 310, 'ativo', 'SQN 208 Bloco B Apto 102', 'Brasília', 'DF'),
('CLI-007', (SELECT id FROM transformadores WHERE codigo = 'TR-BSB-N-001'), 'Comércio 2 - Asa Norte', -15.7795, -47.8895, 'comercial', 'baixa_tensao', 'B3', 850, 'ativo', 'CLN 208 Loja 22', 'Brasília', 'DF');

-- Atualizar geometria dos consumidores
UPDATE consumidores 
SET localizacao = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE localizacao IS NULL;


-- ============================================================================
-- 4. ESTATÍSTICAS
-- ============================================================================

-- Ver resumo dos dados inseridos
SELECT 
    'Transformadores' as tipo,
    COUNT(*) as total,
    SUM(potencia_kva) as potencia_total_kva
FROM transformadores
WHERE subestacao_id = 1

UNION ALL

SELECT 
    'Consumidores' as tipo,
    COUNT(*) as total,
    SUM(consumo_medio_mensal_kwh) as consumo_total_kwh
FROM consumidores c
JOIN transformadores t ON t.id = c.transformador_id
WHERE t.subestacao_id = 1;


-- Ver distribuição por tipo de consumidor
SELECT 
    tipo_cliente,
    COUNT(*) as quantidade,
    AVG(consumo_medio_mensal_kwh) as consumo_medio,
    SUM(consumo_medio_mensal_kwh) as consumo_total
FROM consumidores c
JOIN transformadores t ON t.id = c.transformador_id
WHERE t.subestacao_id = 1
GROUP BY tipo_cliente
ORDER BY quantidade DESC;
