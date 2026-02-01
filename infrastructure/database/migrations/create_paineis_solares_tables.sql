-- ============================================================================
-- Tabela: Painéis Solares Detectados
-- Data: 2026-02-01
-- Descrição: Armazena painéis solares detectados em cada telhado
-- ============================================================================

CREATE TABLE IF NOT EXISTS paineis_solares_detectados (
    id SERIAL PRIMARY KEY,
    telhado_id INTEGER NOT NULL REFERENCES telhados_detectados_transformador(id),
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id),
    
    -- Localização na imagem ROI
    bbox_json JSONB NOT NULL,  -- {x, y, w, h} em pixels
    centroide_json JSONB NOT NULL,  -- {x, y} do centroide
    
    -- Propriedades do painel
    area_pixeis INTEGER NOT NULL,
    area_m2 DOUBLE PRECISION NOT NULL,
    confianca DOUBLE PRECISION NOT NULL CHECK (confianca >= 0 AND confianca <= 1),
    
    -- Tipo e material
    tipo_painel VARCHAR(50) DEFAULT 'desconhecido',  -- monocristalino, policristalino, filme fino, desconhecido
    
    -- Estimativas
    potencia_w DOUBLE PRECISION,  -- Potência individual do painel em Watts
    
    -- Metadata
    timestamp_deteccao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_paineis_telhado 
    ON paineis_solares_detectados(telhado_id);

CREATE INDEX IF NOT EXISTS idx_paineis_transformador 
    ON paineis_solares_detectados(transformador_id);

CREATE INDEX IF NOT EXISTS idx_paineis_subestacao 
    ON paineis_solares_detectados(subestacao_id);

CREATE INDEX IF NOT EXISTS idx_paineis_timestamp 
    ON paineis_solares_detectados(timestamp_deteccao DESC);

CREATE INDEX IF NOT EXISTS idx_paineis_confianca 
    ON paineis_solares_detectados(confianca DESC);


-- ============================================================================
-- Tabela: Resumo de Potência por Telhado
-- Desnormalizado para queries rápidas
-- ============================================================================

CREATE TABLE IF NOT EXISTS potencia_telhados (
    id SERIAL PRIMARY KEY,
    telhado_id INTEGER NOT NULL UNIQUE REFERENCES telhados_detectados_transformador(id),
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    
    -- Aggregates
    num_paineis INTEGER DEFAULT 0,
    area_total_m2 DOUBLE PRECISION DEFAULT 0,
    potencia_instalada_kw DOUBLE PRECISION DEFAULT 0,
    
    -- Produção anual (Brasil)
    producao_diaria_kwh DOUBLE PRECISION DEFAULT 0,
    producao_anual_kwh DOUBLE PRECISION DEFAULT 0,
    economia_anual_brl DOUBLE PRECISION DEFAULT 0,
    
    -- Parâmetros
    potencia_por_m2 DOUBLE PRECISION DEFAULT 150.0,
    fator_capacidade DOUBLE PRECISION DEFAULT 0.15,
    insolacao_media_kwh_m2_dia DOUBLE PRECISION DEFAULT 4.5,
    tarifa_brl_kwh DOUBLE PRECISION DEFAULT 0.80,
    
    -- Metadata
    timestamp_calculo TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_potencia_transformador 
    ON potencia_telhados(transformador_id);

CREATE INDEX IF NOT EXISTS idx_potencia_timestamp 
    ON potencia_telhados(timestamp_atualizacao DESC);


-- ============================================================================
-- Views Agregadas
-- ============================================================================

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

-- Comments
COMMENT ON TABLE paineis_solares_detectados IS 'Painéis solares individuais detectados por YOLO em cada telhado';
COMMENT ON TABLE potencia_telhados IS 'Resumo desnormalizado de potência e produção por telhado';
COMMENT ON COLUMN paineis_solares_detectados.bbox_json IS 'Bounding box do painel na imagem ROI: {x, y, w, h}';
COMMENT ON COLUMN paineis_solares_detectados.potencia_w IS 'Potência estimada do painel (W/m² × área)';
COMMENT ON COLUMN potencia_telhados.producao_anual_kwh IS 'Estimativa de produção anual em kWh baseada em 4.5 kWh/m²/dia (padrão Brasil)';

-- Verificar se foi criado
SELECT table_name FROM information_schema.tables 
WHERE table_name IN ('paineis_solares_detectados', 'potencia_telhados');
