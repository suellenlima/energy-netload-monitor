-- ============================================================================
-- Telhados Detectados por Transformador
-- Suporta armazenamento de detecções YOLO para transformadores individuais
-- ============================================================================

CREATE TABLE IF NOT EXISTS telhados_detectados_transformador (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id),
    
    -- Localização
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    
    -- Propriedades
    area_m2 DOUBLE PRECISION NOT NULL,
    confianca DOUBLE PRECISION NOT NULL CHECK (confianca >= 0 AND confianca <= 1),
    
    -- Bounding box em JSON
    bbox_json JSONB,
    
    -- Fonte e metadata
    fonte_imagem VARCHAR(50) DEFAULT 'google_maps',  -- 'google_maps' ou 'cbers4a'
    resolucao_cm DOUBLE PRECISION DEFAULT 30.0,
    
    -- Timestamps
    timestamp_deteccao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
    timestamp_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_telhados_trafo_transformador 
    ON telhados_detectados_transformador(transformador_id);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_subestacao 
    ON telhados_detectados_transformador(subestacao_id);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_timestamp 
    ON telhados_detectados_transformador(timestamp_deteccao DESC);

CREATE INDEX IF NOT EXISTS idx_telhados_trafo_confianca 
    ON telhados_detectados_transformador(confianca DESC);

-- ============================================================================
-- Views Agregadas
-- ============================================================================

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

-- ============================================================================
-- Comentários
-- ============================================================================

COMMENT ON TABLE telhados_detectados_transformador IS 
    'Armazena telhados/edifícios detectados em imagens de transformadores individuais via YOLO';

COMMENT ON COLUMN telhados_detectados_transformador.confianca IS 
    'Confiança da detecção YOLO (0-1)';

COMMENT ON COLUMN telhados_detectados_transformador.fonte_imagem IS 
    'Fonte da imagem: google_maps (0.3m/px) ou cbers4a (2m/px)';

COMMENT ON VIEW v_telhados_por_transformador IS 
    'Agregações de telhados detectados por transformador';

COMMENT ON VIEW v_telhados_por_subestacao IS 
    'Agregações de telhados detectados por subestação';

-- ============================================================================
-- Resultado
-- ============================================================================

SELECT 
    'Tabela criada com sucesso!' as status,
    (SELECT COUNT(*) FROM information_schema.tables 
     WHERE table_name = 'telhados_detectados_transformador') as tabela_existe;
