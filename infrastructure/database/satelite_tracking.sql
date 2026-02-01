-- ============================================================================
-- TRACKING DE REQUISIÇÕES DE SATÉLITES
-- Controlar uso de Google Maps (limite 25k/mês) e logs CBERS-4A
-- ============================================================================

-- Tabela para rastrear requisições de Google Maps
CREATE TABLE IF NOT EXISTS requisicoes_satelite_google (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    data_requisicao TIMESTAMPTZ DEFAULT NOW(),
    ano_mes TEXT NOT NULL,  -- Formato: YYYY-MM para agregação
    tipo_requisicao VARCHAR(50) NOT NULL,  -- 'static_map', 'street_view', etc.
    status VARCHAR(20) DEFAULT 'sucesso',  -- 'sucesso', 'erro', 'cancelado'
    bbox_min_lat DOUBLE PRECISION,
    bbox_min_lon DOUBLE PRECISION,
    bbox_max_lat DOUBLE PRECISION,
    bbox_max_lon DOUBLE PRECISION,
    observacoes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_ano_mes ON requisicoes_satelite_google(ano_mes);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_subestacao ON requisicoes_satelite_google(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_data ON requisicoes_satelite_google(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_requisicoes_google_status ON requisicoes_satelite_google(status);

-- Tabela para rastrear requisições de CBERS-4A (sem limite)
CREATE TABLE IF NOT EXISTS requisicoes_satelite_cbers4a (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    data_requisicao TIMESTAMPTZ DEFAULT NOW(),
    tipo_requisicao VARCHAR(50) NOT NULL,  -- 'busca', 'download', etc.
    status VARCHAR(20) DEFAULT 'sucesso',  -- 'sucesso', 'erro', 'sem_cobertura'
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_subestacao ON requisicoes_satelite_cbers4a(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_data ON requisicoes_satelite_cbers4a(data_requisicao);
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers_status ON requisicoes_satelite_cbers4a(status);

-- Tabela agregada para monitoramento de quota (Google Maps)
CREATE TABLE IF NOT EXISTS quota_satelite_google_mes (
    id SERIAL PRIMARY KEY,
    ano_mes TEXT NOT NULL UNIQUE,  -- Formato: YYYY-MM
    total_requisicoes INTEGER DEFAULT 0,
    requisicoes_sucesso INTEGER DEFAULT 0,
    requisicoes_erro INTEGER DEFAULT 0,
    requisicoes_canceladas INTEGER DEFAULT 0,
    limite_maximo INTEGER DEFAULT 25000,
    percentual_uso DOUBLE PRECISION DEFAULT 0.0,
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    observacoes TEXT
);

CREATE INDEX IF NOT EXISTS idx_quota_ano_mes ON quota_satelite_google_mes(ano_mes);

-- Tabela de preferências de satélites por subestação
CREATE TABLE IF NOT EXISTS preferencia_satelite_subestacao (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL UNIQUE REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    satelite_preferido VARCHAR(50) NOT NULL DEFAULT 'CBERS-4A',  -- 'CBERS-4A' ou 'GOOGLE_MAPS'
    usar_google_maps_se_necessario BOOLEAN DEFAULT TRUE,
    raio_busca_km DOUBLE PRECISION DEFAULT 5.0,
    cobertura_nuvem_max DOUBLE PRECISION DEFAULT 30.0,
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    observacoes TEXT
);

CREATE INDEX IF NOT EXISTS idx_preferencia_subestacao ON preferencia_satelite_subestacao(subestacao_id);

-- ============================================================================
-- VIEWS PARA MONITORAMENTO
-- ============================================================================

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

-- ============================================================================
-- FUNÇÕES ÚTEIS
-- ============================================================================

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
    
    -- Atualizar agregado mensal
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

-- ============================================================================
-- INSERIR VALORES INICIAIS
-- ============================================================================

-- Inicializar preferências de satélites (todas SEs preferem CBERS-4A)
INSERT INTO preferencia_satelite_subestacao (subestacao_id, satelite_preferido)
SELECT id, 'CBERS-4A' 
FROM subestacoes_detectadas
ON CONFLICT (subestacao_id) DO NOTHING;

-- ============================================================================
-- GOOGLE MAPS V2 - TRANSFORMADORES (novo)
-- Rastreamento de requisições Google Maps por transformador
-- ============================================================================

-- Tabela de requisições do Google Maps para transformadores
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

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_gmaps_req_transformador 
    ON satelite_requisicoes_google_maps(transformador_id);

CREATE INDEX IF NOT EXISTS idx_gmaps_req_data 
    ON satelite_requisicoes_google_maps(data_requisicao);

CREATE INDEX IF NOT EXISTS idx_gmaps_req_mes 
    ON satelite_requisicoes_google_maps(DATE_TRUNC('month', data_requisicao));

-- Tabela de erros específica para transformadores
CREATE TABLE IF NOT EXISTS satelite_erros_transformador (
    id SERIAL PRIMARY KEY,
    transformador_id INTEGER NOT NULL REFERENCES transformadores(id),
    tipo_busca VARCHAR(50) NOT NULL,
    erro_mensagem TEXT,
    data_erro TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Índice para erros
CREATE INDEX IF NOT EXISTS idx_erros_transformador 
    ON satelite_erros_transformador(transformador_id);

CREATE INDEX IF NOT EXISTS idx_erros_data 
    ON satelite_erros_transformador(data_erro);

-- ============================================================================
-- VIEWS DE ESTATÍSTICAS GOOGLE MAPS
-- ============================================================================

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

COMMIT;
