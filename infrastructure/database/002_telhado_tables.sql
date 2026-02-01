-- ============================================================================
-- SCHEMA POSTGRESQL PARA ARMAZENAMENTO DE TELHADOS DETECTADOS
-- ============================================================================
-- Armazena telhados/edifícios detectados em imagens de satélite
-- Integra com tabelas existentes: subestacoes_detectadas, satelite_imagens
-- Author: Energy Netload Monitor
-- Date: 2025-01-29

-- ============================================================================
-- 1. TABELA: telhado_deteccoes
-- ============================================================================
-- Armazena informações de telhados detectados

CREATE TABLE IF NOT EXISTS telhado_deteccoes (
    id_deteccao SERIAL PRIMARY KEY,
    id_telhado VARCHAR(100) NOT NULL UNIQUE,
    id_subestacao INTEGER,  -- FK para subestacoes_detectadas.id
    id_imagem_satelite INTEGER,  -- FK para satelite_imagens.id
    
    -- Localização na imagem (em pixels)
    bbox_x INTEGER NOT NULL,
    bbox_y INTEGER NOT NULL,
    bbox_largura INTEGER NOT NULL,
    bbox_altura INTEGER NOT NULL,
    
    -- Localização normalizada (0-1)
    bbox_x_norm FLOAT NOT NULL,
    bbox_y_norm FLOAT NOT NULL,
    bbox_w_norm FLOAT NOT NULL,
    bbox_h_norm FLOAT NOT NULL,
    
    -- Centróide
    centroide_x FLOAT NOT NULL,
    centroide_y FLOAT NOT NULL,
    
    -- Coordenadas geográficas (se disponível)
    latitude FLOAT,
    longitude FLOAT,
    
    -- Propriedades do telhado
    area_pixeis INTEGER NOT NULL,
    area_m2 FLOAT,
    confianca_deteccao FLOAT NOT NULL CHECK (confianca_deteccao >= 0 AND confianca_deteccao <= 1),
    tipo_edificio VARCHAR(50) DEFAULT 'desconhecido',  -- residencial, comercial, industrial, etc
    
    -- Qualidade
    percentual_cobertura FLOAT CHECK (percentual_cobertura >= 0 AND percentual_cobertura <= 100),
    indice_qualidade FLOAT CHECK (indice_qualidade >= 0 AND indice_qualidade <= 1),
    
    -- Segmentação
    mascara_contorno BYTEA,  -- Máscara binária em formato comprimido
    
    -- Metadados
    modelo_deteccao VARCHAR(100) DEFAULT 'yolov8n-seg',
    timestamp_deteccao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    propriedades_json JSONB DEFAULT '{}',
    
    -- Controle
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT TRUE,
    
    -- Índices
    CONSTRAINT fk_subestacao FOREIGN KEY (id_subestacao) 
        REFERENCES subestacoes_detectadas(id),
    CONSTRAINT fk_imagem_satelite FOREIGN KEY (id_imagem_satelite)
        REFERENCES satelite_imagens(id)
);

CREATE INDEX idx_telhado_deteccoes_subestacao ON telhado_deteccoes(id_subestacao);
CREATE INDEX idx_telhado_deteccoes_imagem ON telhado_deteccoes(id_imagem_satelite);
CREATE INDEX idx_telhado_deteccoes_timestamp ON telhado_deteccoes(timestamp_deteccao DESC);
CREATE INDEX idx_telhado_deteccoes_confianca ON telhado_deteccoes(confianca_deteccao DESC);
CREATE INDEX idx_telhado_deteccoes_tipo_edificio ON telhado_deteccoes(tipo_edificio);
CREATE INDEX idx_telhado_deteccoes_geo ON telhado_deteccoes(latitude, longitude) 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;


-- ============================================================================
-- 2. TABELA: telhado_rois (ROI = Region of Interest)
-- ============================================================================
-- Armazena informações de ROIs extraídas de telhados

CREATE TABLE IF NOT EXISTS telhado_rois (
    id_roi SERIAL PRIMARY KEY,
    id_deteccao INTEGER NOT NULL,
    
    -- Tamanho da ROI
    tamanho_altura INTEGER NOT NULL,
    tamanho_largura INTEGER NOT NULL,
    
    -- Resolução
    resolucao_m_por_pixel FLOAT NOT NULL,
    area_aproximada_m2 FLOAT,
    
    -- Qualidade da ROI
    percentual_cobertura FLOAT CHECK (percentual_cobertura >= 0 AND percentual_cobertura <= 100),
    indice_qualidade_roi FLOAT CHECK (indice_qualidade_roi >= 0 AND indice_qualidade_roi <= 1),
    
    -- Armazenamento
    caminho_arquivo_local VARCHAR(500),  -- Ex: /data/rois/sub_001_telhado_0_0.png
    url_storage_s3 VARCHAR(500),  -- URL no S3/Storage
    tamanho_arquivo_kb FLOAT,
    hash_arquivo VARCHAR(64),  -- SHA256 para deduplicação
    
    -- Metadados
    timestamp_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_expiracao TIMESTAMP,  -- Para cleanup automático
    processada BOOLEAN DEFAULT FALSE,  -- Se foi processada com YOLO
    
    CONSTRAINT fk_deteccao FOREIGN KEY (id_deteccao)
        REFERENCES telhado_deteccoes(id_deteccao) ON DELETE CASCADE
);

CREATE INDEX idx_telhado_rois_deteccao ON telhado_rois(id_deteccao);
CREATE INDEX idx_telhado_rois_processada ON telhado_rois(processada);
CREATE INDEX idx_telhado_rois_hash ON telhado_rois(hash_arquivo);


-- ============================================================================
-- 3. TABELA: telhado_processamento_yolo
-- ============================================================================
-- Armazena resultados do processamento com modelos YOLO

CREATE TABLE IF NOT EXISTS telhado_processamento_yolo (
    id_processamento SERIAL PRIMARY KEY,
    id_roi INTEGER NOT NULL,
    
    -- Modelo utilizado
    modelo_yolo_id VARCHAR(100) NOT NULL,  -- ID do modelo (ex: solar-panels-v1)
    modelo_yolo_versao VARCHAR(20),
    tipo_deteccao VARCHAR(50),  -- solar-panels, cobertura, etc
    
    -- Resultados
    numero_objetos_detectados INTEGER DEFAULT 0,
    numero_paineis_solares_detectados INTEGER DEFAULT 0,
    confianca_media FLOAT CHECK (confianca_media >= 0 AND confianca_media <= 1),
    area_coberta_percentual FLOAT,
    
    -- Performance
    tempo_inferencia_ms FLOAT,
    timestamp_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Detalhes das detecções (JSON)
    deteccoes_json JSONB DEFAULT '[]',  -- Array com cada detecção
    propriedades_calculadas JSONB DEFAULT '{}',  -- Ex: potencial MW, orientação, etc
    
    -- Status
    sucesso BOOLEAN DEFAULT TRUE,
    mensagem_erro VARCHAR(500),
    
    -- Índices
    CONSTRAINT fk_roi FOREIGN KEY (id_roi)
        REFERENCES telhado_rois(id_roi) ON DELETE CASCADE
);

CREATE INDEX idx_yolo_processamento_roi ON telhado_processamento_yolo(id_roi);
CREATE INDEX idx_yolo_processamento_modelo ON telhado_processamento_yolo(modelo_yolo_id);
CREATE INDEX idx_yolo_processamento_tipo ON telhado_processamento_yolo(tipo_deteccao);
CREATE INDEX idx_yolo_processamento_timestamp ON telhado_processamento_yolo(timestamp_processamento DESC);


-- ============================================================================
-- 4. TABELA: telhado_modelos_yolo_registrados
-- ============================================================================
-- Registra modelos YOLO disponíveis na plataforma

CREATE TABLE IF NOT EXISTS telhado_modelos_yolo (
    id_modelo SERIAL PRIMARY KEY,
    modelo_id VARCHAR(100) NOT NULL UNIQUE,
    nome_modelo VARCHAR(200) NOT NULL,
    descricao TEXT,
    
    -- Localização
    caminho_arquivo VARCHAR(500) NOT NULL,
    url_download VARCHAR(500),
    
    -- Modelo
    tipo_deteccao VARCHAR(50) NOT NULL,  -- solar-panels, cobertura, etc
    versao VARCHAR(20) NOT NULL,
    framework VARCHAR(50) DEFAULT 'ultralytics',  -- YOLOv8
    
    -- Métricas
    map50 FLOAT,  -- Mean Average Precision @ IoU=0.5
    map75 FLOAT,  -- Mean Average Precision @ IoU=0.75
    f1_score FLOAT,
    precisao FLOAT,
    recall FLOAT,
    
    -- Status
    ativo BOOLEAN DEFAULT TRUE,
    timestamp_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadados
    metricas_json JSONB DEFAULT '{}',
    notas TEXT
);

CREATE INDEX idx_yolo_modelo_id ON telhado_modelos_yolo(modelo_id);
CREATE INDEX idx_yolo_tipo_deteccao ON telhado_modelos_yolo(tipo_deteccao);
CREATE INDEX idx_yolo_ativo ON telhado_modelos_yolo(ativo);


-- ============================================================================
-- 5. TABELA: telhado_processamento_lotes
-- ============================================================================
-- Armazena histórico de processamentos em lote

CREATE TABLE IF NOT EXISTS telhado_processamento_lotes (
    id_lote SERIAL PRIMARY KEY,
    
    -- Identificação
    identificador_lote VARCHAR(100) UNIQUE,
    
    -- Timing
    timestamp_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_fim TIMESTAMP,
    tempo_total_segundos FLOAT,
    
    -- Estatísticas
    subestacoes_processadas INTEGER DEFAULT 0,
    subestacoes_com_sucesso INTEGER DEFAULT 0,
    subestacoes_com_erro INTEGER DEFAULT 0,
    telhados_detectados_total INTEGER DEFAULT 0,
    telhados_segmentados_total INTEGER DEFAULT 0,
    
    -- Status
    status VARCHAR(50) DEFAULT 'em_processamento',  -- em_processamento, concluido, erro
    taxa_sucesso_percentual FLOAT,
    
    -- Detalhes
    parametros_json JSONB DEFAULT '{}',
    erros_json JSONB DEFAULT '[]',
    
    -- Índices
    CONSTRAINT chk_status CHECK (status IN ('em_processamento', 'concluido', 'erro'))
);

CREATE INDEX idx_lote_timestamp_inicio ON telhado_processamento_lotes(timestamp_inicio DESC);
CREATE INDEX idx_lote_status ON telhado_processamento_lotes(status);


-- ============================================================================
-- 6. TABELA: telhado_cache_segmentacao
-- ============================================================================
-- Cache para evitar reprocessamento de mesmas imagens

CREATE TABLE IF NOT EXISTS telhado_cache_segmentacao (
    id_cache SERIAL PRIMARY KEY,
    
    -- Chave única (hash da URL + parâmetros)
    hash_requisicao VARCHAR(64) NOT NULL UNIQUE,
    
    -- Requisição original
    url_imagem VARCHAR(500),
    parametros_json JSONB,
    
    -- Resultado em cache
    resultado_json JSONB NOT NULL,
    
    -- TTL e controle
    timestamp_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_expiracao TIMESTAMP,
    valido BOOLEAN DEFAULT TRUE,
    numero_acessos INTEGER DEFAULT 0,
    
    CONSTRAINT chk_ttl CHECK (timestamp_expiracao > timestamp_criacao)
);

CREATE INDEX idx_cache_hash ON telhado_cache_segmentacao(hash_requisicao);
CREATE INDEX idx_cache_expiracao ON telhado_cache_segmentacao(timestamp_expiracao)
    WHERE valido = TRUE;


-- ============================================================================
-- 7. TABELA: telhado_estatisticas_diarias
-- ============================================================================
-- Estatísticas agregadas por dia (para análise de performance)

CREATE TABLE IF NOT EXISTS telhado_estatisticas_diarias (
    id_stat SERIAL PRIMARY KEY,
    data_dia DATE NOT NULL UNIQUE,
    
    -- Contagem
    total_subestacoes_processadas INTEGER DEFAULT 0,
    total_telhados_detectados INTEGER DEFAULT 0,
    total_telhados_segmentados INTEGER DEFAULT 0,
    total_imagens_processadas INTEGER DEFAULT 0,
    total_rois_extraidas INTEGER DEFAULT 0,
    
    -- Médias
    media_telhados_por_subestacao FLOAT,
    media_confianca_deteccao FLOAT,
    media_indice_qualidade FLOAT,
    media_area_telhado_m2 FLOAT,
    
    -- Performance
    tempo_medio_processamento_segundos FLOAT,
    tempo_total_processamento_segundos FLOAT,
    
    -- Taxa
    taxa_sucesso_percentual FLOAT,
    numero_erros INTEGER DEFAULT 0,
    
    -- Timestamps
    timestamp_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stats_data ON telhado_estatisticas_diarias(data_dia DESC);


-- ============================================================================
-- 8. VIEWS
-- ============================================================================

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


-- View: Resumo por subestação
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


-- ============================================================================
-- 9. TRIGGERS
-- ============================================================================

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


-- Trigger: Marcar ROI como processada quando há resultado YOLO
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
-- 10. FUNÇÕES ÚTEIS
-- ============================================================================

-- Limpar cache expirado
CREATE OR REPLACE FUNCTION telhado_limpar_cache_expirado()
RETURNS void AS $$
BEGIN
    UPDATE telhado_cache_segmentacao 
    SET valido = FALSE 
    WHERE timestamp_expiracao < CURRENT_TIMESTAMP 
      AND valido = TRUE;
END;
$$ LANGUAGE plpgsql;


-- Calcular potencial solar por subestação
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


-- ============================================================================
-- 11. COMENTÁRIOS
-- ============================================================================

COMMENT ON TABLE telhado_deteccoes IS 'Telhados/edifícios detectados em imagens de satélite via YOLOv8';
COMMENT ON TABLE telhado_rois IS 'ROIs (imagens recortadas) de telhados para processamento posterior';
COMMENT ON TABLE telhado_processamento_yolo IS 'Resultados do processamento com modelos YOLO (painéis solares, cobertura, etc)';
COMMENT ON TABLE telhado_modelos_yolo IS 'Registro de modelos YOLO disponíveis na plataforma';
COMMENT ON COLUMN telhado_deteccoes.confianca_deteccao IS 'Confiança da detecção (0-1), do modelo YOLOv8';
COMMENT ON COLUMN telhado_deteccoes.tipo_edificio IS 'Classificação do tipo de edifício: residencial, comercial, industrial, desconhecido';
COMMENT ON COLUMN telhado_rois.url_storage_s3 IS 'URL em storage cloud (S3/Azure/GCP) para acesso remoto';
COMMENT ON COLUMN telhado_processamento_yolo.propriedades_calculadas IS 'Propriedades derivadas como potencial_mw, orientacao_media, etc';

-- ============================================================================
-- FIM DO SCHEMA
-- ============================================================================
