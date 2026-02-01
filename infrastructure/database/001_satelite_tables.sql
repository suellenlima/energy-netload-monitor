-- Migration: Adicionar tabela de metadados de imagens de satélite
-- Data: 2026-01-29
-- Descrição: Tabela para armazenar metadados de imagens de satélite
--            consultadas via INPE, STAC (Sentinel-2, Landsat), etc.

-- Tabela principal para metadados de imagens
CREATE TABLE IF NOT EXISTS satelite_imagens (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    sensor VARCHAR(50) NOT NULL,  -- Sentinel-2, Landsat, MODIS, etc.
    data_aquisicao TIMESTAMPTZ NOT NULL,
    resolucao_m INTEGER,  -- Resolução em metros (10-60 para S2, 30 para L8/L9)
    cobertura_nuvem_pct FLOAT CHECK (cobertura_nuvem_pct >= 0 AND cobertura_nuvem_pct <= 100),
    url TEXT,  -- URL para acesso/download
    bbox_json JSONB,  -- Bounding box em GeoJSON
    propriedades_json JSONB,  -- Propriedades customizadas (tile, processamento, etc.)
    data_registro TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT satelite_imagens_unique UNIQUE (subestacao_id, sensor, data_aquisicao)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_subestacao 
    ON satelite_imagens(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_data 
    ON satelite_imagens(data_aquisicao DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_sensor 
    ON satelite_imagens(sensor);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_nuvem 
    ON satelite_imagens(cobertura_nuvem_pct);
CREATE INDEX IF NOT EXISTS idx_satelite_imagens_composite 
    ON satelite_imagens(subestacao_id, data_aquisicao DESC);

-- Tabela para rastrear consultas/buscas realizadas
CREATE TABLE IF NOT EXISTS satelite_consultas (
    id BIGSERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    tipo_consulta VARCHAR(50),  -- 'stac_sentinel2', 'stac_landsat', 'wms_inpe', etc.
    data_inicio TIMESTAMPTZ,
    data_fim TIMESTAMPTZ,
    raio_km FLOAT,
    sensores_consultados TEXT[],  -- Array de sensores
    quantidade_resultados INTEGER,
    tempo_execucao_ms INTEGER,
    status VARCHAR(20),  -- 'sucesso', 'erro', 'timeout'
    mensagem_erro TEXT,
    data_consulta TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para tabela de consultas
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_subestacao 
    ON satelite_consultas(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_tipo 
    ON satelite_consultas(tipo_consulta);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_data 
    ON satelite_consultas(data_consulta DESC);
CREATE INDEX IF NOT EXISTS idx_satelite_consultas_status 
    ON satelite_consultas(status);

-- Tabela para cache de resultados STAC (otimizar buscas frequentes)
CREATE TABLE IF NOT EXISTS satelite_cache_stac (
    id BIGSERIAL PRIMARY KEY,
    bbox_hash VARCHAR(64) NOT NULL,  -- Hash da bbox para lookups rápidos
    min_lat FLOAT,
    max_lat FLOAT,
    min_lon FLOAT,
    max_lon FLOAT,
    data_inicio TIMESTAMPTZ,
    data_fim TIMESTAMPTZ,
    sensor VARCHAR(50),
    resultado_json JSONB,  -- Resposta completa do STAC
    data_cache TIMESTAMPTZ DEFAULT NOW(),
    validade_ate TIMESTAMPTZ,  -- TTL do cache
    CONSTRAINT satelite_cache_stac_unique UNIQUE (bbox_hash, sensor, data_inicio, data_fim)
);

-- Índices para cache
CREATE INDEX IF NOT EXISTS idx_satelite_cache_hash 
    ON satelite_cache_stac(bbox_hash);
CREATE INDEX IF NOT EXISTS idx_satelite_cache_validade 
    ON satelite_cache_stac(validade_ate);
CREATE INDEX IF NOT EXISTS idx_satelite_cache_sensor 
    ON satelite_cache_stac(sensor);

-- Tabela para estatísticas de cobertura
CREATE TABLE IF NOT EXISTS satelite_cobertura_stats (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER NOT NULL REFERENCES subestacoes_detectadas(id) ON DELETE CASCADE,
    sensor VARCHAR(50),
    mes_ano DATE,  -- Primeiro dia do mês
    media_nuvem_pct FLOAT,
    total_cenas INTEGER,
    cenas_baixa_nuvem INTEGER,  -- < 20%
    data_atualizacao TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT satelite_stats_unique UNIQUE (subestacao_id, sensor, mes_ano)
);

-- Índices para estatísticas
CREATE INDEX IF NOT EXISTS idx_satelite_stats_subestacao 
    ON satelite_cobertura_stats(subestacao_id);
CREATE INDEX IF NOT EXISTS idx_satelite_stats_sensor 
    ON satelite_cobertura_stats(sensor);
CREATE INDEX IF NOT EXISTS idx_satelite_stats_mes 
    ON satelite_cobertura_stats(mes_ano DESC);

-- Views úteis

-- View: Últimas imagens por subestação
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

-- View: Resumo de cobertura por subestação
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

-- Comentários úteis
COMMENT ON TABLE satelite_imagens IS 
    'Armazena metadados de imagens de satélite de subestações (Sentinel-2, Landsat, MODIS, etc.)';

COMMENT ON TABLE satelite_consultas IS 
    'Rastreia todas as consultas realizadas às APIs STAC e WMS para auditoria e otimização';

COMMENT ON TABLE satelite_cache_stac IS 
    'Cache de resultados STAC para otimizar buscas frequentes na mesma área';

COMMENT ON TABLE satelite_cobertura_stats IS 
    'Estatísticas mensais de cobertura e disponibilidade de imagens por sensor';

COMMENT ON COLUMN satelite_imagens.sensor IS 
    'Sensor/satélite: Sentinel-2, Landsat-8, Landsat-9, MODIS, etc.';

COMMENT ON COLUMN satelite_imagens.resolucao_m IS 
    'Resolução espacial em metros. Sentinel-2: 10-60m, Landsat: 30m, MODIS: 250-1000m';

COMMENT ON COLUMN satelite_imagens.cobertura_nuvem_pct IS 
    'Percentual de cobertura de nuvem (0-100). Importante para qualidade da imagem.';

COMMENT ON COLUMN satelite_imagens.propriedades_json IS 
    'Propriedades customizadas: tile MGRS/UTM, nível de processamento, missão, etc.';

-- Insert de exemplo
INSERT INTO satelite_imagens 
(subestacao_id, sensor, data_aquisicao, resolucao_m, cobertura_nuvem_pct, url, propriedades_json)
VALUES 
(
    1,
    'Sentinel-2',
    '2026-01-15T13:12:41+00:00',
    10,
    12.5,
    'https://example.com/S2A_MSIL2A_20260115T131241_N0500_R031_T23KPA.zip',
    '{"tile": "23KPA", "nivel": "L2A", "missao": "Copernicus"}'::jsonb
)
ON CONFLICT (subestacao_id, sensor, data_aquisicao) DO NOTHING;

-- Função para limpar cache expirado
CREATE OR REPLACE FUNCTION satelite_limpar_cache_expirado()
RETURNS void AS $$
BEGIN
    DELETE FROM satelite_cache_stac 
    WHERE validade_ate < NOW();
    
    RAISE NOTICE 'Cache expirado removido com sucesso';
END;
$$ LANGUAGE plpgsql;

-- Trigger para atualizar estatísticas automaticamente
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

-- Criar trigger
DROP TRIGGER IF EXISTS trigger_satelite_stats ON satelite_imagens;
CREATE TRIGGER trigger_satelite_stats
AFTER INSERT ON satelite_imagens
FOR EACH ROW
EXECUTE FUNCTION satelite_atualizar_stats();

-- Procedure para limpeza periódica
CREATE OR REPLACE FUNCTION satelite_manutencao_periodica()
RETURNS void AS $$
BEGIN
    -- Limpar cache expirado
    PERFORM satelite_limpar_cache_expirado();
    
    -- Atualizar estatísticas
    -- (Feito via trigger)
    
    RAISE NOTICE 'Manutenção de satélite completada';
END;
$$ LANGUAGE plpgsql;

-- Permissões (opcional, para usuários específicos)
-- GRANT SELECT ON satelite_imagens TO "app_user";
-- GRANT SELECT ON satelite_consultas TO "app_user";
-- GRANT SELECT ON v_satelite_ultimas_imagens TO "app_user";
-- GRANT SELECT ON v_satelite_resumo_cobertura TO "app_user";
