-- Script SQL: Schema atualizado para dados REAIS de múltiplas fontes
-- Autor: Energy Netload Monitor
-- Data: 2026-01-31
-- Versão: 2.0 (Dados Reais)

-- ============================================================================
-- 1. EXTENSÕES
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 2. TABELA DE SUBESTAÇÕES (Múltiplas Fontes)
-- ============================================================================

-- Adicionar coluna de origem dos dados (se não existir)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subestacoes_detectadas' 
        AND column_name = 'fonte_dados'
    ) THEN
        ALTER TABLE subestacoes_detectadas 
        ADD COLUMN fonte_dados VARCHAR(50) DEFAULT 'manual';
    END IF;
END $$;

-- Adicionar coluna de código ONS (se não existir)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subestacoes_detectadas' 
        AND column_name = 'codigo_ons'
    ) THEN
        ALTER TABLE subestacoes_detectadas 
        ADD COLUMN codigo_ons VARCHAR(100);
    END IF;
END $$;

-- Adicionar coluna de subsistema (se não existir)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subestacoes_detectadas' 
        AND column_name = 'subsistema'
    ) THEN
        ALTER TABLE subestacoes_detectadas 
        ADD COLUMN subsistema VARCHAR(50);
    END IF;
END $$;

-- Criar índices para fontes de dados
CREATE INDEX IF NOT EXISTS idx_subestacoes_fonte 
ON subestacoes_detectadas(fonte_dados);

CREATE INDEX IF NOT EXISTS idx_subestacoes_codigo_ons 
ON subestacoes_detectadas(codigo_ons);

-- Comentários
COMMENT ON COLUMN subestacoes_detectadas.fonte_dados IS 'Origem dos dados: ONS, ANEEL, OSM, satelite, manual';
COMMENT ON COLUMN subestacoes_detectadas.codigo_ons IS 'Código oficial da subestação no ONS';
COMMENT ON COLUMN subestacoes_detectadas.subsistema IS 'Subsistema: Norte, Nordeste, Sudeste/Centro-Oeste, Sul';

-- ============================================================================
-- 3. TABELA DE TRANSFORMADORES (OpenStreetMap + ANEEL)
-- ============================================================================

-- Adicionar coluna de origem OSM
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'transformadores' 
        AND column_name = 'osm_id'
    ) THEN
        ALTER TABLE transformadores 
        ADD COLUMN osm_id BIGINT;
    END IF;
END $$;

-- Adicionar coluna de fonte de dados
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'transformadores' 
        AND column_name = 'fonte_dados'
    ) THEN
        ALTER TABLE transformadores 
        ADD COLUMN fonte_dados VARCHAR(50) DEFAULT 'manual';
    END IF;
END $$;

-- Criar índices
CREATE INDEX IF NOT EXISTS idx_transformadores_osm 
ON transformadores(osm_id);

CREATE INDEX IF NOT EXISTS idx_transformadores_fonte 
ON transformadores(fonte_dados);

-- Comentários
COMMENT ON COLUMN transformadores.osm_id IS 'ID do objeto no OpenStreetMap';
COMMENT ON COLUMN transformadores.fonte_dados IS 'Origem: OSM, ANEEL, SCADA, manual';

-- ============================================================================
-- 4. TABELA DE USINAS DE GERAÇÃO (ANEEL SIGA)
-- ============================================================================

CREATE TABLE IF NOT EXISTS usinas_geracao (
    id SERIAL PRIMARY KEY,
    codigo_ceg VARCHAR(50) UNIQUE,
    nome VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(11, 7) NOT NULL,
    localizacao GEOMETRY(Point, 4326),
    
    -- Características da usina
    tipo_geracao VARCHAR(50), -- 'UFV' (solar), 'EOL' (eólica), 'UHE', 'PCH', etc.
    fonte_energia VARCHAR(100), -- 'Solar Fotovoltaica', 'Eólica', etc.
    combustivel VARCHAR(100),
    potencia_outorgada_kw DECIMAL(12, 2),
    potencia_fiscalizada_kw DECIMAL(12, 2),
    
    -- Localização administrativa
    municipio VARCHAR(100),
    estado VARCHAR(2),
    bacia_hidrografica VARCHAR(100),
    
    -- Status operacional
    situacao VARCHAR(50), -- 'Operação', 'Construção', 'Outorgada', etc.
    data_operacao DATE,
    
    -- Proprietário
    proprietario VARCHAR(200),
    cpf_cnpj VARCHAR(20),
    
    -- Conexão à rede
    subestacao_conectada_id INTEGER,
    
    -- Metadados
    fonte_dados VARCHAR(50) DEFAULT 'ANEEL',
    data_ultima_atualizacao DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (subestacao_conectada_id) REFERENCES subestacoes_detectadas(id)
);

-- Índices espaciais e de busca
CREATE INDEX IF NOT EXISTS idx_usinas_geom 
ON usinas_geracao USING GIST(localizacao);

CREATE INDEX IF NOT EXISTS idx_usinas_tipo 
ON usinas_geracao(tipo_geracao);

CREATE INDEX IF NOT EXISTS idx_usinas_estado 
ON usinas_geracao(estado);

CREATE INDEX IF NOT EXISTS idx_usinas_situacao 
ON usinas_geracao(situacao);

-- Comentários
COMMENT ON TABLE usinas_geracao IS 'Usinas de geração distribuída (dados ANEEL SIGA)';
COMMENT ON COLUMN usinas_geracao.codigo_ceg IS 'Código CEG (Cadastro de Empreendimentos de Geração)';
COMMENT ON COLUMN usinas_geracao.tipo_geracao IS 'Sigla ANEEL: UFV, EOL, UHE, PCH, UTE, CGH';
COMMENT ON COLUMN usinas_geracao.potencia_outorgada_kw IS 'Potência autorizada pela ANEEL';
COMMENT ON COLUMN usinas_geracao.potencia_fiscalizada_kw IS 'Potência verificada em campo';

-- ============================================================================
-- 5. TABELA DE CONSUMIDORES (Expandida)
-- ============================================================================

-- Adicionar colunas para dados reais de distribuidoras
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'consumidores' 
        AND column_name = 'codigo_unidade_consumidora'
    ) THEN
        ALTER TABLE consumidores 
        ADD COLUMN codigo_unidade_consumidora VARCHAR(50);
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'consumidores' 
        AND column_name = 'medidor_numero'
    ) THEN
        ALTER TABLE consumidores 
        ADD COLUMN medidor_numero VARCHAR(50);
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'consumidores' 
        AND column_name = 'fonte_dados'
    ) THEN
        ALTER TABLE consumidores 
        ADD COLUMN fonte_dados VARCHAR(50) DEFAULT 'manual';
    END IF;
END $$;

-- Criar índices
CREATE INDEX IF NOT EXISTS idx_consumidores_unidade 
ON consumidores(codigo_unidade_consumidora);

CREATE INDEX IF NOT EXISTS idx_consumidores_medidor 
ON consumidores(medidor_numero);

CREATE INDEX IF NOT EXISTS idx_consumidores_fonte 
ON consumidores(fonte_dados);

-- Comentários
COMMENT ON COLUMN consumidores.codigo_unidade_consumidora IS 'Código da UC na distribuidora';
COMMENT ON COLUMN consumidores.medidor_numero IS 'Número do medidor de energia';
COMMENT ON COLUMN consumidores.fonte_dados IS 'Origem: distribuidora, ANEEL, manual';

-- ============================================================================
-- 6. TABELA DE ÁREA DE COBERTURA (Expandida)
-- ============================================================================

-- Adicionar coluna de origem do polígono
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subestacoes_area_cobertura' 
        AND column_name = 'fonte_dados'
    ) THEN
        ALTER TABLE subestacoes_area_cobertura 
        ADD COLUMN fonte_dados VARCHAR(50) DEFAULT 'calculado';
    END IF;
END $$;

-- Adicionar coluna de confiabilidade
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subestacoes_area_cobertura' 
        AND column_name = 'confiabilidade'
    ) THEN
        ALTER TABLE subestacoes_area_cobertura 
        ADD COLUMN confiabilidade INTEGER CHECK (confiabilidade BETWEEN 1 AND 5);
    END IF;
END $$;

-- Comentários
COMMENT ON COLUMN subestacoes_area_cobertura.fonte_dados IS 'Origem: oficial, OSM, calculado, satelite';
COMMENT ON COLUMN subestacoes_area_cobertura.confiabilidade IS 'Nível de confiança: 1 (baixo) a 5 (alto)';

-- ============================================================================
-- 7. TABELA DE LOG DE ETL
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_execucao_log (
    id SERIAL PRIMARY KEY,
    tipo_etl VARCHAR(50) NOT NULL,
    fonte_dados VARCHAR(50) NOT NULL,
    data_execucao TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) NOT NULL, -- 'sucesso', 'falha', 'parcial'
    
    -- Estatísticas
    registros_extraidos INTEGER,
    registros_inseridos INTEGER,
    registros_atualizados INTEGER,
    registros_falhados INTEGER,
    
    duracao_segundos DECIMAL(10, 2),
    
    -- Detalhes
    mensagem TEXT,
    erro TEXT,
    
    -- Metadados
    usuario VARCHAR(100),
    host VARCHAR(100)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_etl_log_tipo 
ON etl_execucao_log(tipo_etl);

CREATE INDEX IF NOT EXISTS idx_etl_log_data 
ON etl_execucao_log(data_execucao DESC);

CREATE INDEX IF NOT EXISTS idx_etl_log_status 
ON etl_execucao_log(status);

-- Comentários
COMMENT ON TABLE etl_execucao_log IS 'Log de execuções do ETL para auditoria';
COMMENT ON COLUMN etl_execucao_log.tipo_etl IS 'Tipo: ons, aneel, osm, scada, satelite';

-- ============================================================================
-- 8. VIEW: QUALIDADE DOS DADOS
-- ============================================================================

CREATE OR REPLACE VIEW vw_qualidade_dados AS
SELECT 
    'subestacoes' as tabela,
    fonte_dados,
    COUNT(*) as total_registros,
    COUNT(CASE WHEN localizacao IS NULL THEN 1 END) as sem_coordenadas,
    COUNT(CASE WHEN nome IS NULL OR nome = '' THEN 1 END) as sem_nome,
    ROUND(
        100.0 * COUNT(CASE WHEN localizacao IS NOT NULL AND nome IS NOT NULL THEN 1 END) / COUNT(*),
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

-- ============================================================================
-- 9. VIEW: COBERTURA POR SUBESTAÇÃO
-- ============================================================================

CREATE OR REPLACE VIEW vw_cobertura_subestacoes AS
SELECT 
    se.id,
    se.nome,
    se.fonte_dados as fonte_subestacao,
    se.subsistema,
    se.latitude,
    se.longitude,
    
    -- Área de cobertura
    ac.area_km2,
    ac.metodo_definicao as metodo_area,
    ac.confiabilidade,
    ac.fonte_dados as fonte_area,
    
    -- Equipamentos conectados
    COUNT(DISTINCT t.id) as total_transformadores,
    COUNT(DISTINCT CASE WHEN t.status = 'ativo' THEN t.id END) as transformadores_ativos,
    SUM(t.potencia_kva) as potencia_total_kva,
    
    COUNT(DISTINCT c.id) as total_consumidores,
    COUNT(DISTINCT CASE WHEN c.status = 'ativo' THEN c.id END) as consumidores_ativos,
    SUM(c.consumo_medio_mensal_kwh) as consumo_total_kwh,
    
    COUNT(DISTINCT u.id) as usinas_conectadas,
    SUM(u.potencia_outorgada_kw) as geracao_distribuida_kw,
    
    -- Última atualização
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
-- 10. FUNÇÃO: ATRIBUIR CONFIABILIDADE
-- ============================================================================

CREATE OR REPLACE FUNCTION calcular_confiabilidade_area(p_subestacao_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    v_metodo VARCHAR(100);
    v_fonte VARCHAR(50);
    v_total_trans INTEGER;
    v_confiabilidade INTEGER;
BEGIN
    -- Buscar informações da área
    SELECT metodo_definicao, fonte_dados
    INTO v_metodo, v_fonte
    FROM subestacoes_area_cobertura
    WHERE subestacao_id = p_subestacao_id;
    
    -- Contar transformadores
    SELECT COUNT(*)
    INTO v_total_trans
    FROM transformadores
    WHERE subestacao_id = p_subestacao_id
      AND status = 'ativo';
    
    -- Calcular confiabilidade
    IF v_metodo = 'cadastro_oficial' THEN
        v_confiabilidade := 5; -- Máxima confiança
    ELSIF v_fonte = 'OSM' AND v_total_trans >= 10 THEN
        v_confiabilidade := 4; -- Alta confiança
    ELSIF v_metodo = 'analise_topologica' AND v_total_trans >= 5 THEN
        v_confiabilidade := 3; -- Média confiança
    ELSIF v_total_trans >= 3 THEN
        v_confiabilidade := 2; -- Baixa confiança
    ELSE
        v_confiabilidade := 1; -- Muito baixa confiança
    END IF;
    
    -- Atualizar
    UPDATE subestacoes_area_cobertura
    SET confiabilidade = v_confiabilidade
    WHERE subestacao_id = p_subestacao_id;
    
    RETURN v_confiabilidade;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 11. ÍNDICES DE PERFORMANCE
-- ============================================================================

-- Melhorar queries de relacionamento
CREATE INDEX IF NOT EXISTS idx_transformadores_subestacao_status 
ON transformadores(subestacao_id, status);

CREATE INDEX IF NOT EXISTS idx_consumidores_transformador_status 
ON consumidores(transformador_id, status);

CREATE INDEX IF NOT EXISTS idx_usinas_subestacao 
ON usinas_geracao(subestacao_conectada_id);

-- Índices de data para auditoria
CREATE INDEX IF NOT EXISTS idx_transformadores_updated 
ON transformadores(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_consumidores_updated 
ON consumidores(updated_at DESC);

-- ============================================================================
-- 12. COMENTÁRIOS FINAIS
-- ============================================================================

COMMENT ON DATABASE energy_monitor IS 'Energy Netload Monitor - Dados REAIS de ONS, ANEEL e OpenStreetMap';

-- ============================================================================
-- FIM DO SCRIPT
-- ============================================================================

-- Mostrar resumo
SELECT 
    'Schema atualizado com sucesso!' as status,
    NOW() as data_execucao;

SELECT 
    'Fontes de dados suportadas:' as info,
    'ONS, ANEEL SIGA, OpenStreetMap, SCADA, Satélite' as fontes;
