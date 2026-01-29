-- Script de criação de índices para otimização de queries
-- Execute este script no banco de dados PostgreSQL para melhorar performance

-- =============================================================================
-- ÍNDICES PARA TABELA subestacoes_ons
-- =============================================================================

-- Índice para filtro por distribuidora (usado em várias queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_ons_distribuidora
    ON subestacoes_ons (distribuidora);

-- Índice para ordenação por tensão (usado em listagens)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_ons_tensao_desc
    ON subestacoes_ons (tensao_kv DESC NULLS LAST);

-- Índice para filtro por subsistema
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_ons_subsistema
    ON subestacoes_ons (subsistema);

-- Índice composto para queries com distribuidora + ordenação por tensão
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_ons_dist_tensao
    ON subestacoes_ons (distribuidora, tensao_kv DESC NULLS LAST);

-- =============================================================================
-- ÍNDICES PARA TABELA subestacoes_detectadas
-- =============================================================================

-- Índice para filtro por distribuidora
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_detectadas_distribuidora
    ON subestacoes_detectadas (distribuidora);

-- Índice para ordenação por potência (usado em listagens)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_detectadas_potencia_desc
    ON subestacoes_detectadas (potencia_total_mw DESC);

-- =============================================================================
-- ÍNDICES PARA TABELA usinas_siga
-- =============================================================================

-- Índice para coordenadas válidas (usado no clustering)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usinas_siga_coords_valid
    ON usinas_siga (latitude, longitude)
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- =============================================================================
-- ÍNDICES PARA TABELA gd_detalhada
-- =============================================================================

-- Índice para filtro por distribuidora
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gd_detalhada_distribuidora
    ON gd_detalhada (distribuidora);

-- Índice para agregação por classe
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gd_detalhada_classe
    ON gd_detalhada (classe);

-- =============================================================================
-- ÍNDICES PARA TABELA gd_granular
-- =============================================================================

-- Índice para filtro por distribuidora
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gd_granular_distribuidora
    ON gd_granular (distribuidora);

-- Índice para agregação por tipo de estabelecimento
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gd_granular_tipo
    ON gd_granular (tipo_estabelecimento);

-- =============================================================================
-- ÍNDICES PARA TABELA carga_ons
-- =============================================================================

-- Índice para filtro por subsistema + ordenação por tempo
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_carga_ons_subsistema_time
    ON carga_ons (subsistema, time DESC);

-- =============================================================================
-- ÍNDICES PARA TABELA clima_real
-- =============================================================================

-- Índice para JOIN com carga_ons
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clima_real_subsistema_time
    ON clima_real (subsistema, time);

-- =============================================================================
-- ÍNDICES PARA TABELA auditoria_visual
-- =============================================================================

-- Índice para filtro por distribuidora + ordenação por data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_auditoria_visual_dist_data
    ON auditoria_visual (distribuidora, data_inspecao DESC);

-- =============================================================================
-- EXTENSÃO PARA BUSCA TEXTUAL (ILIKE mais eficiente)
-- =============================================================================

-- Habilita extensão pg_trgm para índices de busca textual
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Índice trigram para busca ILIKE em distribuidora (subestacoes_ons)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subestacoes_ons_dist_trgm
    ON subestacoes_ons USING gin (distribuidora gin_trgm_ops);

-- =============================================================================
-- ATUALIZA ESTATÍSTICAS
-- =============================================================================

-- Atualiza estatísticas das tabelas para o query planner
ANALYZE subestacoes_ons;
ANALYZE subestacoes_detectadas;
ANALYZE usinas_siga;
ANALYZE gd_detalhada;
ANALYZE gd_granular;
ANALYZE carga_ons;
ANALYZE clima_real;
ANALYZE auditoria_visual;
