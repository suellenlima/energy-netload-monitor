-- ============================================================================
-- MIGRATION: Sincronizar dados de subestacoes_ons para subestacoes_detectadas
-- ============================================================================
-- Copia os dados da tabela ONS (populada pela ETL) para a tabela de detecções
-- Author: Energy Netload Monitor
-- Date: 2026-01-30

-- Inserir dados de subestacoes_ons em subestacoes_detectadas
-- Mapeando as colunas disponíveis em ambas as tabelas
INSERT INTO subestacoes_detectadas (nome, latitude, longitude, distribuidora, subsistema, geom)
SELECT 
    nome,
    latitude,
    longitude,
    distribuidora,
    subsistema,
    geometry
FROM subestacoes_ons
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Verificar quantos registros foram inseridos
SELECT COUNT(*) as total_subestacoes FROM subestacoes_detectadas;

-- Mostrar alguns exemplos
SELECT id, nome, distribuidora, latitude, longitude FROM subestacoes_detectadas LIMIT 5;
