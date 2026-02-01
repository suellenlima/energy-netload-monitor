-- Migração: Adicionar área poligonal ao transformador
-- Data: 31/01/2026
-- Descrição: Adiciona coluna area_poligonal_km à tabela transformadores
--            para armazenar a dimensão poligonal do bbox sem necessidade de parâmetro

-- Adicionar coluna se não existir
ALTER TABLE transformadores
ADD COLUMN IF NOT EXISTS area_poligonal_km DECIMAL(10, 2) DEFAULT 1.0;

-- Adicionar coluna se não existir
ALTER TABLE transformadores_area_cobertura
ADD COLUMN IF NOT EXISTS area_poligonal_km DECIMAL(10, 2) DEFAULT 1.0;

-- Atualizar com valores baseados no raio aproximado (raio ≈ metade da dimensão poligonal)
UPDATE transformadores_area_cobertura
SET area_poligonal_km = CASE 
    WHEN raio_aproximado_m IS NOT NULL AND raio_aproximado_m > 0 
    THEN ROUND(CAST((raio_aproximado_m * 2 / 1000) AS NUMERIC), 2)
    ELSE 1.0
END
WHERE area_poligonal_km = 1.0;

-- Atualizar tabela transformadores com dados de transformadores_area_cobertura
UPDATE transformadores t
SET area_poligonal_km = COALESCE(tac.area_poligonal_km, 1.0)
FROM transformadores_area_cobertura tac
WHERE t.id = tac.transformador_id;

-- Comentário sobre as colunas
COMMENT ON COLUMN transformadores.area_poligonal_km IS 
    'Dimensão da área poligonal em km (bounding box) para busca de imagens - padrão 1.0 km';

COMMENT ON COLUMN transformadores_area_cobertura.area_poligonal_km IS 
    'Dimensão da área poligonal em km para este transformador - baseada em bbox';

-- Criar índice para performance
CREATE INDEX IF NOT EXISTS idx_transformadores_area_poligonal 
ON transformadores(area_poligonal_km);

-- Confirmar mudanças
SELECT 
    id, 
    codigo, 
    nome, 
    area_poligonal_km,
    COUNT(*) OVER() as total_transformadores
FROM transformadores
WHERE area_poligonal_km IS NOT NULL
LIMIT 10;
