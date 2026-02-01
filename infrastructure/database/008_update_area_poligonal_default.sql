-- Migração: Atualizar área_poligonal_km para todos os transformadores
-- Data: 31/01/2026
-- Descrição: Define um valor mínimo de 2.0 km para transformadores com área 0 ou NULL

-- Atualizar transformadores com área poligonal 0 ou NULL
UPDATE transformadores
SET area_poligonal_km = 2.0
WHERE area_poligonal_km IS NULL OR area_poligonal_km = 0.0;

-- Confirmar
SELECT 
    COUNT(*) total,
    COUNT(CASE WHEN area_poligonal_km = 0.0 THEN 1 END) com_zero,
    COUNT(CASE WHEN area_poligonal_km IS NULL THEN 1 END) com_null,
    MIN(area_poligonal_km) min_area,
    MAX(area_poligonal_km) max_area
FROM transformadores;
