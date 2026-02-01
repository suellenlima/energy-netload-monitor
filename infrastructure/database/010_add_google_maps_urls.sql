-- Migration: Adicionar URLs do Google Maps à tabela satelite_imagens
-- Data: 2026-01-31
-- Descrição: Adiciona suporte para armazenar URLs de imagens do Google Maps
--            (satellite e hybrid) junto com imagens CBERS-4A

-- Adicionar campos para URLs do Google Maps
ALTER TABLE satelite_imagens 
ADD COLUMN IF NOT EXISTS url_google_maps_satellite TEXT,
ADD COLUMN IF NOT EXISTS url_google_maps_hybrid TEXT;

-- Adicionar índice para melhor performance em buscas
CREATE INDEX IF NOT EXISTS idx_satelite_google_maps_satellite 
    ON satelite_imagens(url_google_maps_satellite) 
    WHERE url_google_maps_satellite IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_satelite_google_maps_hybrid 
    ON satelite_imagens(url_google_maps_hybrid) 
    WHERE url_google_maps_hybrid IS NOT NULL;

-- Adicionar comentários nas colunas (documentação)
COMMENT ON COLUMN satelite_imagens.url_google_maps_satellite IS 'URL da imagem satellite do Google Maps Static API';
COMMENT ON COLUMN satelite_imagens.url_google_maps_hybrid IS 'URL da imagem hybrid do Google Maps Static API';
