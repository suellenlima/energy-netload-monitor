-- ============================================================================
-- Migração: Adicionar campo url_imagem_origem
-- Data: 2026-02-01
-- Descrição: Adiciona URL da imagem original onde o telhado foi detectado
--            Permite que o modelo de painéis solares processe apenas o bbox
-- ============================================================================

-- Adicionar coluna se não existir
ALTER TABLE telhados_detectados_transformador 
ADD COLUMN IF NOT EXISTS url_imagem_origem TEXT;

-- Adicionar comentário
COMMENT ON COLUMN telhados_detectados_transformador.url_imagem_origem IS 
'URL da imagem original (Google Maps grid) onde o telhado foi detectado. Use com bbox_json para cortar a região do telhado.';

-- Verificar se foi criado
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'telhados_detectados_transformador'
  AND column_name = 'url_imagem_origem';
