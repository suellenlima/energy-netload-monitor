-- Migração: Adicionar associação de UCs com subestações
-- Data: 2024-02-02
-- Objetivo: Permitir análise por subestação (FASE 2)

-- 1. Adicionar coluna de geometria na gd_granular (se não existir)
ALTER TABLE gd_granular
ADD COLUMN IF NOT EXISTS geom GEOMETRY(Point, 4326);

-- 2. Adicionar coluna de associação com subestação
ALTER TABLE gd_granular
ADD COLUMN IF NOT EXISTS subestacao_id INTEGER;

-- 3. Criar índice espacial para performance
CREATE INDEX IF NOT EXISTS idx_gd_granular_geom ON gd_granular USING GIST(geom);

-- 4. Criar índice na subestacao_id
CREATE INDEX IF NOT EXISTS idx_gd_granular_subestacao ON gd_granular(subestacao_id);

-- 5. Adicionar foreign key (opcional - se quiser garantir integridade)
-- ALTER TABLE gd_granular
-- ADD CONSTRAINT fk_gd_granular_subestacao
-- FOREIGN KEY (subestacao_id) REFERENCES subestacoes_detectadas(id)
-- ON DELETE SET NULL;

-- Comentários
COMMENT ON COLUMN gd_granular.geom IS 'Localização geográfica da UC (Point)';
COMMENT ON COLUMN gd_granular.subestacao_id IS 'ID da subestação mais próxima (raio configurável)';

-- Nota: Execute este script no banco de dados para aplicar as mudanças
-- Exemplo: docker-compose exec db psql -U energy_user -d energy_db -f /migrations/001_add_subestacao_association.sql
