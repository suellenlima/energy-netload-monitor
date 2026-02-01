-- Migração: Adicionar suporte a transformadores em requisicoes_satelite_cbers4a
-- Data: 31/01/2026
-- Descrição: Permite registrar requisições CBERS-4A para transformadores (sem subestação)

-- Tornar subestacao_id nullable (permite transformadores)
ALTER TABLE requisicoes_satelite_cbers4a
ALTER COLUMN subestacao_id DROP NOT NULL;

-- Adicionar coluna transformador_id se não existir
ALTER TABLE requisicoes_satelite_cbers4a
ADD COLUMN IF NOT EXISTS transformador_id INTEGER;

-- Adicionar foreign key para transformadores se não existir
ALTER TABLE requisicoes_satelite_cbers4a
ADD CONSTRAINT fk_requisicoes_cbers4a_transformador 
FOREIGN KEY (transformador_id) REFERENCES transformadores(id) ON DELETE CASCADE
NOT VALID;

-- Validar constraint (não bloqueia operações)
ALTER TABLE requisicoes_satelite_cbers4a
VALIDATE CONSTRAINT fk_requisicoes_cbers4a_transformador;

-- Criar índice
CREATE INDEX IF NOT EXISTS idx_requisicoes_cbers4a_transformador_id
ON requisicoes_satelite_cbers4a(transformador_id);

-- Confirmar estrutura
\d requisicoes_satelite_cbers4a
