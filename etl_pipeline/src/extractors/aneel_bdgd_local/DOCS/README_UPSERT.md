# ETL ANEEL BDGD - Estratégia UPSERT

## ✅ Proteção Contra Duplicatas

A ETL está configurada para **NÃO duplicar dados** ao reprocessar a mesma distribuidora. Todas as tabelas usam **UPSERT** (INSERT ... ON CONFLICT DO UPDATE).

## 📊 Tabelas Protegidas

### 1. **transformadores_aneel**
- **Chave única**: `codigo`
- **Comportamento**: Se o código já existe, atualiza todos os campos (nome, potência, coordenadas, etc.)
- **Campo atualizado**: `data_atualizacao` recebe timestamp atual

### 2. **subestacoes_aneel**
- **Chave única**: `codigo`
- **Comportamento**: Se o código já existe, atualiza nome, coordenadas, tensão
- **Campo atualizado**: `data_atualizacao` recebe timestamp atual

### 3. **consumidores_bt_aneel** (Baixa Tensão)
- **Chave única**: `codigo`
- **Comportamento**: Atualiza carga instalada, coordenadas, classe/subclasse
- **Campo atualizado**: `data_atualizacao` recebe timestamp atual

### 4. **consumidores_mt_aneel** (Média Tensão)
- **Chave única**: `codigo`
- **Comportamento**: Atualiza carga instalada, demanda contratada, coordenadas
- **Campo atualizado**: `data_atualizacao` recebe timestamp atual

### 5. **consumidores_at_aneel** (Alta Tensão)
- **Chave única**: `codigo`
- **Comportamento**: Atualiza carga instalada, demanda contratada, coordenadas
- **Campo atualizado**: `data_atualizacao` recebe timestamp atual

## 🔄 Como Funciona

```sql
INSERT INTO tabela (codigo, campo1, campo2, data_atualizacao)
VALUES (:cod, :val1, :val2, NOW())
ON CONFLICT (codigo) DO UPDATE SET
    campo1 = EXCLUDED.campo1,
    campo2 = EXCLUDED.campo2,
    data_atualizacao = EXCLUDED.data_atualizacao;
```

- **ON CONFLICT (codigo)**: Detecta se o código já existe
- **DO UPDATE SET**: Atualiza os campos com os novos valores
- **EXCLUDED**: Referencia os valores que seriam inseridos

## 📅 Campos de Auditoria

Todas as tabelas têm:
- **`data_criacao`**: Mantida no primeiro INSERT, nunca alterada
- **`data_atualizacao`**: Atualizada a cada reprocessamento

Isso permite saber:
- **Quando o registro foi criado originalmente**
- **Quando foi atualizado pela última vez**

## 🚀 Executando ETL para LIGHT

### Opção 1: Reprocessar tudo (recomendado)
```bash
docker compose exec -T etl python /app/src/extractors/aneel_bdgd_local/etl_aneel_bdgd_local.py --distribuidora LIGHT
```

### Opção 2: Modo debug (mais logs)
```bash
docker compose exec -T etl python /app/src/extractors/aneel_bdgd_local/etl_aneel_bdgd_local.py --distribuidora LIGHT --debug
```

## 📈 O que Acontece

1. **Extração**: Lê arquivos GDB da pasta `data/aneel_bdgd/LIGHT_*`
2. **Transformação**: Processa geometrias, limpa dados, remove duplicatas no DataFrame
3. **Carregamento**: 
   - **Primeiro registro**: INSERT cria novo
   - **Reprocessamento**: UPDATE atualiza dados existentes
4. **Vinculação**: Após carregar, vincula transformadores às subestações por proximidade (10km)

## 🔍 Verificando Atualizações

```sql
-- Ver quando registros foram atualizados
SELECT 
    distribuidora,
    COUNT(*) as total,
    MAX(data_atualizacao) as ultima_atualizacao
FROM transformadores_aneel
GROUP BY distribuidora;

-- Encontrar registros atualizados recentemente (últimas 24h)
SELECT codigo, nome, data_criacao, data_atualizacao
FROM transformadores_aneel
WHERE data_atualizacao > NOW() - INTERVAL '24 hours'
ORDER BY data_atualizacao DESC
LIMIT 10;
```

## ⚠️ Importante

- **Código é chave única**: Se dois registros têm o mesmo código, o último processado prevalece
- **Dados antigos não são deletados**: Apenas atualizados
- **Performance**: UPSERT é mais lento que INSERT puro, mas garante integridade
- **Logs**: Mensagens mostram "carregados/atualizados" ao invés de apenas "carregados"

## 🎯 Vantagens

✅ **Sem duplicatas**: Pode executar ETL múltiplas vezes  
✅ **Dados atualizados**: Sempre prevalece a versão mais recente  
✅ **Auditável**: `data_atualizacao` mostra quando foi processado  
✅ **Seguro**: Não perde dados antigos se algo der errado  
✅ **Idempotente**: Executar 1x ou 10x tem o mesmo resultado final  

## 🔧 Troubleshooting

**Problema**: ETL muito lenta  
**Solução**: UPSERT faz consulta para cada registro. Para grandes volumes, considere:
- Criar índice em `codigo`: `CREATE INDEX IF NOT EXISTS idx_codigo ON tabela(codigo)`
- Processar em batches menores

**Problema**: Registros não são atualizados  
**Causa**: Constraint UNIQUE em `codigo` deve existir  
**Verificar**: 
```sql
SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name = 'transformadores_aneel' AND constraint_type = 'UNIQUE';
```
