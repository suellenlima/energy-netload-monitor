# ✅ Schema.sql Atualizado - Resumo

## 📅 Data: 2026-02-07

## 🎯 O Que Foi Adicionado

### Novas Tabelas de Consumidores ANEEL (Unidades Consumidoras - BDGD)

#### 1. **consumidores_bt_aneel** (Baixa Tensão)
```sql
- id (SERIAL PRIMARY KEY)
- codigo (VARCHAR UNIQUE) ← Chave para UPSERT
- distribuidora
- dist_codigo
- subestacao_codigo
- classe_subclasse_codigo
- tensao_fornecimento_codigo
- carga_instalada_kw
- latitude, longitude
- data_criacao, data_atualizacao
```

**Índices**:
- `idx_consumidores_bt_distribuidora`
- `idx_consumidores_bt_subestacao`
- `idx_consumidores_bt_coords`

#### 2. **consumidores_mt_aneel** (Média Tensão)
```sql
- Todos os campos de BT +
- circuito_mt_codigo
- demanda_contratada_kw
```

**Índices**:
- `idx_consumidores_mt_distribuidora`
- `idx_consumidores_mt_subestacao`
- `idx_consumidores_mt_coords`

#### 3. **consumidores_at_aneel** (Alta Tensão)
```sql
- Todos os campos de BT +
- circuito_at_codigo
- demanda_contratada_kw
```

**Índices**:
- `idx_consumidores_at_distribuidora`
- `idx_consumidores_at_subestacao`
- `idx_consumidores_at_coords`

## ✅ Verificação Realizada

```sql
-- Tabelas criadas: ✓
- consumidores_bt_aneel
- consumidores_mt_aneel
- consumidores_at_aneel

-- Constraints UNIQUE: ✓
- consumidores_bt_aneel_codigo_key
- consumidores_mt_aneel_codigo_key
- consumidores_at_aneel_codigo_key

-- Índices: ✓
- 5 índices por tabela (total: 15)
```

## 🔄 Compatibilidade

### UPSERT Funcionando
Todas as tabelas suportam INSERT ON CONFLICT:
```sql
INSERT INTO consumidores_bt_aneel (codigo, distribuidora, ...)
VALUES (:cod, :dist, ...)
ON CONFLICT (codigo) DO UPDATE SET
    distribuidora = EXCLUDED.distribuidora,
    ...
```

### Banco de Dados Existente
- ✅ Tabelas já criadas manualmente não são afetadas
- ✅ Constraints já existentes são preservadas
- ✅ Dados existentes permanecem intactos

## 📦 Aplicação em Novos Ambientes

### Docker Compose (Novo Container)
```bash
docker compose down db
docker compose up -d db
```

As tabelas serão criadas automaticamente via:
- `/docker-entrypoint-initdb.d/schema.sql`

### Banco Existente (Upgrade)
```bash
# Aplicar apenas as novas tabelas
Get-Content "infrastructure/database/create_consumidores_tables.sql" | `
    docker compose exec -T db psql -U admin -d energy_monitor
```

## 🎯 Benefícios

1. **Consistência**: Schema centralizado em um único arquivo
2. **Automação**: Novos ambientes já têm as tabelas
3. **UPSERT Ready**: Constraints UNIQUE garantem funcionamento
4. **Performance**: Índices otimizados para queries
5. **Documentação**: Estrutura clara e comentada

## 📝 Localização

**Arquivo principal**:
```
infrastructure/database/schema.sql
```

**Linhas adicionadas**: ~75 linhas (após subestacoes_aneel)

**Arquivos relacionados**:
- `create_consumidores_tables.sql` (backup/manual)
- `test_schema.sql` (verificação)
- `verify_constraints.sql` (diagnóstico)

## 🚀 Próximos Passos

Agora você pode:
1. ✅ Executar ETL sem criar tabelas manualmente
2. ✅ Destruir/recriar banco sem perder estrutura
3. ✅ Compartilhar schema com time
4. ✅ Deploy em produção com confiança

## 🔍 Comandos de Verificação

```powershell
# Testar schema completo
Get-Content "infrastructure/database/test_schema.sql" | `
    docker compose exec -T db psql -U admin -d energy_monitor

# Verificar apenas constraints
docker compose exec -T db psql -U admin -d energy_monitor -c `
    "SELECT table_name, constraint_name FROM information_schema.table_constraints 
     WHERE table_name LIKE 'consumidores_%_aneel' AND constraint_type = 'UNIQUE'"
```

---

**Status**: ✅ Schema.sql atualizado e testado  
**Versão**: 2026-02-07  
**Backward Compatible**: Sim  
**Production Ready**: Sim
