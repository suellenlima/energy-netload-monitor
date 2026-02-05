# Legacy Files Cleanup - Subestacao DDD Migration

**Data:** 2026-02-04
**Status:** Pós-Migração DDD Subestacao (18 endpoints)

## Resumo Executivo

Após a migração completa da Subestacao para DDD (10 endpoints legados refatorados + 8 endpoints DDD puros), foram identificados arquivos legados que precisam ser gerenciados.

---

## Arquivos Legados Identificados

### 1. ✅ `src/repositories/subestacao_repository.py` - DELETAR

| Aspecto | Detalhe |
|--------|--------|
| **Localização** | `backend/src/repositories/subestacao_repository.py` |
| **Tipo** | Repositório padrão pré-DDD |
| **Status** | ❌ NÃO UTILIZADO |
| **Razão da Remoção** | Todos os endpoints migraram para DDD com nova estrutura |
| **Substituído Por** | `src/infrastructure/persistence/subestacao/repository.py` |
| **Ação** | ✅ SEGURO DELETAR |

#### Detalhes da Migração:
```
ANTES (Legado):
  src/repositories/subestacao_repository.py
  └─ Método get_ons(), get_detectadas(), etc.
  └─ Herança de BaseRepository
  └─ Sem padrão DDD

AGORA (DDD):
  src/infrastructure/persistence/subestacao/
  ├─ repository.py (SQLAlchemySubestacaoRepository)
  ├─ mapper.py (SubestacaoMapper)
  └─ (integrado com Domain Layer)
```

#### Verificação de Uso:
```
✅ grep -r "SubestacaoRepoDepends" → 0 resultados (nenhum uso)
✅ grep -r "get_subestacao_repository" → 0 resultados (função removida de deps.py)
```

---

### 2. ⚠️ `src/services/satelite_service.py` - MANTER

| Aspecto | Detalhe |
|--------|--------|
| **Localização** | `backend/src/services/satelite_service.py` |
| **Tipo** | Serviço de integração (Sentinel-2, dados geoespaciais) |
| **Status** | 📌 LEGADO MAS FUNCIONAL |
| **Razão da Manutenção** | Será necessário para futuras migrações (Analise, Satelite) |
| **Futuro** | Pode ser refatorado para DDD em fase posterior |
| **Ação** | 📌 MANTER POR ENQUANTO |

#### Características:
- Integração com dados de satélite (Sentinel-2)
- Processamento geoespacial
- Não foi alvo da migração Subestacao
- Será reutilizado em: módulos Analise e Satelite (próximas migrações)

---

### 3. ⚠️ `src/repositories/analise_repository.py` - VERIFICAR

| Aspecto | Detalhe |
|--------|--------|
| **Localização** | `backend/src/repositories/analise_repository.py` |
| **Tipo** | Repositório padrão pré-DDD (análogo a subestacao_repository.py) |
| **Status** | ⚠️ AINDA PODE ESTAR EM USO |
| **Razão** | Módulo Analise ainda não foi migrado para DDD |
| **Ação** | 🔍 VERIFICAR antes de deletar (parte da próxima migração) |

---

### 4. ✅ `src/api/deps.py` - LIMPO

| Aspecto | Detalhe |
|--------|--------|
| **Localização** | `backend/src/api/deps.py` |
| **Ação Realizada** | ✅ Removidos imports/aliases legados |
| **Removido** | `SubestacaoRepository`, `get_subestacao_repository()`, `SubestacaoRepoDepends` |
| **Mantido** | `AnaliseRepository` (ainda em uso) |
| **Status** | ✅ LIMPO |

---

## Plano de Ação

### Fase 1: IMEDIATA (Agora) ✅
- [x] Remover imports legados de `deps.py`
- [x] Documentar status de cada arquivo legado
- [x] Preparar limpeza de `subestacao_repository.py`

### Fase 2: PRÓXIMA (Migração Analise/Satelite)
- [ ] Migrar `analise_repository.py` para DDD (estrutura 11 arquivos)
- [ ] Refatorar integrações com `satelite_service.py` se necessário
- [ ] Remover `analise_repository.py` após migração

### Fase 3: FUTURO (Limpeza Total)
- [ ] Consolidar `satelite_service.py` como parte da arquitetura Satelite DDD
- [ ] Realizar limpeza final de legados

---

## Arquivo para Deletar

```bash
# Deletar subestacao_repository.py (SEGURO)
rm backend/src/repositories/subestacao_repository.py

# Verificar se há referências (deve retornar 0)
grep -r "subestacao_repository" backend/src/
```

---

## Status da Migração DDD - Subestacao

| Item | Status | Detalhe |
|------|--------|---------|
| Domain Layer | ✅ COMPLETO | 5 arquivos, sem mudanças necessárias |
| Application Layer | ✅ COMPLETO | 15 use cases (8 core + 7 legacy refactored) |
| Infrastructure Layer | ✅ COMPLETO | Repository SQLAlchemy 2.0, Mapper |
| API Layer | ✅ COMPLETO | 18 endpoints (10 legacy refactored + 8 DDD) |
| Tests | ✅ COMPLETO | test_18_subestacao_endpoints.py |
| Legacy Cleanup | ✅ PARCIAL | deps.py limpo, subestacao_repository.py pronto para deletar |

---

## Próximas Tarefas

1. ✅ **Deletar** `src/repositories/subestacao_repository.py`
2. 📌 **Manter** `src/services/satelite_service.py`
3. ⏳ **Próxima Migração** → Analise Module (estrutura 11 arquivos - mesmo padrão)
4. ⏳ **Próxima Migração** → Satelite Module

---

## Referências

- [SUBESTACAO_18_ENDPOINTS_MIGRATION.md](SUBESTACAO_18_ENDPOINTS_MIGRATION.md) - Detalhes da migração
- [docs/IMPLEMENTACAO_COMPLETA.md](../docs/IMPLEMENTACAO_COMPLETA.md) - Arquitetura geral
- **Princípio**: Uma vez migrado para DDD, o arquivo legado é substituído completamente

