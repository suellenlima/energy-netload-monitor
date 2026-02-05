# Subestacao DDD Migration - Phase 2 Plan (TIER 2: Refactoring Services)

**Data:** 2026-02-04
**Status:** Phase 1 Complete ✅ | Phase 2 Planning ⏳

---

## Executive Summary

Fase 1 (Subestacao DDD) foi **100% completa**, mas 6 endpoints ficaram em estado **HÍBRIDO** porque dependem de serviços legados que ainda não foram refatorados:

- `subestacoes_clustering.py` (2 endpoints)
- `area_service.py` (3 endpoints)
- `subestacao_repository.py` (0 endpoints - DELETAR)

**Objetivo Phase 2:** Refatorar esses serviços para DDD e eliminar dependências legadas.

---

## Part 1: Arquivos Legados - Status Detalhado

### 1.1 ❌ `src/repositories/subestacao_repository.py` - DELETAR IMEDIATAMENTE

```
Status: NÃO UTILIZADO ✅ SEGURO DELETAR
Localização: backend/src/repositories/subestacao_repository.py
Substituído Por: src/infrastructure/persistence/subestacao/repository.py
```

**Verificação:**
```bash
grep -r "subestacao_repository" backend/src/
# Resultado: Nenhum import encontrado
```

**Ação:**
```bash
rm backend/src/repositories/subestacao_repository.py
```

---

### 1.2 ⚠️ `src/services/subestacoes_clustering.py` - REFATORAR (TIER 2)

```
Status: CRÍTICO - EM USO POR 2 ENDPOINTS
Localização: backend/src/services/subestacoes_clustering.py
Dependências: GD (Geração Distribuída), DBSCAN clustering geoespacial
```

**Endpoints que Dependem:**
```
1. GET /subestacoes/detectadas (legado refatorado)
   └─ Usa: detect_subestacoes_by_clustering()
   
2. POST /subestacoes/detectadas/atualizar
   └─ Usa: detect_subestacoes_by_clustering() + load_detected_subestacoes()
```

**Funções Principais:**
```python
def detect_subestacoes_by_clustering(df_gd: pd.DataFrame, eps_km: float) -> pd.DataFrame
def load_detected_subestacoes(df: pd.DataFrame, engine, logger) -> int
```

**Plano de Refatoração:**
```
1. Criar src/application/subestacao/clustering_use_cases.py
   ├─ DetectarSubestacoesPorClusteringUseCase
   └─ CarregarSubestacioesDetectadasUseCase

2. Criar src/infrastructure/services/subestacao_clustering_service.py (DDD)
   ├─ SubestacaoClusteringService (refactored)
   └─ SubestacaoDetectionRepository (new)

3. Atualizar endpoints:
   └─ GET /subestacoes/detectadas → Usar novo use case
   └─ POST /subestacoes/detectadas/atualizar → Usar novo use case
```

---

### 1.3 ⚠️ `src/services/area_service.py` - REFATORAR (TIER 2)

```
Status: CRÍTICO - EM USO POR 3 ENDPOINTS
Localização: backend/src/services/area_service.py
Dependências: Cálculo de áreas, geometria
```

**Endpoints que Dependem:**
```
1. GET /subestacoes/{id}/area
   └─ Usa: AreaService.obter_area_cobertura()
   
2. GET /subestacoes/{id}/transformadores
   └─ Usa: AreaService.obter_transformadores()
   
3. GET /subestacoes/areas/stats
   └─ Usa: AreaService.obter_estatisticas_areas()
```

**Plano de Refatoração:**
```
1. Criar src/application/subestacao/area_use_cases.py
   ├─ ObtenerAreaSubestacaoUseCase
   ├─ ObtenerTransformadoresUseCase
   └─ ObtenerEstatisticasAreasUseCase

2. Criar src/infrastructure/services/subestacao_area_service.py (DDD)
   ├─ SubestacaoAreaService (refactored)
   └─ SubestacaoAreaRepository (new)

3. Atualizar endpoints:
   └─ GET /subestacoes/{id}/area → Usar novo use case
   └─ GET /subestacoes/{id}/transformadores → Usar novo use case
   └─ GET /subestacoes/areas/stats → Usar novo use case
```

---

## Part 2: Endpoints Híbridos - Status Atual

### Endpoints que Funcionam Completamente (DDD Puro) ✅

```
1. GET /subestacoes/ons ✅ DDD Completo
2. GET /subestacoes/detectadas ⚠️ HÍBRIDO (usa subestacoes_clustering)
3. GET /subestacoes/geo ✅ DDD Completo
4. GET /subestacoes/resumo ✅ DDD Completo
5. GET /subestacoes/{id}/area ⚠️ HÍBRIDO (usa area_service)
6. GET /subestacoes/{id}/transformadores ⚠️ HÍBRIDO (usa area_service)
7. GET /subestacoes/areas/stats ⚠️ HÍBRIDO (usa area_service)
8. POST /subestacoes/associar-ucs ⚠️ HÍBRIDO (usa subestacoes_clustering)
9. GET /subestacoes/{id}/mix-consumidores ✅ DDD Completo
10. GET /subestacoes/{id}/carga-sintetica (TBD)

DDD Endpoints (8):
1. GET /api/v1/subestacoes ✅ DDD Completo
2. GET /api/v1/subestacoes/{codigo} ✅ DDD Completo
3. GET /api/v1/subestacoes/stats ✅ DDD Completo
4. GET /api/v1/subestacoes/tensao/{tensao_kv} ✅ DDD Completo
5. GET /api/v1/subestacoes/distribuidora/{codigo} ✅ DDD Completo
6. GET /api/v1/subestacoes/{codigo}/tipo-tensao ✅ DDD Completo
7. POST /api/v1/subestacoes/{codigo}/ativar ✅ DDD Completo
8. POST /api/v1/subestacoes/{codigo}/desativar ✅ DDD Completo

RESUMO:
   10/10 Legacy: 7 DDD Completo ✅ | 3 Híbrido ⚠️
   8/8 DDD: 8 DDD Completo ✅ | 0 Híbrido ✅
   Total: 15/18 DDD Completo ✅ | 3/18 Híbrido ⚠️
```

---

## Part 3: Plano de Ação - TIER 2

### Fase 2A: Refatorar subestacoes_clustering.py

**Arquivos a Criar (5 novos):**

```
src/application/subestacao/clustering_use_cases.py      (NEW - 150 linhas)
src/infrastructure/services/subestacao_clustering.py    (NEW - 200 linhas)
src/infrastructure/persistence/subestacao/clustering_mapper.py  (NEW - 50 linhas)
```

**Arquivos a Atualizar:**

```
src/api/subestacoes.py  (atualizar 2 endpoints)
```

**Endpoints Migrados:**

```
GET /subestacoes/detectadas → Usar DetectarSubestacioesUseCase
POST /subestacoes/detectadas/atualizar → Usar AtualizarSubestacioesDetectadasUseCase
```

---

### Fase 2B: Refatorar area_service.py

**Arquivos a Criar (5 novos):**

```
src/application/subestacao/area_use_cases.py           (NEW - 150 linhas)
src/infrastructure/services/subestacao_area.py          (NEW - 200 linhas)
src/infrastructure/persistence/subestacao/area_mapper.py (NEW - 50 linhas)
```

**Arquivos a Atualizar:**

```
src/api/subestacoes.py  (atualizar 3 endpoints)
```

**Endpoints Migrados:**

```
GET /subestacoes/{id}/area → Usar ObtenerAreaSubestacaoUseCase
GET /subestacoes/{id}/transformadores → Usar ObtenerTransformadoresUseCase
GET /subestacoes/areas/stats → Usar ObtenerEstatisticasAreasUseCase
```

---

### Fase 2C: Deletar Arquivo Legado

```bash
# Remover arquivo não utilizado
rm backend/src/repositories/subestacao_repository.py

# Verificar que não há impacto
grep -r "subestacao_repository" backend/src/
# Resultado: Nenhum (seguro)
```

---

## Part 4: Timeline e Prioridades

### 🔴 CRÍTICO - Fazer Agora (30 minutos)

```
✅ Deletar subestacao_repository.py (não utilizado)
✅ Documentar plano TIER 2
✅ Atualizar deps.py (já feito)
```

### 🟡 IMPORTANTE - Próxima Sessão

**Opção A: Continuar com TIER 2 Clustering** (2-3 horas)
```
1. Criar 3 novos arquivos para clustering
2. Refatorar 2 endpoints
3. Testar
```

**Opção B: Começar Nova Migração - Analise Module** (2-3 horas)
```
1. Criar 11 arquivos DDD para Analise
2. Migrar ~10 endpoints
3. Testar
```

**Opção C: Híbrida** (mais equilibrada)
```
1. TIER 2 Clustering (1 hora) - deixa sistema mais limpo
2. Analise Module Phase 1 (2 horas) - mais valor agregado
```

---

## Part 5: Decision Matrix

| Critério | TIER 2 Clustering | Analise Module | Recomendação |
|----------|------------------|----------------|--------------|
| **Impacto** | Limpa 3 endpoints | Adiciona 10+ endpoints | Analise |
| **Complexidade** | Média (200 linhas) | Baixa-Média (padrão repetível) | Analise |
| **Tempo** | 2-3h | 2-3h | Analise |
| **Valor** | Manutenibilidade | Funcionalidade | Analise |
| **Bloqueador** | Não | Não | Nenhum |

**RECOMENDAÇÃO:** 
- Se o foco é **Funcionalidade**: Ir para **Analise Module**
- Se o foco é **Qualidade**: Fazer **TIER 2 Clustering** primeiro
- Se o foco é **Velocidade**: Parallelizar ambas (2 pessoas)

---

## Summary: O Que Fazer Agora

### ✅ Imediatamente (5 minutos)

```bash
# 1. Deletar arquivo não utilizado
rm backend/src/repositories/subestacao_repository.py

# 2. Verificar que não há impacto
grep -r "from.*subestacao_repository" backend/src/
# Deve retornar: 0 resultados
```

### ⏳ Próxima Decisão

Escolher entre:
1. **TIER 2 Refactor** - Limpar 3 endpoints híbridos (sugestão: primeiro)
2. **Analise Module** - Migrar novo módulo (10+ endpoints)
3. **Ambas em paralelo** - Se houver recursos

---

## Referências

- [SUBESTACAO_18_ENDPOINTS_MIGRATION.md](SUBESTACAO_18_ENDPOINTS_MIGRATION.md) - Phase 1 Complete
- [LEGACY_FILES_CLEANUP.md](LEGACY_FILES_CLEANUP.md) - Status de arquivos legados
- [docs/IMPLEMENTACAO_COMPLETA.md](../docs/IMPLEMENTACAO_COMPLETA.md) - Arquitetura geral

