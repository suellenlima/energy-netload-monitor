# Relatório de Migração: src/services → DDD

**Data**: 2026-02-05  
**Status**: Análise Completa - Aguardando Decisão

---

## 📊 Summary

| Métrica | Valor |
|---------|-------|
| **Total de services legados** | 12 arquivos |
| **Em uso (importados)** | 4 arquivos |
| **Não utilizados** | 8 arquivos |
| **Endpoints afetados** | 2 rotas legacy |

---

## 🔴 Dependências Ativas (4/12)

### 1. `transformador_pipeline.py`
- **Importado por**: `src/api/transformador_pipeline.py`
- **Rota**: `POST /transformador`, `GET /transformador/{id}/resultado`, etc.
- **Status**: Legacy (não refatorado para DDD)
- **Equivalente DDD**: `/api/v1/transformadores` em `api/transformadores.py` ✅
- **Ação**: Deletar endpoint legacy, usar apenas DDD

### 2. `telhado_multifonte.py`
- **Importado por**: `src/api/telhado_multifonte.py`
- **Rota**: `POST /telhados/detectar-multifonte`, etc.
- **Status**: Legacy (não refatorado para DDD)
- **Equivalente DDD**: `/api/v1/telhados` em `api/telhados.py` ✅
- **Ação**: Deletar endpoint legacy, usar apenas DDD

### 3. `roof_detection_service.py`
- **Importado por**: `src/application/telhado_detection/service.py`
- **Função**: Classe com métodos de detecção ML usando YOLO
- **Tipo**: Infraestrutura (não é lógica de negócio)
- **Status**: Deve ser extraído para `src/infrastructure/services/`
- **Ação**: Mover para infraestrutura, não deletar

### 4. `roof_service.py`
- **Importado por**: `src/application/telhado_detection/service.py`
- **Função**: Dataclasses, value objects, tipos de dados
- **Tipo**: Modelo (deveria estar em `src/domain/`)
- **Status**: Deve ser migrado para `src/domain/telhado/`
- **Ação**: Mover para domínio, não deletar

---

## 🟢 Não Utilizados (8/12)

Estes arquivos **NÃO são importados** em nenhuma parte do código e podem ser deletados imediatamente:

1. ✅ `anomaly_detection.py`
2. ✅ `cache_service.py`
3. ✅ `google_maps_service.py`
4. ✅ `image_service.py`
5. ✅ `inpe_service.py`
6. ✅ `profile_calibration.py`
7. ✅ `solar_panel_service.py`
8. ✅ `synthetic_load.py`

---

## 🔄 Plano de Migração Recomendado (OPÇÃO B)

### STEP 1: Extrair RoofDetectionService
```
src/services/roof_detection_service.py
  ↓
src/infrastructure/services/roof_detection_service.py
```
- Criar: `src/infrastructure/services/` (novo diretório)
- Mover: `roof_detection_service.py`
- Criar: `src/infrastructure/services/__init__.py`
- Atualizar imports em: `src/application/telhado_detection/service.py`

### STEP 2: Migrar roof_service types
```
src/services/roof_service.py (tipos/dataclasses)
  ↓
src/domain/telhado/value_objects.py ou entities.py
```
- Extrair classes de dados
- Colocá-las em `src/domain/telhado/`
- Atualizar imports

### STEP 3: Deletar 8 services não utilizados
```
rm src/services/{
    anomaly_detection,
    cache_service,
    google_maps_service,
    image_service,
    inpe_service,
    profile_calibration,
    solar_panel_service,
    synthetic_load
}.py
```

### STEP 4: Deletar endpoints legacy
- Remover: `src/api/transformador_pipeline.py`
- Remover: `src/api/telhado_multifonte.py`
- Remover imports em: `src/main.py`

### STEP 5: Consolidar em DDD
- Usar apenas: `/api/v1/telhados` (api/telhados.py)
- Usar apenas: `/api/v1/transformadores` (api/transformadores.py)
- Usar apenas: `/api/v1/subestacoes` (api/subestacoes.py)

### STEP 6: Deletar src/services
```
rm -r src/services/
```

---

## ⚠️ Riscos & Considerações

| Risco | Impacto | Mitigação |
|-------|--------|-----------|
| Deletar endpoints legacy | Quebra clientes | Documentar migração para /api/v1/ |
| Mover RoofDetectionService | Quebra imports | Atualizar todos os 3+ arquivos |
| Mover roof_service types | Quebra imports | Criar redirects ou organizar types |

---

## 📋 Checklist de Migração

- [ ] STEP 1: Criar `src/infrastructure/services/`
- [ ] STEP 1: Mover `roof_detection_service.py`
- [ ] STEP 1: Atualizar imports em `telhado_detection/service.py`
- [ ] STEP 2: Extrair types de `roof_service.py`
- [ ] STEP 2: Migrar para `src/domain/telhado/`
- [ ] STEP 3: Deletar 8 services não utilizados
- [ ] STEP 4: Deletar `src/api/transformador_pipeline.py`
- [ ] STEP 4: Deletar `src/api/telhado_multifonte.py`
- [ ] STEP 4: Atualizar `src/main.py`
- [ ] STEP 5: Testar endpoints `/api/v1/telhados` e `/api/v1/transformadores`
- [ ] STEP 6: Deletar `src/services/` completamente
- [ ] Validar: Nenhuma import de services restante
- [ ] Validar: API inicia sem erros

---

## 📝 Recomendação Final

**Implementar OPÇÃO B**: Refatoração completa com extração de dependências antes de deletar.

**Motivo**:
1. Preserva funcionalidades ML críticas
2. Coloca tipos no lugar correto (domain)
3. Coloca infraestrutura no lugar correto (infrastructure/services)
4. Elimina ambigüidade de arquitetura
5. Prepara para futuro crescimento

**Tempo Estimado**: 30-45 minutos

---

## 🎯 Próximo Passo

Confirme se deseja proceder com:
- **OPÇÃO A**: Deletar tudo imediatamente (quebra endpoints)
- **OPÇÃO B**: Refatoração completa (recomendado)
- **OPÇÃO C**: Deletar apenas não-utilizados por enquanto

