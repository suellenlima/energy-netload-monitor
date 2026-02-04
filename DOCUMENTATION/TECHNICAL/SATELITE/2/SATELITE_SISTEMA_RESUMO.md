# 🛰️ Resumo: Sistema de Satélites Implementado

## ✅ O que foi feito

### 1. **SatelliteSourceService** (Orquestrador Principal)
Arquivo: `etl_pipeline/src/services/satellite_source_service.py`

**Funções:**
- ✅ Decidir automaticamente entre **CBERS-4A** (Brasil, sem limite) ou **Google Maps** (global, 25k/mês)
- ✅ Verificar quota mensal do Google Maps (25.000 requisições)
- ✅ Registrar todas as requisições no banco para auditoria
- ✅ Fornecer estatísticas de uso
- ✅ Gerenciar preferências de satélite por subestação

**Lógica de Priorização:**
```
1️⃣ CBERS-4A (padrão)
   - ✅ Gratuito
   - ✅ SEM limite de requisições
   - ✅ Dados brasileiros
   - ✅ Resolução: 2 metros

2️⃣ Google Maps (fallback)
   - ⚠️ Limite: 25.000 requisições/mês
   - ✅ Cobertura global
   - ✅ Resolução: 0.3 metros
   - ✅ Usado se CBERS-4A sem cobertura
```

---

### 2. **INPEServiceV2** (CBERS-4A com Busca por Polígono)
Arquivo: `etl_pipeline/src/services/inpe_service_v2.py`

**Mudança Principal:** De RAIO para POLÍGONO
- ❌ Antes: Busca em círculo (20km de raio) - impreciso
- ✅ Agora: Busca no polígono da subestação - preciso!

**Fluxo:**
1. Busca polígono `area_cobertura` da SE no banco
2. Calcula bounding box do polígono
3. Consulta STAC INPE com bbox
4. Filtra por cobertura de nuvens (≤30%)
5. Registra requisição automaticamente

---

### 3. **Tabelas de Banco de Dados**
Arquivo: `infrastructure/database/satelite_tracking.sql`

#### Requisições de Satélites

| Tabela | Propósito |
|--------|-----------|
| `requisicoes_satelite_google` | Rastreia Google Maps (quota) |
| `requisicoes_satelite_cbers4a` | Rastreia CBERS-4A (auditoria) |
| `preferencia_satelite_subestacao` | Preferência por SE |
| `quota_satelite_google_mes` | Agregado mensal de quota |

#### Views para Monitoramento

| View | Objetivo |
|------|----------|
| `view_quota_google_mes` | % de quota usado |
| `view_cbers4a_por_subestacao` | Imagens CBERS por SE |
| `view_status_requisicoes_satelite` | Comparação geral |

---

### 4. **Funções SQL Automáticas**
Implementadas na tabela de schema:

```sql
-- Registrar requisição CBERS-4A
SELECT registrar_requisicao_cbers4a(se_id, tipo, status, data, nuvem, ...)

-- Registrar requisição Google Maps
SELECT registrar_requisicao_google_maps(se_id, tipo, status, bbox, ...)

-- Verificar se pode usar Google Maps (em plpgsql)
SELECT pode_usar_google_maps_este_mes()  -- Retorna BOOLEAN
```

---

## 📊 Como Usar

### Exemplo 1: Decidir qual satélite usar

```python
from src.services.satellite_source_service import SatelliteSourceService
from src.core import create_db_engine, load_settings

engine = create_db_engine(settings.database.url)
service = SatelliteSourceService(engine)

# Decidir fonte para SE ID 1
decisao = service.decidir_fonte_satelite(subestacao_id=1)

print(f"Fonte: {decisao['fonte']}")                    # CBERS-4A
print(f"Pode usar: {decisao['pode_usar']}")            # True
print(f"Motivo: {decisao['motivo']}")                 # CBERS-4A padrão
print(f"Resolução: {decisao['resolucao_metros']}m")   # 2.0
```

---

### Exemplo 2: Verificar quota Google Maps

```python
# Verificar se ainda tem quota este mês
quota = service.verificar_quota_google_maps()

print(f"Requisições usadas: {quota['usada']}/{quota['limite']}")  # 15000/25000
print(f"Percentual: {quota['percentual_uso']:.1f}%")             # 60.0%
print(f"Pode usar: {quota['pode_usar']}")                        # True
print(f"Disponível: {quota['disponivel']}")                      # 10000
```

---

### Exemplo 3: Buscar imagens CBERS-4A por polígono

```python
from src.services.inpe_service_v2 import INPEServiceV2

inpe = INPEServiceV2(engine, satellite_source_service=service)

# Buscar imagens da SE 1 (por polígono, não raio!)
resultado = inpe.buscar_imagens_cbers4a_poligono(
    subestacao_id=1,
    data_inicio='2025-01-01',
    data_fim='2026-01-31',
    cobertura_nuvem_max=30  # Máximo 30% de nuvens
)

print(f"Imagens encontradas: {resultado['imagens_encontradas']}")  # 5
print(f"Bbox: {resultado['bbox']}")                                # (-60.5, -15.8, ...)
print(f"Status: {resultado['status']}")                            # sucesso

for img in resultado['imagens']:
    print(f"  - {img['id']}: {img['cobertura_nuvem_percent']}% nuvens")
```

---

### Exemplo 4: Ver estatísticas

```python
# Estatísticas do mês
stats = service.obter_estatisticas_satelite()

print(f"Google Maps:")
print(f"  Total: {stats['google_maps']['total']}")
print(f"  % de quota: {stats['google_maps']['percentual_usado']:.1f}%")

print(f"CBERS-4A:")
print(f"  Total: {stats['cbers4a']['total']}")
print(f"  Sucesso: {stats['cbers4a']['sucesso']}")
```

---

## 🔍 Monitoramento via SQL

### Verificar quota Google Maps

```sql
-- Ver uso este mês
SELECT * FROM requisicoes_satelite_google 
WHERE ano_mes = '2026-01' AND status = 'sucesso';

-- Contar: deve ser ≤ 25000
SELECT COUNT(*) FROM requisicoes_satelite_google 
WHERE ano_mes = '2026-01' AND status = 'sucesso';
```

### Ver imagens CBERS-4A

```sql
-- Melhores imagens (menos nuvens)
SELECT id, data_requisicao, cobertura_nuvem_percentual
FROM requisicoes_satelite_cbers4a
WHERE subestacao_id = 1
ORDER BY cobertura_nuvem_percentual ASC;
```

### Dashboard rápido

```sql
SELECT * FROM view_status_requisicoes_satelite;

-- Resultado:
-- satelite    | mes     | requisicoes | limite | uso
-- CBERS-4A    | 2026-01 | 500         | NULL   | NULL
-- Google Maps | 2026-01 | 15000       | 25000  | 60.0%
```

---

## ⚙️ Configuração por Subestação

Mudar preferência de uma SE (padrão é CBERS-4A):

```python
# Preferir Google Maps para SE 42
service.definir_preferencia_subestacao(
    subestacao_id=42,
    satelite_preferido='GOOGLE_MAPS'
)

# Volta para padrão (CBERS-4A)
service.definir_preferencia_subestacao(
    subestacao_id=42,
    satelite_preferido='CBERS-4A'
)
```

---

## 🚨 Alertas e Limitações

### Google Maps Aproximando de Limite

```python
quota = service.verificar_quota_google_maps()

if quota['percentual_uso'] > 80:
    print("⚠️ AVISO: Google Maps em 80% de quota!")
    print(f"   Usado: {quota['usada']}/{quota['limite']}")
```

### Sem Cobertura de Satélite

O sistema automáticamente fará fallback:
```
CBERS-4A sem cobertura? → Tenta Google Maps
Google Maps sem quota?   → Tenta CBERS-4A
Ambos sem opção?         → status = 'sem_cobertura'
```

---

## 📁 Arquivos Criados

```
✅ etl_pipeline/src/services/satellite_source_service.py
   - Orquestrador principal (700 linhas)

✅ etl_pipeline/src/services/inpe_service_v2.py  
   - CBERS-4A com polígono (350 linhas)

✅ infrastructure/database/satelite_tracking.sql
   - Schema + 4 tabelas + 4 views + 4 funções

✅ etl_pipeline/src/exemplo_satelites.py
   - 5 exemplos práticos de uso

✅ documentation/SATELITE/ARQUITETURA_SISTEMA_SATELITES.md
   - Documentação completa
```

---

## 🎯 Próximas Etapas

1. **GoogleMapsServiceV2** (em desenvolvimento)
   - Busca por polígono
   - Tracking de quota
   - Integração com Google Maps API

2. **API Endpoints**
   - `GET /satelite/imagens/{subestacao_id}`
   - `GET /satelite/quota`
   - `POST /satelite/preferencia/{se_id}`

3. **Scheduler de Monitoramento**
   - Alerta quando quota Google Maps > 80%
   - Reset automático no 1º de cada mês

4. **Dashboard**
   - Visualizar requisições por satélite
   - Mostrar melhor cobertura de nuvens
   - Timeline de imagens

---

## 📞 Suporte

Dúvidas sobre:
- **Quota Google Maps:** `service.verificar_quota_google_maps()`
- **Imagens CBERS-4A:** `inpe_service.buscar_imagens_cbers4a_poligono()`
- **Estatísticas:** `service.obter_estatisticas_satelite()`
- **Preferências:** `service.definir_preferencia_subestacao()`

---

**Data:** 31 de janeiro de 2026  
**Status:** ✅ CBERS-4A implementado | 📝 Google Maps em desenvolvimento  
**Limite de Requisições:** 25.000/mês (Google Maps apenas)
