# 🛰️ Sistema Integrado de Satélites - Arquitetura Final

## 📋 Resumo Executivo

Seu sistema agora possui **gerenciamento inteligente de satélites** com:

✅ **Priorização automática** entre CBERS-4A e Google Maps  
✅ **Busca por POLÍGONO** em vez de raio (mais preciso)  
✅ **Rastreamento de quota** do Google Maps (25k/mês)  
✅ **Banco de dados completo** para auditoria e monitoramento  
✅ **Fallback automático** quando um serviço esgota quota/cobertura  

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                   API REST (FastAPI)                        │
│                  /satelite/imagens/{id}                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│          SatelliteSourceService                             │
│  ✅ Decidir fonte (CBERS-4A vs Google Maps)                │
│  ✅ Verificar quotas e limites                              │
│  ✅ Registrar requisições no banco                          │
│  ✅ Fornecer estatísticas                                   │
└─────────────────────────────────────────────────────────────┘
          ↙                              ↘
┌──────────────────────────┐    ┌──────────────────────────┐
│  INPEServiceV2           │    │  GoogleMapsServiceV2     │
│  (CBERS-4A com busca     │    │  (com tracking de quota) │
│   por POLÍGONO)          │    │                          │
│                          │    │  ⚠️ EM DESENVOLVIMENTO  │
│ • 2m resolução           │    │  • 0.3m resolução        │
│ • Brasil/LatAm           │    │  • Global                │
│ • SEM limite             │    │  • 25k/mês limite        │
└──────────────────────────┘    └──────────────────────────┘
          ↓                              ↓
    STAC INPE API          Google Maps API (Static/StreetView)
    https://data.inpe.br           https://maps.google.com
```

---

## 💾 Banco de Dados

### Tabelas de Rastreamento

#### `requisicoes_satelite_google` (Google Maps)
```sql
- id (PK)
- subestacao_id (FK)
- data_requisicao (TIMESTAMPTZ)
- ano_mes (TEXT) -- Agrupamento mensal
- tipo_requisicao (VARCHAR) -- 'static_map', 'street_view'
- status (VARCHAR) -- 'sucesso', 'erro', 'cancelado'
- bbox (lat/lon) -- Área consultada
- observacoes (TEXT)
```

**Índices:**
- `ano_mes` (para verificar quota mensal)
- `subestacao_id` (para filtrar por SE)
- `status` (para contar sucesso)

---

#### `requisicoes_satelite_cbers4a` (CBERS-4A INPE)
```sql
- id (PK)
- subestacao_id (FK)
- data_requisicao (TIMESTAMPTZ)
- tipo_requisicao (VARCHAR) -- 'busca_poligono', 'download'
- status (VARCHAR) -- 'sucesso', 'erro', 'sem_cobertura'
- data_imagem (TIMESTAMPTZ) -- Quando foi capturada
- cobertura_nuvem_percentual (DOUBLE) -- % nuvens
- resolucao_metros (DOUBLE) -- 2.0 para PAN
- bbox (lat/lon) -- Polígono consultado
- imagem_id (VARCHAR) -- ID no catálogo INPE
- url_download (TEXT) -- Link para download
- tamanho_mb (DOUBLE) -- Tamanho da imagem
```

---

#### `preferencia_satelite_subestacao`
```sql
- subestacao_id (FK, UNIQUE)
- satelite_preferido (VARCHAR) -- 'CBERS-4A' (padrão) ou 'GOOGLE_MAPS'
- usar_google_maps_se_necessario (BOOLEAN) -- Fallback automático
- data_atualizacao (TIMESTAMPTZ)
```

**Padrão:** Todos os SEs preferem CBERS-4A (gratuito, sem limite)

---

#### `quota_satelite_google_mes`
```sql
- ano_mes (TEXT, UNIQUE) -- 'YYYY-MM'
- total_requisicoes (INTEGER)
- requisicoes_sucesso (INTEGER)
- percentual_uso (DOUBLE) -- 0-100%
```

---

### Views para Monitoramento

**`view_quota_google_mes`**
```
Mostra uso mensal de Google Maps (requisições e %quota)
```

**`view_cbers4a_por_subestacao`**
```
Mostra imagens CBERS-4A por SE (total, sucesso, melhor qualidade)
```

**`view_status_requisicoes_satelite`**
```
Comparação geral: Google Maps (com quota) vs CBERS-4A (sem limite)
```

---

## 🔑 Componentes Python

### 1. `SatelliteSourceService` (Orquestrador)

**Responsabilidades:**
- ✅ Decidir qual fonte usar
- ✅ Verificar quota Google Maps
- ✅ Registrar requisições
- ✅ Fornecer estatísticas

**Métodos principais:**
```python
# Decidir qual fonte usar
decisao = service.decidir_fonte_satelite(se_id)
# → {'fonte': 'CBERS-4A', 'pode_usar': True, ...}

# Verificar quota
quota = service.verificar_quota_google_maps()
# → {'pode_usar': True, 'usada': 15000, 'disponivel': 10000, ...}

# Registrar requisição
service.registrar_requisicao_cbers4a(
    subestacao_id=1,
    tipo_requisicao='busca_poligono',
    status='sucesso',
    bbox=(min_lat, min_lon, max_lat, max_lon)
)

# Obter estatísticas
stats = service.obter_estatisticas_satelite()
```

---

### 2. `INPEServiceV2` (CBERS-4A com Polígono)

**Mudança principal:** Busca por POLÍGONO em vez de RAIO

**Como funciona:**
1. Busca polígono `area_cobertura` da subestação no banco
2. Calcula bounding box do polígono
3. Consulta STAC INPE com bbox (não raio)
4. Filtra por nuvens (<=30%)
5. Registra requisição via `SatelliteSourceService`

**Método:**
```python
resultado = inpe_service.buscar_imagens_cbers4a_poligono(
    subestacao_id=1,
    # poligono_wkt=None (busca automaticamente)
    data_inicio='2025-01-01',
    data_fim='2026-01-31',
    cobertura_nuvem_max=30,
    max_imagens=50
)

# Retorna:
{
    'fonte': 'CBERS-4A',
    'imagens_encontradas': 5,
    'imagens': [
        {
            'id': 'CBERS_4A_228062_100_2026_01_15',
            'data': '2026-01-15T...',
            'cobertura_nuvem_percent': 12.5,
            'resolucao_metros': 2.0,
            'banda_pan': 'https://...',  # Download
        }
    ],
    'bbox': (-60.5, -15.8, -60.0, -15.3),
    'status': 'sucesso'
}
```

---

### 3. `GoogleMapsServiceV2` (EM DESENVOLVIMENTO)

Será criado com funcionalidades:
- Busca por POLÍGONO (não raio)
- Tracking de quota (25k/mês)
- Fallback automático para CBERS-4A se exceder quota

---

## 🔄 Fluxo de Uso

### Scenario 1: Requisição Normal (CBERS-4A)

```
1. API recebe: GET /satelite/imagens/1

2. SatelliteSourceService.decidir_fonte_satelite(1)
   → CBERS-4A (padrão, gratuito)

3. INPEServiceV2.buscar_imagens_cbers4a_poligono(1)
   a) Busca polígono de SE 1 no banco
   b) Calcula bbox do polígono
   c) Consulta STAC INPE
   d) Filtra por nuvens (<=30%)
   e) Registra requisição na tabela

4. API retorna lista de imagens
```

---

### Scenario 2: Google Maps com Fallback

```
1. Preferência: GOOGLE_MAPS (definida manualmente)

2. SatelliteSourceService.decidir_fonte_satelite(1)
   a) Verifica quota: 24500/25000 (98%)
   b) Retorna: GOOGLE_MAPS (ainda tem quota)
   
3. GoogleMapsServiceV2.buscar_por_poligono(1)
   a) Busca imagens
   b) Registra na tabela requisicoes_satelite_google
   c) Decrementa quota mensal

4. Se exceder 25k → Próxima requisição usa CBERS-4A
```

---

### Scenario 3: Sem Cobertura → Fallback

```
1. CBERS-4A tentado: SEM COBERTURA

2. INPEServiceV2.buscar_imagens_cbers4a_poligono(1)
   → status: 'sem_cobertura'

3. SatelliteSourceService.decidir_fonte_satelite(1)
   → Se usar_google_maps_se_necessario=TRUE
   → Tenta Google Maps como fallback

4. Resultado: Imagens do Google Maps
```

---

## 📊 Monitoramento via Banco

### Verificar Quota Google Maps Este Mês

```sql
SELECT * FROM view_quota_google_mes
WHERE ano_mes = '2026-01';

-- Resultado:
-- ano_mes    | total | sucesso | erro | percentual_uso
-- 2026-01    | 15000 |   15000 |    0 |     60.0%
```

### Melhores Imagens CBERS-4A

```sql
SELECT * FROM view_cbers4a_por_subestacao
WHERE total_imagens > 0
ORDER BY imagens_sucesso DESC;

-- id | nome        | total_imagens | sucesso | min_nuvem | mais_recente
-- 1  | SE Brasília | 5             | 5       | 8.5%      | 2026-01-15
-- 2  | SE Anápolis | 3             | 3       | 15.2%     | 2026-01-12
```

### Comparar Fontes

```sql
SELECT * FROM view_status_requisicoes_satelite;

-- satelite   | mes     | requisicoes | limite | uso
-- CBERS-4A   | 2026-01 | 328         | NULL   | NULL
-- Google Maps| 2026-01 | 15000       | 25000  | 60.0%
```

---

## 🚀 Próximos Passos

1. **Implementar `GoogleMapsServiceV2`** com busca por polígono
2. **Testar integração** com API endpoints
3. **Configurar scheduler** para monitoramento mensal de quota
4. **Adicionar alertas** quando Google Maps aproxima de 25k
5. **Dashboard** de visualização de requisições

---

## 📝 Exemplo de Uso Completo

```python
from sqlalchemy import create_engine
from services.satellite_source_service import SatelliteSourceService
from services.inpe_service_v2 import INPEServiceV2

# Setup
engine = create_engine("postgresql://...")
sat_service = SatelliteSourceService(engine)
inpe_service = INPEServiceV2(engine, sat_service)

# Para subestação 1
se_id = 1

# 1. Decidir fonte
decisao = sat_service.decidir_fonte_satelite(se_id)
print(f"Usando: {decisao['fonte']}")  # CBERS-4A

# 2. Buscar imagens
resultado = inpe_service.buscar_imagens_cbers4a_poligono(
    se_id,
    data_inicio='2025-01-01',
    data_fim='2026-01-31',
    cobertura_nuvem_max=30
)

# 3. Verificar quota
if decisao['fonte'] == 'GOOGLE_MAPS':
    quota = sat_service.verificar_quota_google_maps()
    print(f"Quota: {quota['usada']}/{quota['limite']}")

# 4. Ver estatísticas
stats = sat_service.obter_estatisticas_satelite()
print(stats)
```

---

## 🎯 Benefícios

| Aspecto | Antes | Depois |
|--------|-------|--------|
| **Fonte** | Apenas Sentinel-2 (10m, ruim) | CBERS-4A (2m, ótimo) + Google Maps |
| **Busca** | Por raio (impreciso) | Por POLÍGONO (exato) |
| **Quota** | N/A | ✅ Rastreado (25k/mês Google Maps) |
| **Fallback** | Nenhum | ✅ Automático entre fontes |
| **Auditoria** | Nenhuma | ✅ Tabelas completas de tracking |
| **Priorização** | Manual | ✅ Automática (CBERS-4A > Google Maps) |

---

## 📚 Arquivos Criados/Modificados

```
✅ infrastructure/database/satelite_tracking.sql  (Novo)
   - Tabelas de tracking
   - Views de monitoramento
   - Funções de registro

✅ etl_pipeline/src/services/satellite_source_service.py  (Novo)
   - Orquestrador principal
   - Verificação de quota
   - Decisão de fonte

✅ etl_pipeline/src/services/inpe_service_v2.py  (Novo)
   - CBERS-4A com busca por polígono
   - Integração com STAC
   - Suporte a tracking

📝 etl_pipeline/src/exemplo_satelites.py  (Novo)
   - 5 exemplos de uso
   - Testes de funcionalidade
```

---

## ❓ FAQ

**P: Qual fonte é a padrão?**  
R: CBERS-4A (gratuito, sem limite)

**P: Como fallback automático funciona?**  
R: Se CBERS-4A sem cobertura OU Google Maps sem quota → usa a outra

**P: Limite de 25k é para quê?**  
R: Google Maps Static/Street View API. CBERS-4A não tem limite.

**P: Posso mudar preferência de uma SE?**  
R: Sim, via `SatelliteSourceService.definir_preferencia_subestacao()`

**P: Como monitoro quota?**  
R: SQL: `SELECT * FROM view_quota_google_mes`

---

**Data:** 31 de janeiro de 2026  
**Versão:** 2.0  
**Status:** ✅ Implementado (CBERS-4A) | 📝 Em desenvolvimento (Google Maps)
