# 🛰️ Sistema de Satélites - Diagrama Visual Completo

## 1️⃣ Decisão de Fonte (Fluxo Principal)

```
┌─────────────────────────────────────────────────────────┐
│  Requisição de Imagem (SE ID = 1)                       │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  SatelliteSourceService.decidir_fonte_satelite(1)      │
└─────────────────────────────────────────────────────────┘
                          ↓
                    ┌─────────────┐
                    │  Preferência│
                    │  da SE?     │
                    └─────────────┘
                    /             \
          CBERS-4A (90%)    GOOGLE_MAPS (10%)
          /                           \
    ┌──────────────┐        ┌─────────────────┐
    │CBERS-4A      │        │Verificar Quota  │
    │sempre OK!    │        │Google Maps      │
    │(sem limite)  │        └─────────────────┘
    └──────────────┘             /        \
         ↓                    Tem       Sem
    INPEServiceV2         Quota       Quota
    busca_imagens         ↓            ↓
                     GOOGLE_MAPS   CBERS-4A
                      (fallback)    (fallback)
```

---

## 2️⃣ Busca por Polígono (Nova Abordagem)

### Antes (❌ RAIO - Impreciso)
```
    Subestação
         ⊕
       / | \  ← Raio 20km
      /  |  \
     +   |   +
     |   |   |
     +   |   +
      \  |  /
       \ | /
    
    Problema: Busca em área circular
    - Incluir SEs vizinhas por engano
    - Incluir áreas sem cobertura
    - Perder dados nas pontas do polígono
```

### Agora (✅ POLÍGONO - Preciso)
```
    ╔═══════════════════╗
    ║ Polígono Real da  ║
    ║ Subestação        ║
    ║  ╱╲╱╲╱╲╱╲        ║
    ║ ╱  ╲╱  ╲╱        ║
    ║╱   Exato!        ║
    ╚═══════════════════╝
    
    Vantagens:
    ✅ Busca na área exata
    ✅ Sem dados de outras SEs
    ✅ Sem desperdício de busca
    ✅ Mais barato (menos requisições)
```

---

## 3️⃣ Arquitetura de Banco de Dados

```
┌──────────────────────────────────────────────────────────────┐
│                      REQUISIÇÕES                             │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────────┐      ┌──────────────────────┐      │
│  │requisicoes_satelite │      │requisicoes_satelite  │      │
│  │     GOOGLE          │      │      CBERS4A         │      │
│  ├─────────────────────┤      ├──────────────────────┤      │
│  │ id (PK)             │      │ id (PK)              │      │
│  │ subestacao_id (FK)  │      │ subestacao_id (FK)   │      │
│  │ ano_mes (TEXT)      │◄────►│ data_requisicao      │      │
│  │ tipo_requisicao     │      │ tipo_requisicao      │      │
│  │ status              │      │ status               │      │
│  │ bbox (lat/lon)      │      │ bbox (lat/lon)       │      │
│  │ observacoes         │      │ imagem_id            │      │
│  └─────────────────────┘      │ cobertura_nuvem      │      │
│         △                      │ url_download         │      │
│         │                      └──────────────────────┘      │
│         │                               △                    │
│    3-6 consultas/dia            50+ consultas/dia            │
│    (teste)                       (produção)                  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│                    MONITORAMENTO                             │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────┐    ┌──────────────────────┐       │
│  │ quota_satelite       │    │preferencia_satelite  │       │
│  │ google_mes           │    │subestacao            │       │
│  ├──────────────────────┤    ├──────────────────────┤       │
│  │ ano_mes (UNIQUE)     │    │subestacao_id (UNIQUE)│       │
│  │ total_requisicoes    │    │satelite_preferido    │       │
│  │ requisicoes_sucesso  │    │usar_google_fallback  │       │
│  │ percentual_uso       │    │data_atualizacao      │       │
│  └──────────────────────┘    └──────────────────────┘       │
│        ↑                              ↑                      │
│   Atualizado                   Gerenciado por               │
│   automaticamente          SatelliteSourceService           │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## 4️⃣ Views de Monitoramento

### View 1: Quota Google Maps Mensal

```sql
view_quota_google_mes

ano_mes    total  sucesso  erro  percentual_limite
────────────────────────────────────────────────
2026-01    15000  15000     0      60.0%  ✅
2026-02     8500   8500     0      34.0%  ✅
2026-03    25000  25000     0     100.0%  ⚠️ LIMITE!
```

---

### View 2: CBERS-4A por Subestação

```sql
view_cbers4a_por_subestacao

id  nome           total  sucesso  min_nuvem  imagem_recente
─────────────────────────────────────────────────────────────
1   SE Brasília     5      5        8.5%      2026-01-15
2   SE Anápolis     3      3       15.2%      2026-01-12
3   SE Goiânia      8      7       12.1%      2026-01-18
```

---

### View 3: Comparação Geral

```sql
view_status_requisicoes_satelite

satelite     mes     requisicoes  limite  %uso
─────────────────────────────────────────────
CBERS-4A     2026-01      328      NULL   NULL  (SEM LIMITE)
Google Maps  2026-01    15000    25000   60.0%
```

---

## 5️⃣ Fluxo Completo: Do API ao Banco

```
1. API GET /satelite/imagens/1
   ├─ Recebe subestacao_id = 1
   └─ Passa para SatelliteSourceService

2. SatelliteSourceService.decidir_fonte_satelite(1)
   ├─ Busca preferência no BD
   │  └─ Padrão: CBERS-4A
   ├─ Se CBERS-4A: Retorna logo
   ├─ Se Google Maps: Verifica quota
   │  ├─ Quota OK? → Usa Google Maps
   │  └─ Sem quota? → Fallback CBERS-4A
   └─ Retorna: {'fonte': 'CBERS-4A', ...}

3. INPEServiceV2.buscar_imagens_cbers4a_poligono(1)
   ├─ Busca polígono SE 1 no BD
   │  └─ SELECT ST_AsText(area_cobertura)
   ├─ Calcula bbox do polígono
   ├─ Consulta STAC INPE
   │  └─ GET https://data.inpe.br/bdc/stac/v1
   ├─ Filtra: nuvens <= 30%
   ├─ Registra requisição CBERS-4A
   │  └─ INSERT requisicoes_satelite_cbers4a
   └─ Retorna lista de imagens

4. SatelliteSourceService.registrar_requisicao_cbers4a()
   ├─ INSERT requisicoes_satelite_cbers4a
   └─ Atualiza view_status_requisicoes_satelite

5. API retorna JSON com imagens
   {
     "fonte": "CBERS-4A",
     "imagens": [
       {
         "id": "CBERS_4A_...",
         "data": "2026-01-15",
         "nuvens": 12.5,
         "resolucao": 2.0
       }
     ]
   }

6. Cliente baixa imagens
   └─ GET banda_pan/banda_red/banda_blue
```

---

## 6️⃣ Matriz de Decisão

```
┌─────────────────────────────────────────────────────────────┐
│             DECISÃO: QUAL SATÉLITE USAR?                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  CBERS-4A DISPONÍVEL?                                        │
│  ├─ SIM  → CBERS-4A (sempre preferido)                       │
│  └─ NÃO  → Google Maps (se tiver quota)                      │
│                                                               │
│  GOOGLE MAPS DISPONÍVEL?                                     │
│  ├─ SIM  → Google Maps (menos que 25k/mês)                   │
│  ├─ NÃO  → CBERS-4A fallback                                 │
│  └─ LIMITE ATINGIDO → Bloqueia requisição                    │
│                                                               │
│  NENHUM DISPONÍVEL?                                          │
│  └─ Status: 'sem_cobertura' + Retorna NULL                   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 7️⃣ Ciclo de Vida de Uma Requisição

```
GOOGLE MAPS REQUISIÇÃO (Exemplo)
═════════════════════════════════

T+0s:  APP → API: "Busca imagens para SE 42"

T+1s:  API → SatelliteSourceService
       "Qual fonte usar para SE 42?"

T+2s:  SatelliteSourceService → BD
       SELECT preferencia... (GOOGLE_MAPS)
       SELECT COUNT(*) requisicoes_janeiro... (15000)
       
T+3s:  SatelliteSourceService → APP
       ✅ "Pode usar Google Maps (10000 disponíveis)"

T+4s:  APP → GoogleMapsAPI
       GET maps.google.com/api/staticmap?bbox=...

T+5s:  GoogleMapsAPI → APP
       Retorna imagem PNG

T+6s:  APP → BD
       INSERT requisicoes_satelite_google
       (subestacao_id=42, status='sucesso', ...)

T+7s:  BD → APP
       ✅ Requisição registrada (ID: 12345)

T+8s:  APP → Cliente
       Retorna JSON com imagem + metadata
```

---

## 8️⃣ Quota Google Maps - Visualização Mensal

```
JANEIRO 2026
════════════════════════════════════════════════════════════

0%    25%    50%    75%    100%
│────────────────────│─────────│────────────────────│
                     │         │
                 15000      20000       25000
              USADO      ÷ 2    LIMITE

Status: ✅ Ainda tem 10.000 requisições disponíveis
Alerta: Se usar >25.000 → Fallback automático para CBERS-4A


MARÇO 2026 (LIMITE ATINGIDO)
════════════════════════════════════════════════════════════

0%    25%    50%    75%    100%
│─────────────────────────────────────────────────────│
                                                 ⚠️ 25000

Status: ⚠️ LIMITE ATINGIDO
Próximas requisições: Usarão CBERS-4A
```

---

## 9️⃣ Integração com API (Exemplo)

```python
from fastapi import APIRouter
from sqlalchemy import create_engine
from services.satellite_source_service import SatelliteSourceService
from services.inpe_service_v2 import INPEServiceV2

router = APIRouter()
engine = create_engine("postgresql://...")
sat_service = SatelliteSourceService(engine)
inpe_service = INPEServiceV2(engine, sat_service)

@router.get("/satelite/imagens/{subestacao_id}")
async def buscar_imagens_satelite(subestacao_id: int):
    """Busca melhor imagem de satélite para subestação"""
    
    # 1. Decidir fonte
    decisao = sat_service.decidir_fonte_satelite(subestacao_id)
    
    if not decisao['pode_usar']:
        return {"erro": "Sem satélites disponíveis"}
    
    # 2. Buscar imagens
    if decisao['fonte'] == 'CBERS-4A':
        resultado = inpe_service.buscar_imagens_cbers4a_poligono(subestacao_id)
    else:  # Google Maps
        resultado = google_service.buscar_imagens_por_poligono(subestacao_id)
    
    # 3. Retornar
    return {
        "fonte": decisao['fonte'],
        "resolucao": decisao['resolucao_metros'],
        "imagens": resultado['imagens'],
        "status": resultado['status']
    }

@router.get("/satelite/quota")
async def obter_quota():
    """Retorna quota Google Maps do mês"""
    quota = sat_service.verificar_quota_google_maps()
    return quota

@router.get("/satelite/stats")
async def obter_estatisticas():
    """Retorna estatísticas de uso"""
    stats = sat_service.obter_estatisticas_satelite()
    return stats
```

---

## 🔟 Resumo de Tabelas

| Tabela | Tipo | Registros | Propósito |
|--------|------|-----------|----------|
| `requisicoes_satelite_google` | TIMESERIES | 15k/mês | Quota control |
| `requisicoes_satelite_cbers4a` | TIMESERIES | 50k+/mês | Auditoria |
| `preferencia_satelite_subestacao` | DIMENSION | 1.715 | Preferências |
| `quota_satelite_google_mes` | FACT | 12 | Agregado mensal |
| `view_quota_google_mes` | VIEW | ∞ | Monitoramento |
| `view_cbers4a_por_subestacao` | VIEW | ∞ | Análise |
| `view_status_requisicoes_satelite` | VIEW | ∞ | Dashboard |

---

**Data:** 31 de janeiro de 2026
