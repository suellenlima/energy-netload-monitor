# 🔍 Busca de Imagens por TRANSFORMADOR

## Overview

Sistema de busca de imagens de satélite **por transformador individual** (não por subestação).

**Mudança de paradigma:**
- ❌ Antes: Área da subestação inteira (polígono grande)
- ✅ Agora: Ponto específico do transformador + raio customizável

## Arquitetura

```
BANCO DE DADOS (PostgreSQL + PostGIS)
           ↓
  transformadores (lat, lon)
           ↓
  INPEServiceV2.buscar_imagens_cbers4a_transformador()
           ↓
     Calcula BBOX com raio
           ↓
   Query STAC INPE (CBERS-4A)
           ↓
SatelliteSourceService.registrar_requisicao()
           ↓
  Resultado JSON com imagens
```

## Métodos Principais

### 1. `SatelliteSourceService.decidir_fonte_satelite_transformador()`

Decide qual fonte usar para um transformador.

```python
decisao = sat_service.decidir_fonte_satelite_transformador(
    transformador_id=1,
    preferencia=None  # Pode ser 'CBERS4A' ou 'GOOGLE_MAPS'
)

# Retorna:
{
    'transformador_id': 1,
    'fonte': 'CBERS-4A',
    'pode_usar': True,
    'resolucao_metros': 2,
    'cobertura': 'Brasil',
    'motivo': 'CBERS-4A disponível no Brasil'
}
```

**Lógica de decisão:**
1. Verifica localização do transformador
2. Se no Brasil → CBERS-4A (2m, sem limite)
3. Se fora do Brasil → Google Maps (0.3m, com limite 25k/mês)
4. Verifica quota Google Maps

### 2. `INPEServiceV2.buscar_imagens_cbers4a_transformador()`

Busca imagens do CBERS-4A ao redor de um transformador.

```python
resultado = inpe_service.buscar_imagens_cbers4a_transformador(
    transformador_id=1,
    raio_km=2.0,              # Raio em km (default: 2km)
    cobertura_nuvem_max=25,   # Máximo 25% nuvens
    cloud_cover_tolerance=5,  # Tolerância para cálculo
    max_imagens=50,           # Máximo de imagens a retornar
    limite_dias_passados=30   # Considerar últimos 30 dias
)

# Retorna:
{
    'transformador_id': 1,
    'imagens_encontradas': 5,
    'status': 'sucesso',
    'bbox': (-60.5, -15.8, -60.0, -15.3),
    'raio_km': 2.0,
    'imagens': [
        {
            'id': 'CBERS_4A_PAN_20240101_...',
            'data': '2024-01-01',
            'cobertura_nuvem_percent': 15,
            'resolucao_metros': 2,
            'banda_pan': 'https://...',
            'banda_red': 'https://...',
            'banda_green': 'https://...',
            'banda_blue': 'https://...'
        },
        ...
    ]
}
```

## Entendendo o Raio

A busca usa um **raio em km** ao redor do transformador para gerar bbox.

### Cálculo do BBOX

```
Centro: (lat, lon)
Raio: 2.0 km

Aproximação usada: 1° latitude ≈ 111 km
                  1° longitude ≈ 111 * cos(latitude) km

Para transformador em (-22.5, -60.0) com raio 2km:

delta_lat = 2.0 / 111 ≈ 0.018°
delta_lon = 2.0 / (111 * cos(-22.5°)) ≈ 0.0195°

bbox_min_lat = -22.5 - 0.018 ≈ -22.518
bbox_min_lon = -60.0 - 0.0195 ≈ -60.0195
bbox_max_lat = -22.5 + 0.018 ≈ -22.482
bbox_max_lon = -60.0 + 0.0195 ≈ -59.9805
```

### Raios Recomendados

| Raio | Área (aprox.) | Caso de Uso |
|------|-------|-----------|
| **0.25 km** | ~0.2 km² | Muito preciso, apenas local |
| **0.5 km** | ~0.8 km² | Painel solar específico |
| **1.0 km** | ~3.1 km² | Transformador + vizinhança próxima |
| **1.5 km** | ~7.1 km² | **RECOMENDADO** |
| **2.0 km** | ~12.6 km² | Contexto maior |
| **3.0 km** | ~28.3 km² | Bairro/região |
| **5.0 km** | ~78.5 km² | Grande área urbana |

## Workflow Típico

### Passo 1: Verificar Fonte

```python
from src.services.satellite_source_service import SatelliteSourceService

sat_service = SatelliteSourceService(engine)

decisao = sat_service.decidir_fonte_satelite_transformador(
    transformador_id=1234
)

if not decisao['pode_usar']:
    print(f"❌ Não pode usar: {decisao['motivo']}")
else:
    print(f"✅ Usar {decisao['fonte']}")
```

### Passo 2: Buscar Imagens

```python
from src.services.inpe_service_v2 import INPEServiceV2

inpe = INPEServiceV2(engine, sat_service)

resultado = inpe.buscar_imagens_cbers4a_transformador(
    transformador_id=1234,
    raio_km=1.5,
    cobertura_nuvem_max=20  # Máximo 20% nuvens
)
```

### Passo 3: Processar Resultados

```python
if resultado['status'] == 'sucesso' and resultado['imagens']:
    melhor_imagem = resultado['imagens'][0]  # Ordenadas por qualidade
    
    print(f"📸 Melhor imagem: {melhor_imagem['id']}")
    print(f"   Data: {melhor_imagem['data']}")
    print(f"   Nuvens: {melhor_imagem['cobertura_nuvem_percent']}%")
    print(f"   Resolução: {melhor_imagem['resolucao_metros']}m")
    print(f"   Links:")
    print(f"     - PAN: {melhor_imagem['banda_pan']}")
    print(f"     - Red: {melhor_imagem['banda_red']}")
    print(f"     - Green: {melhor_imagem['banda_green']}")
    print(f"     - Blue: {melhor_imagem['banda_blue']}")
else:
    print(f"❌ Erro: {resultado['status']}")
```

## Exemplos de Código

### Exemplo 1: Busca Simples

```python
from core import create_db_engine, load_settings
from src.services.satellite_source_service import SatelliteSourceService
from src.services.inpe_service_v2 import INPEServiceV2

settings = load_settings()
engine = create_db_engine(settings.database.url)

sat_service = SatelliteSourceService(engine)
inpe = INPEServiceV2(engine, sat_service)

# Buscar para transformador 1001
resultado = inpe.buscar_imagens_cbers4a_transformador(
    transformador_id=1001,
    raio_km=1.5
)

print(f"✅ Encontradas {resultado['imagens_encontradas']} imagens")
```

### Exemplo 2: Busca com Filtros

```python
resultado = inpe.buscar_imagens_cbers4a_transformador(
    transformador_id=1001,
    raio_km=2.0,              # 2km ao redor
    cobertura_nuvem_max=15,   # Máximo 15% nuvens (mais rigoroso)
    max_imagens=10,           # Apenas 10 melhores
    limite_dias_passados=7    # Apenas últimos 7 dias
)
```

### Exemplo 3: Comparar Múltiplos Raios

```python
transformador_id = 1001
raios = [0.5, 1.0, 1.5, 2.0]

for raio in raios:
    resultado = inpe.buscar_imagens_cbers4a_transformador(
        transformador_id=transformador_id,
        raio_km=raio
    )
    print(f"Raio {raio}km: {resultado['imagens_encontradas']} imagens")
```

### Exemplo 4: Lote de Transformadores

```python
transformador_ids = [1001, 1002, 1003, 1004, 1005]

resultados = {}
for trans_id in transformador_ids:
    resultado = inpe.buscar_imagens_cbers4a_transformador(
        transformador_id=trans_id,
        raio_km=1.5
    )
    resultados[trans_id] = resultado
    print(f"T{trans_id}: {resultado['imagens_encontradas']} imagens")

# Estatísticas
total = sum(r['imagens_encontradas'] for r in resultados.values())
print(f"Total: {total} imagens para {len(transformador_ids)} transformadores")
```

## Integração com API (FastAPI)

```python
from fastapi import APIRouter, HTTPException
from src.services.satellite_source_service import SatelliteSourceService
from src.services.inpe_service_v2 import INPEServiceV2

router = APIRouter(prefix="/api/satelite", tags=["satelite"])

@router.get("/imagens/transformador/{transformador_id}")
async def get_satelite_transformador(
    transformador_id: int,
    raio_km: float = 1.5,
    cobertura_nuvem_max: int = 25,
    db: Session = Depends(get_db)
):
    """Buscar imagens de satélite para um transformador"""
    
    inpe = INPEServiceV2(db.get_bind(), SatelliteSourceService(db.get_bind()))
    
    resultado = inpe.buscar_imagens_cbers4a_transformador(
        transformador_id=transformador_id,
        raio_km=raio_km,
        cobertura_nuvem_max=cobertura_nuvem_max
    )
    
    if resultado['status'] != 'sucesso':
        raise HTTPException(status_code=400, detail=resultado['status'])
    
    return resultado
```

## Monitoramento e Quota

### Verificar Quota Google Maps

```python
quota = sat_service.verificar_quota_google_maps()

print(f"Google Maps - Mês {quota['mes_atual']}/{quota['ano_atual']}")
print(f"Usadas: {quota['requisicoes_usadas']} / 25000")
print(f"Disponíveis: {quota['requisicoes_disponiveis']}")
print(f"Percentual: {quota['percentual_usado']:.1f}%")
```

### Ver Estatísticas

```python
stats = sat_service.obter_estatisticas_satelite()

print("CBERS-4A (últimos 30 dias):")
print(f"  Requisições: {stats['cbers4a']['requisicoes']}")
print(f"  Imagens: {stats['cbers4a']['imagens_totais']}")
print(f"  Média/requisição: {stats['cbers4a']['imagens_media']:.2f}")

print("Google Maps (este mês):")
print(f"  Requisições: {stats['google_maps']['requisicoes']}")
print(f"  Quota: {stats['google_maps']['quota_usada']}/25000")
```

## Troubleshooting

### Problema: "Transformador não encontrado"

```
Causa: Transformador_id não existe no banco
Solução: Verificar ID ou listar transformadores disponíveis

# Encontrar transformadores em subestação
SELECT id, latitude, longitude 
FROM transformadores 
WHERE subestacao_id = 1 
LIMIT 5;
```

### Problema: "Nenhuma imagem encontrada"

```
Causa: Sem cobertura CBERS-4A ou fora do período
Solução: 
1. Aumentar raio_km (2.0 ao invés de 1.0)
2. Aumentar cobertura_nuvem_max (30% ao invés de 15%)
3. Aumentar limite_dias_passados (60 ao invés de 30)
```

### Problema: "Quota Google Maps excedida"

```
Causa: Atingiu 25k requisições/mês
Solução:
1. Usar apenas CBERS-4A até próximo mês
2. Reduzir buscas desnecessárias
3. Aguardar renovação de quota (próximo dia 1°)
```

## Performance

### Tempos Típicos (Transformador no Brasil)

| Operação | Tempo |
|----------|-------|
| Decidir fonte | ~50ms |
| Buscar CBERS-4A (1km) | ~300-500ms |
| Buscar CBERS-4A (2km) | ~400-700ms |
| Processar 10 transformadores | ~3-5s |
| Registrar no banco | ~10-20ms |

### Otimizações

1. **Cache de decisões**: Usar mesma fonte para múltiplas buscas
2. **Parallelização**: Buscar múltiplos transformadores em paralelo
3. **Índices BD**: Transformadores indexed por (subestacao_id, latitude, longitude)

## Próximas Funcionalidades

- [ ] GoogleMapsServiceV2 com busca por transformador
- [ ] Endpoint API para múltiplos transformadores
- [ ] Cache local de imagens
- [ ] Comparação de imagens temporais
- [ ] Detecção automática de painéis solares

## Referências

- [INPE STAC API](https://data.inpe.br/bdc/stac/v1/)
- [CBERS-4A Specifications](https://www.inpe.br/cbers/)
- [PostGIS Distance Functions](https://postgis.net/docs/distance_sphere.html)
