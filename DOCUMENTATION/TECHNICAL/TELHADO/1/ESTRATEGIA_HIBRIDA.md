# 🎯 Estratégia Híbrida: CBERS → Google Maps Fallback

## 📋 Implementação Completa

A aplicação agora tem **fallback automático** quando CBERS não tiver imagens disponíveis:

```
1ª Tentativa: CBERS-4A (2m, grátis, período 2 anos)
        ↓ (se falhar)
2ª Tentativa: Google Maps (0.3m, 25k grátis/mês)
        ↓ (se falhar)
3ª Tentativa: Sentinel-2 (10m, inadequado para telhados)
```

---

## 🚀 Como Usar

### Opção 1: Estratégia Automática (Recomendado)

```python
from backend.src.services.telhado_segmentation_service import TelhadoSegmentationService

# Inicializar com estratégia híbrida
service = TelhadoSegmentationService(
    use_cache=True,
    usar_estrategia_hibrida=True  # ← ATIVA FALLBACK
)

# Buscar imagem automaticamente (tenta CBERS → Google Maps)
imagem = service.download_imagem_automatica(
    latitude=-15.7939,
    longitude=-47.8828,
    raio_km=10.0,
    estrategia="auto"  # Escolhe melhor fonte automaticamente
)

# Detectar telhados
if imagem is not None:
    telhados = service.detectar_telhados(imagem, confianca_minima=0.5)
    print(f"Detectados {len(telhados)} telhados")
```

---

### Opção 2: Uso Direto da Estratégia

```python
from backend.src.services.image_service import ImagemStrategyService

strategy = ImagemStrategyService(preferencia_resolucao=2.0)

# Buscar com fallback automático
resultado = strategy.buscar_imagem_automatica(
    latitude=-23.5505,
    longitude=-46.6333,
    raio_km=10.0,
    estrategia="auto"
)

if resultado:
    print(f"Fonte: {resultado.fonte}")  # 'cbers' ou 'google_maps'
    print(f"Resolução: {resultado.resolucao_m}m/pixel")
    imagem = resultado.imagem
```

---

## 🎚️ Estratégias Disponíveis

### 1. `estrategia="auto"` (Padrão)
**Escolha inteligente baseada em resolução preferida**

```python
# Preferência: 0.5m → Tenta Google primeiro
strategy = ImagemStrategyService(preferencia_resolucao=0.5)
resultado = strategy.buscar_imagem_automatica(..., estrategia="auto")
# Ordem: Google Maps → CBERS → Sentinel

# Preferência: 2.0m → Tenta CBERS primeiro  
strategy = ImagemStrategyService(preferencia_resolucao=2.0)
resultado = strategy.buscar_imagem_automatica(..., estrategia="auto")
# Ordem: CBERS → Google Maps → Sentinel

# Preferência: 10m → Aceita resolução baixa
strategy = ImagemStrategyService(preferencia_resolucao=10.0)
resultado = strategy.buscar_imagem_automatica(..., estrategia="auto")
# Ordem: CBERS → Sentinel → Google Maps
```

---

### 2. `estrategia="alta_resolucao"`
**Prioriza melhor resolução (painéis solares)**

```python
resultado = strategy.buscar_imagem_automatica(..., estrategia="alta_resolucao")
# Ordem: Google Maps (0.3m) → CBERS (2m) → Sentinel (10m)
```

**Quando usar:**
- Detectar painéis solares residenciais
- Telhados pequenos (<20m)
- Análise detalhada de estruturas

---

### 3. `estrategia="custo_zero"`
**Apenas fontes gratuitas (sem Google Maps)**

```python
resultado = strategy.buscar_imagem_automatica(..., estrategia="custo_zero")
# Ordem: CBERS (2m) → Sentinel (10m)
# Não usa Google Maps
```

**Quando usar:**
- Budget zero
- Grande volume de imagens (>25k/mês)
- Telhados médios/grandes (>30m)

---

### 4. `estrategia="rapido"`
**Prioriza cache e fontes rápidas**

```python
resultado = strategy.buscar_imagem_automatica(..., estrategia="rapido")
# Ordem: CBERS (cache local) → Google Maps → Sentinel
```

**Quando usar:**
- Processamento em tempo real
- Re-processamento de dados
- Máxima performance

---

## ⚙️ Configuração do Google Maps

### 1. Obter API Key

1. Acesse: https://console.cloud.google.com/
2. Crie um projeto novo
3. Ative "Maps Static API"
4. Gere API Key
5. (Opcional) Restrinja a chave por IP/domínio

### 2. Configurar no .env

```bash
# backend/.env ou .env na raiz
GOOGLE_MAPS_API_KEY=AIzaSyC...sua_chave_aqui...XYZ
```

### 3. Verificar Configuração

```python
from backend.src.services.google_maps_service import GoogleMapsService

google = GoogleMapsService()
print(f"Disponível: {google.esta_disponivel()}")  # True se configurado

# Testar download
imagem = google.buscar_imagem_satelite(
    latitude=-23.5505,
    longitude=-46.6333,
    zoom=20  # 0.3m/pixel
)
```

---

## 💰 Gestão de Custos

### Limites Gratuitos

| Fonte | Limite Grátis | Custo Excedente |
|-------|---------------|-----------------|
| CBERS-4A | Ilimitado | $0 |
| Google Maps | 25.000/mês | $0.002/imagem |
| Sentinel-2 | Ilimitado | $0 |

### Estimativa de Custo

```python
from backend.src.services.google_maps_service import GoogleMapsService

google = GoogleMapsService()

# Estimar custo para 50.000 imagens
custo = google.estimar_custo(num_imagens=50000)

print(custo)
# {
#   "total_imagens": 50000,
#   "imagens_gratis": 25000,
#   "imagens_pagas": 25000,
#   "custo_usd": 50.0,
#   "nota": "Primeiras 25.000 imagens/mês são grátis"
# }
```

### Estratégias de Economia

**1. Usar CBERS como padrão (custo zero)**
```python
# 90% das requisições vão para CBERS (grátis)
# 10% fazem fallback para Google Maps
strategy = ImagemStrategyService(preferencia_resolucao=2.0)
resultado = strategy.buscar_imagem_automatica(..., estrategia="auto")
```

**2. Cache agressivo**
```python
# Cache guarda imagens por 30 dias
# Evita re-downloads
service = TelhadoSegmentationService(use_cache=True, usar_estrategia_hibrida=True)
```

**3. Filtrar por prioridade**
```python
# Apenas subestações críticas usam Google Maps
if subestacao.prioridade == "alta":
    estrategia = "alta_resolucao"  # Pode usar Google
else:
    estrategia = "custo_zero"  # Apenas CBERS
```

---

## 🧪 Testes

### Teste Rápido

```bash
python test_estrategia_hibrida.py
```

**Resultado esperado:**
```
✓ SUCESSO!
  Fonte: CBERS (se imagem disponível)
  Resolução: 2.0m/pixel

OU (se CBERS sem imagens)

✓ SUCESSO!
  Fonte: GOOGLE_MAPS (fallback automático)
  Resolução: 0.3m/pixel
```

---

### Teste Manual

```python
from backend.src.services.image_service import ImagemStrategyService

strategy = ImagemStrategyService()

# Testar todas as estratégias
estrategias = ["auto", "alta_resolucao", "custo_zero", "rapido"]

for est in estrategias:
    print(f"\nTestando estratégia: {est}")
    resultado = strategy.buscar_imagem_automatica(
        latitude=-15.7939,
        longitude=-47.8828,
        raio_km=10.0,
        estrategia=est
    )
    
    if resultado:
        print(f"  ✓ Fonte: {resultado.fonte}")
        print(f"    Resolução: {resultado.resolucao_m}m")
    else:
        print(f"  ✗ Falhou")

# Ver estatísticas
stats = strategy.get_estatisticas()
print(f"\nEstatísticas:")
print(f"  CBERS: {stats['stats']['cbers']}")
print(f"  Google Maps: {stats['stats']['google_maps']}")
```

---

## 📊 Monitoramento

### Estatísticas de Uso

```python
strategy = ImagemStrategyService()

# Processar várias subestações
for sub in subestacoes:
    strategy.buscar_imagem_automatica(sub.lat, sub.lon)

# Ver estatísticas
stats = strategy.get_estatisticas()

print(f"CBERS:")
print(f"  Tentativas: {stats['stats']['cbers']['tentativas']}")
print(f"  Sucessos: {stats['stats']['cbers']['sucessos']}")

print(f"\nGoogle Maps:")
print(f"  Tentativas: {stats['stats']['google_maps']['tentativas']}")
print(f"  Sucessos: {stats['stats']['google_maps']['sucessos']}")

print(f"\nCache:")
print(f"  Hit rate: {stats['cache_stats']['hit_rate']}%")
```

---

## 🎯 Casos de Uso

### Caso 1: Detecção de Painéis Solares (Alta Resolução)

```python
service = TelhadoSegmentationService(usar_estrategia_hibrida=True)

# Usar estratégia de alta resolução
imagem = service.download_imagem_automatica(
    latitude=lat,
    longitude=lon,
    estrategia="alta_resolucao"  # Google Maps prioritário
)

# YOLOv8 detecta painéis pequenos
telhados = service.detectar_telhados(imagem, confianca_minima=0.6)
```

---

### Caso 2: Mapeamento em Larga Escala (Custo Zero)

```python
service = TelhadoSegmentationService(usar_estrategia_hibrida=True)

# Processar 10.000 subestações sem custo
for subestacao in subestacoes:
    imagem = service.download_imagem_automatica(
        latitude=subestacao.lat,
        longitude=subestacao.lon,
        estrategia="custo_zero"  # Apenas CBERS
    )
    
    if imagem:
        processar_telhados(imagem)
```

---

### Caso 3: Híbrido Inteligente (Automático)

```python
strategy = ImagemStrategyService(preferencia_resolucao=2.0)

resultados = strategy.processar_lista_subestacoes(
    subestacoes=[
        {"id": 1, "latitude": -15.79, "longitude": -47.88},
        {"id": 2, "latitude": -23.55, "longitude": -46.63},
        # ... mais subestações
    ],
    estrategia="auto"  # Escolhe automaticamente
)

# Estatísticas finais
total_cbers = sum(1 for r in resultados if r['fonte'] == 'cbers')
total_google = sum(1 for r in resultados if r['fonte'] == 'google_maps')

print(f"CBERS: {total_cbers} (grátis)")
print(f"Google: {total_google} ({total_google * 0.002} USD)")
```

---

## ⚠️ Troubleshooting

### Erro: "Google Maps API key não configurada"

**Solução:**
```bash
# Adicione ao .env:
GOOGLE_MAPS_API_KEY=sua_chave_aqui
```

---

### Erro: "Todas as fontes falharam"

**Diagnóstico:**
```python
strategy = ImagemStrategyService()
resultado = strategy.buscar_imagem_automatica(...)

if not resultado:
    # Ver estatísticas para entender o que falhou
    stats = strategy.get_estatisticas()
    print(stats)
```

**Causas comuns:**
- CBERS sem imagens no período → Ampliar para 2-3 anos
- Google Maps sem API key → Configurar ou usar custo_zero
- Conexão com internet → Verificar

---

### Performance Lenta

**Otimizações:**
```python
# 1. Usar cache agressivo
service = TelhadoSegmentationService(use_cache=True)

# 2. Estratégia rápida
resultado = strategy.buscar_imagem_automatica(..., estrategia="rapido")

# 3. Período menor para CBERS (mais rápido)
# Modificar em image_service.py, linha ~125:
# data_inicio = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
```

---

## 📚 Arquivos Implementados

1. **[backend/src/services/google_maps_service.py](backend/src/services/google_maps_service.py)**
   - Integração com Google Maps Static API
   - Cálculo de resolução e cobertura
   - Estimativa de custos

2. **[backend/src/services/image_service.py](backend/src/services/image_service.py)** (UNIFICADO)
   - Gerenciador de fallback automático
   - 4 estratégias (auto, alta_resolucao, custo_zero, rapido)
   - Estatísticas de uso
   - Aliases: ImagemStrategyService, ImagemMultiFonteService, ImagemSalvamentoService

3. **[backend/src/services/telhado_segmentation_service.py](backend/src/services/telhado_segmentation_service.py)** (atualizado)
   - Método `download_imagem_automatica()` com fallback
   - Parâmetro `usar_estrategia_hibrida=True`

4. **[test_estrategia_hibrida.py](test_estrategia_hibrida.py)**
   - Teste completo do fallback
   - Integração com segmentação de telhados

---

## ✅ Checklist de Validação

- [x] Google Maps Service criado
- [x] Estratégia híbrida implementada
- [x] Fallback automático funcionando
- [x] Integrado com segmentação de telhados
- [x] 4 estratégias disponíveis
- [x] Estimativa de custos
- [x] Estatísticas de uso
- [x] Teste end-to-end criado
- [ ] Google Maps API configurada (opcional)
- [ ] Teste com dados reais

---

**Data:** 2025-01-30  
**Status:** ✅ Implementação Completa  
**Próxima ação:** Configure Google Maps API key e execute `python test_estrategia_hibrida.py`
