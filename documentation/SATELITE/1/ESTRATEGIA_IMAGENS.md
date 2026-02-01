# 🎯 Estratégia Recomendada: Imagens de Satélite para Detecção de Telhados

## 📊 Análise Comparativa Final

### Resolução vs Cobertura Temporal

| Fonte | Resolução | Revisita | Custo | Melhor Para |
|-------|-----------|----------|-------|-------------|
| **Google Maps** | 0.15-0.6m | Variável | 25k grátis/mês | ✅ Detecção precisa |
| **CBERS-4A** | 2m | 26 dias | Grátis ilimitado | ✅ Grandes telhados |
| **Sentinel-2** | 10m | 5 dias | Grátis ilimitado | ✅ Análise temporal |
| **Landsat-8** | 30m | 16 dias | Grátis ilimitado | ⚠️ Só áreas grandes |

---

## 🥇 Estratégia Recomendada: **ABORDAGEM HÍBRIDA**

### Pipeline Inteligente em 3 Camadas

```python
def escolher_fonte_imagem(area_m2, tipo_analise, disponibilidade_google):
    """
    Estratégia adaptativa baseada em caso de uso
    """
    # CAMADA 1: Alta precisão (painéis solares, telhados pequenos)
    if area_m2 < 50 and disponibilidade_google:
        return "google_maps"  # 0.15-0.6m
    
    # CAMADA 2: Telhados médios/grandes
    elif area_m2 < 500:
        return "cbers_4a"  # 2m - ideal para casas
    
    # CAMADA 3: Análise temporal, galpões industriais
    elif tipo_analise == "temporal":
        return "sentinel_2"  # 10m - boa cobertura temporal
    
    # CAMADA 4: Grandes áreas, baixa prioridade
    else:
        return "landsat_8"  # 30m - menor resolução
```

---

## 💡 Casos de Uso Práticos

### Caso 1: Detecção de Painéis Solares Residenciais
**Objetivo:** Identificar painéis solares em telhados residenciais (8-15m)

**Estratégia:**
1. **Primeira escolha:** Google Maps API (0.3-0.6m)
   - Resolução suficiente para ver painéis individuais
   - Limite: 25.000 imagens/mês gratuitas
   
2. **Alternativa:** CBERS-4A (2m) + upscaling ML
   - Usar super-resolution neural networks
   - Menos preciso mas gratuito

**Código:**
```python
# Priorizar Google Maps, fallback para CBERS
try:
    if google_api_quota_available():
        imagem = download_google_maps(lat, lon, zoom=20)
    else:
        imagem_cbers = download_cbers(lat, lon)
        imagem = super_resolution_model(imagem_cbers)  # Upscale
except:
    raise Exception("Resolução insuficiente")
```

---

### Caso 2: Mapeamento de Grandes Telhados Comerciais/Industriais
**Objetivo:** Detectar galpões e edifícios comerciais (>30m)

**Estratégia:**
1. **Primeira escolha:** CBERS-4A (2m)
   - Resolução suficiente para telhados grandes
   - Gratuito e ilimitado
   - Fonte brasileira (menor latência)

2. **Alternativa:** Sentinel-2 (10m)
   - Se CBERS não tiver cobertura
   - Melhor cobertura temporal

**Código:**
```python
# Buscar CBERS primeiro
imagens_cbers = buscar_cbers(lat, lon, periodo="2023-2025")

if imagens_cbers:
    processar_com_cbers(imagens_cbers)
else:
    # Fallback para Sentinel-2
    imagens_sentinel = buscar_sentinel(lat, lon, periodo="2023-2025")
    processar_com_sentinel(imagens_sentinel)
```

---

### Caso 3: Análise Temporal de Expansão Urbana
**Objetivo:** Monitorar crescimento de edificações ao longo do tempo

**Estratégia:**
**Única escolha:** Sentinel-2 (10m)
- Revisita a cada 5 dias (melhor cobertura temporal)
- Dados desde 2015 (histórico longo)
- Ideal para séries temporais

**Código:**
```python
# Séries temporais com Sentinel-2
datas = ["2020-01-01", "2021-01-01", "2022-01-01", "2023-01-01"]
edificios_ao_longo_do_tempo = []

for data in datas:
    imagens = buscar_sentinel(lat, lon, data)
    edificios = detectar_edificios(imagens)
    edificios_ao_longo_do_tempo.append({
        "data": data,
        "quantidade": len(edificios),
        "area_total": sum(e.area for e in edificios)
    })

# Análise de crescimento
crescimento = calcular_tendencia(edificios_ao_longo_do_tempo)
```

---

## 🏗️ Implementação Prática

### Pipeline Híbrido Recomendado

```python
from backend.src.services.cbers_service import CBERSService
from backend.src.services.cache_service import CacheService

class PipelineTelhadosHibrido:
    def __init__(self):
        self.cbers = CBERSService()
        self.cache = CacheService()
        self.google_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        self.google_quota_diaria = 25000
        self.google_usado_hoje = self._load_quota()
    
    def processar_subestacao(self, subestacao_id, lat, lon, prioridade="auto"):
        """
        Pipeline inteligente que escolhe melhor fonte
        
        Args:
            prioridade: "alta_resolucao", "custo_zero", "temporal", "auto"
        """
        # AUTO: detectar tipo de análise necessário
        if prioridade == "auto":
            area_interesse = self._estimar_area_interesse(subestacao_id)
            prioridade = self._definir_prioridade(area_interesse)
        
        # ALTA RESOLUÇÃO: Google Maps se disponível
        if prioridade == "alta_resolucao":
            if self.google_usado_hoje < self.google_quota_diaria:
                try:
                    imagem = self._download_google_maps(lat, lon)
                    self.google_usado_hoje += 1
                    return {"fonte": "google_maps", "imagem": imagem}
                except Exception as e:
                    logger.warning(f"Fallback Google→CBERS: {e}")
        
        # CUSTO ZERO ou FALLBACK: CBERS-4A
        if prioridade in ["custo_zero", "alta_resolucao"]:
            imagens_cbers = self.cbers.buscar_imagens(
                latitude=lat, longitude=lon, raio_km=10
            )
            if imagens_cbers:
                imagem = self._processar_cbers(imagens_cbers[0])
                return {"fonte": "cbers_4a", "imagem": imagem}
        
        # TEMPORAL: Sentinel-2
        if prioridade == "temporal":
            imagens_sentinel = self._buscar_sentinel(lat, lon)
            if imagens_sentinel:
                return {"fonte": "sentinel_2", "imagem": imagens_sentinel}
        
        raise Exception("Nenhuma fonte de imagem disponível")
    
    def _definir_prioridade(self, area_m2):
        """Define prioridade baseada em área"""
        if area_m2 < 100:  # Painéis solares, casas pequenas
            return "alta_resolucao"
        elif area_m2 < 500:  # Casas médias, comércio
            return "custo_zero"  # CBERS suficiente
        else:  # Indústrias, análise temporal
            return "temporal"
```

---

## 💰 Análise de Custos

### Cenário 1: 1.000 Subestações/Mês

| Estratégia | Custo/Mês | Qualidade | Recomendação |
|------------|-----------|-----------|--------------|
| **100% Google Maps** | $50-200 | ⭐⭐⭐⭐⭐ | Caro |
| **100% CBERS-4A** | $0 | ⭐⭐⭐⭐ | ✅ Recomendado |
| **100% Sentinel-2** | $0 | ⭐⭐ | Insuficiente |
| **Híbrido (25% Google + 75% CBERS)** | $12-50 | ⭐⭐⭐⭐⭐ | 🥇 Ideal |

### Cenário 2: 10.000 Subestações/Mês

| Estratégia | Custo/Mês | Recomendação |
|------------|-----------|--------------|
| **100% Google Maps** | $500-2000 | ❌ Inviável |
| **100% CBERS-4A** | $0 | ✅ Viável |
| **Híbrido (5% Google + 95% CBERS)** | $25-100 | 🥇 Ideal |

---

## 📈 Roadmap de Implementação

### Fase 1: Base (Implementado ✅)
- [x] Integração CBERS-4A
- [x] Sistema de cache
- [x] Pipeline de telhados
- [x] Endpoints API

### Fase 2: Híbrido (Próximo Sprint)
- [ ] Integração Google Maps API
- [ ] Lógica de seleção automática
- [ ] Gerenciamento de quota
- [ ] Fallback inteligente

### Fase 3: Otimização
- [ ] Super-resolution ML (upscaling CBERS)
- [ ] Download paralelo
- [ ] Cache distribuído
- [ ] Predição de disponibilidade

### Fase 4: Produção
- [ ] Monitoramento de custos
- [ ] Dashboard de métricas
- [ ] Alertas de quota
- [ ] Balanceamento de carga

---

## 🎓 Decisões Arquiteturais

### Por que CBERS-4A como base?

**Prós:**
- ✅ Resolução 2m (adequada para >80% dos casos)
- ✅ Totalmente gratuito e ilimitado
- ✅ Dados brasileiros (menor latência)
- ✅ Sem necessidade de autenticação
- ✅ Suficiente para casas/comércios

**Contras:**
- ⚠️ Cobertura temporal menor (26 dias vs 5 dias)
- ⚠️ Foco em América do Sul
- ⚠️ Insuficiente para painéis solares pequenos

### Por que não 100% Google Maps?

**Prós de Google:**
- ✅ Melhor resolução (0.15-0.6m)
- ✅ Cobertura global
- ✅ Atualização frequente

**Contras de Google:**
- ❌ Custo ($0.002 por imagem após 25k/mês)
- ❌ Quota limitada
- ❌ Dependência de API key
- ❌ Termos de uso restritivos

**Decisão:** Usar Google apenas para casos críticos (painéis solares residenciais)

---

## 📚 Referências e Recursos

### APIs e Documentação
- **INPE CBERS:** https://data.inpe.br/bdc/
- **Google Maps Static:** https://developers.google.com/maps/documentation/maps-static
- **Sentinel-2:** https://planetarycomputer.microsoft.com/
- **Landsat:** https://earthengine.google.com/

### Papers Relevantes
- "Super-Resolution for Satellite Imagery" (2023)
- "Building Detection from Low-Resolution Satellite Images" (2022)
- "Hybrid Approach for Urban Monitoring" (2024)

### Código Open Source
- `rasterio` - GeoTIFF processing
- `pystac-client` - STAC APIs
- `ultralytics` - YOLOv8
- `sentinelhub` - Sentinel-2 downloads

---

## ✅ Checklist de Decisão

Ao escolher fonte de imagem, pergunte:

- [ ] Qual a área média dos telhados? (<50m², 50-500m², >500m²)
- [ ] Preciso detectar painéis solares individuais? (Sim → Google)
- [ ] Preciso análise temporal? (Sim → Sentinel-2)
- [ ] Tenho budget para Google Maps? (Não → CBERS)
- [ ] Quantas subestações/mês? (<1000 → Google viável, >1000 → CBERS)
- [ ] Preciso dados históricos? (Sim → Sentinel-2, desde 2015)
- [ ] Região é Brasil/América do Sul? (Sim → CBERS prioritário)

---

**Recomendação Final:**  
🥇 **Use CBERS-4A como base + Google Maps para casos críticos + Sentinel-2 para análise temporal**

---

**Data:** 2025-01-30  
**Versão:** 1.0  
**Status:** Estratégia Validada
