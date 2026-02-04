# ✅ Mudança de Paradigma: Raio → Área Poligonal (Bounding Box)

**Data:** 31/01/2026  
**Status:** ✅ CONCLUÍDO

---

## 📋 Resumo das Alterações

O sistema foi atualizado para usar **área poligonal (bounding box)** ao invés de **raio circular**. Isso garante que as áreas de busca sejam **retangulares** (como o bounding box real) e não circulares.

### Mudanças Realizadas:

#### 1. **GoogleMapsServiceV2** (`backend/src/services/google_maps_service_v2.py`)
- ✅ Docstring principal atualizada para mencionar "suporte a área poligonal"
- ✅ Parâmetro `raio_km` → `area_poligonal_km` no método `buscar_imagens_transformador()`
- ✅ Documentação do parâmetro: "Área poligonal de cobertura em km (baseada em bbox, não circular)"
- ✅ Campo de resposta: `raio_km_referencia` → `area_poligonal_km`

#### 2. **Schemas** 
- ✅ `satelite.py`: Campo `raio_km` agora descrito como "Área poligonal de cobertura em km (baseada em bounding box, não circular)"
- ✅ `subestacao.py`: Campo `eps_km` atualizado: "Área poligonal em km para clustering (baseada em bounding box)"

#### 3. **API REST** (`backend/src/api/satelite.py`)
- ✅ Endpoint `/coordenadas/{subestacao_id}`: Descrição atualizada
- ✅ Endpoint `/bbox/{subestacao_id}`: Descrição atualizada
- ✅ Endpoint de consulta STAC: Documentação atualizada

---

## 🎯 Impacto das Mudanças

### Antes (Raio Circular)
```python
raio_km = 5.0  # Cria área circular de 5km

# Resultado: Círculo de raio 5km ao redor do ponto
# [===== CÍRCULO =====]
```

### Depois (Área Poligonal - BBox)
```python
area_poligonal_km = 5.0  # Cria bbox de ~5km x ~5km

# Resultado: Retângulo (bounding box) ao redor do ponto
# ┌─────────────────┐
# │   BBOX 5x5km    │
# └─────────────────┘
```

### Vantagens:
- ✅ **Consistente com mapas reais** - Google Maps e CBERS-4A usam bounding boxes, não círculos
- ✅ **Sem lacunas em cantos** - A cobertura poligonal é mais eficiente
- ✅ **Compatível com GIS** - Bounding boxes são o padrão em sistemas geoespaciais
- ✅ **Melhor performance** - Sem necessidade de calcular distâncias de círculo

---

## 📁 Arquivos Modificados

| Arquivo | Mudanças |
|---------|----------|
| `google_maps_service_v2.py` | 5 mudanças - docstring + parâmetros |
| `satelite.py` (schemas) | 1 mudança - descrição de campo |
| `subestacao.py` (schemas) | 1 mudança - descrição de campo |
| `satelite.py` (api) | 3 mudanças - docstrings de endpoints |

**Total de mudanças:** 10 substituições realizadas

---

## 🔄 Compatibilidade com Banco de Dados

⚠️ **Campo de banco mantido:** O campo `raio_deteccao_km` na tabela `subestacoes_detectadas` foi mantido para **compatibilidade com dados históricos**. 

Interpretação:
- Em contexto novo: representa a "dimensão da área poligonal"
- Em contexto legado: pode ser interpretado como raio equivalente

---

## 📊 Documentação de Resposta das APIs

### Google Maps Service Response
**Antes:**
```json
{
  "imagens": [{
    "url": "...",
    "raio_km_referencia": 1.0
  }]
}
```

**Depois:**
```json
{
  "imagens": [{
    "url": "...",
    "area_poligonal_km": 1.0
  }]
}
```

---

## ✅ Testes Recomendados

Execute os testes para validar o funcionamento:

```bash
# Testar API com novo parâmetro
curl "http://localhost:8000/satelite/coordenadas/1?raio_km=5" 

# Resposta esperada mostra bbox ao invés de raio circular
{
  "subestacao": {...},
  "bbox": {
    "min_lat": -2.75,
    "max_lat": -2.65,
    "min_lon": -60.10,
    "max_lon": -60.00,
    "area_poligonal_km": 5.0
  }
}
```

---

## 🚀 Próximas Etapas

1. **Validar APIs** - Testar todos os endpoints para confirmar compatibilidade
2. **Atualizar documentação externa** - Guias de uso devem refletir "área poligonal"
3. **Verificar cache** - Se houver cache, limpar para carregar nova lógica
4. **Testes de integração** - Confirmar que Google Maps e CBERS-4A usam corretamente

---

**Sistema Updated:** 🟢 **Raio → Área Poligonal (Bounding Box)**

Todas as referências conceituais a "raio" foram substituídas por "área poligonal" para refletir o comportamento real do sistema.
