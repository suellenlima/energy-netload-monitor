# 📋 RESUMO TÉCNICO - Detecção de Subestações

## 🎯 Objetivo
Implementar sistema completo de **detecção de subestações de distribuição** usando duas abordagens:
1. **Dados oficiais ONS** (públicos)
2. **Clustering geoespacial automático** (DBSCAN)

---

## 📁 Arquivos Modificados/Criados

### 1. Base de Dados
```
infrastructure/database/schema.sql
├─ ✨ Nova tabela: subestacoes_ons
│  └─ Armazena subestações oficiais do ONS
├─ ✨ Nova tabela: subestacoes_detectadas
│  └─ Armazena subestações detectadas via clustering
└─ ✨ Novos índices para ambas as tabelas
```

### 2. ETL Pipeline
```
etl_pipeline/src/extractors/
└─ ✨ subestacoes_client.py (NOVO)
   ├─ download_gd_csv() - Stub para API real
   ├─ _get_mock_subestacoes() - Dados de demonstração
   ├─ extract_subestacoes_data() - Extração
   ├─ transform_subestacoes_data() - Transformação em GeoDataFrame
   ├─ load_subestacoes_data() - Carregamento no DB
   └─ run_extraction() - Orquestração
```

### 3. Backend

#### Serviço de Clustering
```
backend/src/services/
└─ ✨ subestacoes_clustering.py (NOVO)
   ├─ detect_subestacoes_by_clustering() - Função principal
   ├─ _fetch_gd_locations() - Busca pontos de GD
   ├─ _run_dbscan_clustering() - Executa DBSCAN
   ├─ _generate_subestacao_records() - Gera registros
   ├─ _calculate_max_distance() - Haversine distance
   ├─ _infer_subsistema() - Inferência de subsistema
   └─ load_detected_subestacoes() - Carregamento no DB
```

#### Router API
```
backend/src/api/
├─ ✨ subestacoes.py (NOVO)
│  ├─ GET  /subestacoes/ons - Lista ONS
│  ├─ GET  /subestacoes/detectadas - Lista detectadas
│  ├─ POST /subestacoes/detectadas/atualizar - Executa clustering
│  ├─ GET  /subestacoes/geo - GeoJSON
│  └─ GET  /subestacoes/resumo - Estatísticas
└─ main.py (MODIFICADO)
   └─ Integrado novo router
```

### 4. Frontend

#### Componente Streamlit
```
frontend/src/components/
├─ ✨ subestacoes.py (NOVO)
│  ├─ render_subestacoes_section() - Seção principal
│  ├─ render_tab_subestacoes_ons() - Aba ONS
│  ├─ render_tab_subestacoes_detectadas() - Aba Detectadas
│  ├─ render_tab_mapa_subestacoes() - Aba Mapa
│  ├─ render_resumo_subestacoes() - Resumo
│  └─ atualizar_subestacoes_detectadas() - Acionador
└─ app.py (MODIFICADO)
   └─ Integrado novo componente
```

### 5. Documentação
```
├─ ✨ SUBESTACOES_README.md - Guia completo
└─ ✨ scripts/demo_subestacoes.py - Script de teste
```

---

## 🔄 Fluxo de Dados

### Fluxo 1: Dados ONS
```
subestacoes_client.py
  ↓
extract_subestacoes_data() [mock ou API real]
  ↓
transform_subestacoes_data() [GeoDataFrame]
  ↓
load_subestacoes_data() [PostgreSQL+PostGIS]
  ↓
subestacoes_ons (tabela)
```

### Fluxo 2: Detecção Automática
```
usinas_siga + gd_detalhada
  ↓
detect_subestacoes_by_clustering()
  ├─ _fetch_gd_locations()
  ├─ _run_dbscan_clustering()
  ├─ _generate_subestacao_records()
  └─ load_detected_subestacoes()
  ↓
subestacoes_detectadas (tabela)
```

### Fluxo 3: Visualização
```
Frontend Streamlit
  ├─ render_subestacoes_section()
  │  ├─ Tab: ONS
  │  ├─ Tab: Detectadas
  │  ├─ Tab: Mapa
  │  └─ Resumo
  ↓
API Endpoints
  ├─ /subestacoes/ons
  ├─ /subestacoes/detectadas
  ├─ /subestacoes/geo
  └─ /subestacoes/resumo
```

---

## 🔑 Componentes Principais

### 1. DBSCAN Clustering
**Tecnologia:** scikit-learn DBSCAN

**Parâmetros:**
- `eps_km` (raio): 5 km (ajustável)
- `min_samples` (mínimo): 3 pontos

**Vantagens:**
- ✅ Detecta clusters de qualquer forma
- ✅ Identifica ruído automaticamente
- ✅ Escalável para grandes datasets

**Processo:**
1. Converte coords para Web Mercator (distâncias em metros)
2. Executa DBSCAN
3. Calcula centróide e raio de cada cluster
4. Filtra ruído (label = -1)

### 2. Haversine Distance
**Cálculo:** Distância entre dois pontos na Terra

```python
# Implementado em _calculate_max_distance()
# Usa fórmula de Haversine para raio do cluster
```

### 3. PostGIS Integration
**Geometrias:** Point com EPSG:4326

**Índices:**
- subestacoes_ons (distribuidora, subsistema)
- subestacoes_detectadas (distribuidora, cluster_id)

---

## 📊 Dados de Entrada/Saída

### Entrada
```
usinas_siga:
├─ nome (TEXT)
├─ potencia_kw (DOUBLE)
├─ latitude/longitude (DOUBLE)
└─ fonte (TEXT)

gd_detalhada:
├─ distribuidora (TEXT)
├─ classe (TEXT)
├─ potencia_mw (DOUBLE)
└─ sigla_uf (TEXT)
```

### Saída
```
subestacoes_ons:
├─ nome (TEXT UNIQUE)
├─ sigla_se (TEXT)
├─ tensao_kv (DOUBLE)
├─ subsistema (TEXT)
├─ distribuidora (TEXT)
└─ geom (geometry)

subestacoes_detectadas:
├─ cluster_id (INTEGER)
├─ quantidade_gd (INTEGER)
├─ potencia_total_mw (DOUBLE)
├─ raio_deteccao_km (DOUBLE)
└─ geom (geometry)
```

---

## 🚀 Como Usar

### 1. Carregar Dados ONS
```python
from etl_pipeline.src.extractors.subestacoes_client import run_extraction

resultado = run_extraction()
print(f"Carregadas {resultado} subestações")
```

### 2. Executar Clustering
```bash
curl -X POST "http://localhost:8000/subestacoes/detectadas/atualizar?eps_km=5.0"
```

### 3. Consultar via API
```bash
# Listar ONS
curl "http://localhost:8000/subestacoes/ons?limite=100"

# Listar Detectadas
curl "http://localhost:8000/subestacoes/detectadas?limite=50"

# GeoJSON
curl "http://localhost:8000/subestacoes/geo"
```

### 4. Visualizar no Frontend
- Abrir app Streamlit
- Clicar em "Atualizar Dashboard"
- Ir para seção "⚡ Análise de Subestações"

---

## 📦 Dependências Adicionadas

**Backend:**
```
scikit-learn>=1.0.0
scipy>=1.7.0
geopandas>=0.10.0
shapely>=1.8.0
```

**Frontend:**
```
streamlit>=1.20.0
pandas>=1.5.0
```

---

## ⚠️ Considerações Importantes

### 1. Dados Mock
- Atualmente usando dados mock para subestações
- Para dados reais: obter dataset do ONS e atualizar URL
- Função `_get_mock_subestacoes()` pode ser substituída

### 2. Performance
- Clustering em real-time é OK para ~1000 pontos de GD
- Para mais pontos: considerar processamento assíncrono
- Índices PostGIS melhoram significativamente queries

### 3. Precisão
- Clustering com `eps_km=5` assume SEs em raios de 5 km
- Ajustar parâmetro conforme características da rede
- Validar resultados com dados ONS reais

### 4. Cobertura
- Muitos pontos de GD sem localização? Melhorar dados de entrada
- Poucos clusters? Aumentar `eps_km` ou reduzir `min_samples`

---

## 🔍 Testes Recomendados

```bash
# 1. Verificar schema
psql -c "SELECT * FROM information_schema.tables WHERE table_name LIKE 'subestacao%'"

# 2. Contar registros
psql -c "SELECT COUNT(*) FROM subestacoes_ons"

# 3. Testar API
curl http://localhost:8000/subestacoes/ons

# 4. Executar demo
python scripts/demo_subestacoes.py
```

---

## 📈 Próximos Passos Sugeridos

1. **Integrar dados reais ONS**
   - Encontrar URL oficial
   - Validar formato CSV
   - Teste de parsing

2. **Validação de Clusters**
   - Comparar com malha real
   - Ajustar parâmetros DBSCAN
   - Melhorar inferência de subsistema

3. **Visualizações Avançadas**
   - Mapa interativo com Folium
   - Gráficos de potência por SE
   - Timeline de detecções

4. **Processamento Assíncrono**
   - Celery para clustering em background
   - Webhook para notificações
   - Cache de resultados

---

## 📞 Troubleshooting Rápido

| Problema | Solução |
|----------|---------|
| Nenhuma SE detectada | Aumentar `eps_km` ou reduzir `min_samples` |
| Clustering muito lento | Limitar por distribuidora, aumentar `eps_km` |
| API retorna vazio | Verificar se dados foram carregados no DB |
| Mapa não renderiza | Validar dados de latitude/longitude |

---

**Versão:** 1.0  
**Data:** 2026-01-21  
**Status:** ✅ Pronto para Testes
