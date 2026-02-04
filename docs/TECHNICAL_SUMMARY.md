# 📋 RESUMO TÉCNICO - Sistema de Monitoramento de Carga Líquida

## 🎯 Objetivo
Implementar sistema completo de **monitoramento de carga líquida**, **estimativa de geração distribuída (MMGD)**, **detecção de anomalias** e **análise de subestações** para sistemas elétricos de distribuição:

1. **Análise de Carga e MMGD** - Cálculo de consumo real (carga líquida + geração distribuída)
2. **Detecção de Anomalias** - Identificação automática de padrões anormais
3. **Monitoramento Tempo Real** - Dashboard operacional com auto-refresh
4. **Análise de Subestações** - Dados ONS + clustering geoespacial (DBSCAN)
5. **Auditoria e Integridade** - Histórico de alertas e detecção de fraudes

---

## 📁 Arquivos Modificados/Criados

### 1. Base de Dados
```
infrastructure/database/
├─ schema.sql (ATUALIZADO - 9 tabelas)
│  ├─ ✨ carga_liquida_ons (medições ONS temporais)
│  ├─ ✨ gd_detalhada (geração distribuída)
│  ├─ ✨ usinas_siga (usinas ANEEL)
│  ├─ ✨ estabelecimentos (classes de consumo)
│  ├─ ✨ subestacoes_ons (SEs oficiais ONS)
│  ├─ ✨ subestacoes_detectadas (clustering)
│  ├─ ✨ alertas_fraude (detecção manual/IA)
│  ├─ ✨ load_profiles (perfis EPE/ANEEL)
│  └─ ✨ anomalias_automaticas (detecção automática)
└─ migrations/ (NOVO)
   └─ Histórico de alterações do schema
```

### 2. ETL Pipeline
```
etl_pipeline/src/extractors/
├─ ✨ subestacoes_client.py
│  ├─ extract_subestacoes_data()
│  ├─ transform_subestacoes_data()
│  └─ run_extraction()
│
├─ ✨ bdgd_client.py
│  └─ Extração de dados BDGD (ANEEL)
│
└─ schedulers/ (NOVO)
   └─ Agendamento de jobs ETL
```

### 3. Backend Services

#### Análise de Carga e MMGD
```
backend/src/services/
├─ ✏️ load_calc.py (ATUALIZADO - 470 linhas)
│  ├─ calculate_mmgd_generation() - Estimativa solar
│  ├─ get_latest_load_data() - Dados mais recentes
│  ├─ calculate_real_consumption() - Carga + MMGD
│  └─ fetch_fraud_alert() - Integração anomalias
│
├─ ✨ synthetic_load.py (NOVO - 250 linhas)
│  ├─ generate_synthetic_load_curve() - Curvas sintéticas
│  ├─ _apply_noise_and_variation() - Variabilidade
│  └─ Perfis por classe (residencial, comercial, industrial)
│
├─ ✨ realtime_estimation.py (NOVO - 180 linhas)
│  ├─ estimate_current_state() - Estado atual
│  ├─ get_current_irradiance() - Open-Meteo API
│  └─ _calculate_solar_generation() - Geração solar
│
└─ ✨ profile_calibration.py (NOVO)
   ├─ calibrate_profiles() - Ajuste automático
   └─ load_profiles_from_json() - Carregamento
```

#### Detecção de Anomalias
```
backend/src/services/
└─ ✨ anomaly_detection.py (NOVO - 800 linhas)
   ├─ AnomalyDetector (classe principal)
   ├─ detect_anomalies() - Detecção multi-critério
   ├─ generate_historical_alerts() - Alertas para demo
   ├─ _analyze_distribuidora() - Análise por distribuidora
   ├─ _analyze_load_factor() - Fator de carga
   ├─ _detect_abnormal_peaks() - Picos anormais
   └─ get_latest_alert() - Último alerta
```

#### Clustering de Subestações
```
backend/src/services/
└─ ✨ subestacoes_clustering.py (290 linhas)
   ├─ detect_subestacoes_by_clustering()
   ├─ _fetch_gd_locations()
   ├─ _run_dbscan_clustering()
   ├─ _generate_subestacao_records()
   ├─ _calculate_max_distance()
   └─ load_detected_subestacoes()
```

### 4. Backend API Routers

#### Router de Análise
```
backend/src/api/
├─ ✏️ analise.py (ATUALIZADO - 430 linhas, 8 endpoints)
│  ├─ GET  /analise/carga-oculta
│  ├─ GET  /analise/estabelecimentos
│  ├─ GET  /analise/classes-consumo
│  ├─ GET  /analise/perfil-carga
│  ├─ GET  /analise/simulacao-fraude
│  ├─ GET  /analise/estado-atual (tempo real)
│  ├─ GET  /analise/alertas-historico
│  └─ POST /analise/detectar-anomalias
│
└─ ✨ subestacoes.py (NOVO - 250 linhas, 5 endpoints)
   ├─ GET  /subestacoes/ons
   ├─ GET  /subestacoes/detectadas
   ├─ POST /subestacoes/detectadas/atualizar
   ├─ GET  /subestacoes/geo
   └─ GET  /subestacoes/resumo
```

### 5. Frontend Components

#### Componentes Principais
```
frontend/src/components/
├─ ✨ kpis.py (NOVO - 260 linhas)
│  ├─ render_executive_kpis() - 4 KPIs no topo
│  ├─ _apply_kpi_card_style() - CSS minimalista
│  └─ Padrão ApiResult correto (data, error)
│
├─ ✏️ charts.py (ATUALIZADO - 680 linhas)
│  ├─ load_carga_data() - Carregamento
│  ├─ render_carga_section() - Gráfico principal
│  ├─ render_classes_consumo() - Classes completas
│  ├─ render_classes_consumo_compact() - Versão compacta
│  ├─ render_estabelecimentos_section() - Estabelecimentos
│  ├─ render_estabelecimentos_compact() - Versão compacta
│  └─ render_perfis_carga() - Perfis típicos
│
├─ ✨ realtime.py (NOVO - 350 linhas)
│  ├─ render_realtime_dashboard() - Dashboard operacional
│  ├─ _render_current_metrics() - Métricas atuais
│  ├─ _render_hourly_forecast() - Previsão horária
│  └─ Auto-refresh configurável
│
├─ ✏️ subestacoes.py (ATUALIZADO - 700 linhas)
│  ├─ render_subestacoes_section() - Seção principal
│  ├─ render_analise_local_subestacao() - Análise local
│  ├─ render_tab_subestacoes_ons() - Tab ONS
│  ├─ render_tab_subestacoes_detectadas() - Tab detectadas
│  └─ render_tab_mapa_subestacoes() - Tab mapa
│
├─ ✨ audit.py (NOVO - 270 linhas)
│  ├─ render_auditoria() - Auditoria de fraudes
│  ├─ render_historico_alertas() - Histórico completo
│  └─ Filtros + gráficos de distribuição
│
├─ ✏️ sidebar.py (ATUALIZADO - 180 linhas)
│  ├─ render_sidebar() - Controles principais
│  └─ Filtros (subsistema, distribuidora, multiplicador)
│
└─ alerts.py
   └─ fetch_alerta() - Busca alertas
   └─ render_alerta() - Exibição de alertas
```

#### App Principal
```
frontend/src/
├─ ✏️ app.py (ATUALIZADO - 250 linhas)
│  ├─ 5 tabs principais
│  │  ├─ 📊 Visão Geral
│  │  ├─ ⚡ Tempo Real
│  │  ├─ 🏭 Subestações
│  │  ├─ 📈 Perfis & Análise
│  │  └─ 🔍 Auditoria
│  ├─ KPIs executivos sempre visíveis
│  ├─ Tooltip educativo
│  └─ Breadcrumb navigation
│
└─ services/api_client.py
   └─ ApiClient com ApiResult dataclass
```

### 6. Documentação
```
docs/
├─ ✏️ INDEX.md (ATUALIZADO - 300 linhas)
├─ ✏️ IMPLEMENTACAO_COMPLETA.md (ATUALIZADO)
├─ ✏️ TECHNICAL_SUMMARY.md (este arquivo)
├─ QUICKSTART.md
├─ SUBESTACOES_README.md
└─ ETL_DIAGNOSTICO.md
```

---

## 🔄 Fluxo de Dados

### Fluxo 1: Análise de Carga e MMGD
```
carga_liquida_ons + gd_detalhada + Open-Meteo
  ↓
load_calc.py
  ├─ calculate_mmgd_generation() [irradiância solar]
  ├─ calculate_real_consumption() [carga + MMGD]
  └─ get_latest_load_data() [séries temporais]
  ↓
API /analise/carga-oculta
API /analise/estado-atual
  ↓
Frontend charts.py
  ├─ render_carga_section() [gráfico principal]
  └─ KPIs executivos (kpis.py)
```

### Fluxo 2: Estimativa Tempo Real
```
Open-Meteo API (irradiância atual)
  ↓
realtime_estimation.py
  ├─ get_current_irradiance() [lat/lon → W/m²]
  ├─ estimate_current_state() [hora atual]
  └─ _calculate_solar_generation() [potência instalada]
  ↓
API /analise/estado-atual
  ↓
Frontend realtime.py
  ├─ render_realtime_dashboard()
  ├─ Auto-refresh automático
  └─ KPIs executivos atualizados
```

### Fluxo 3: Detecção de Anomalias
```
gd_detalhada + estabelecimentos + carga_liquida_ons
  ↓
anomaly_detection.py
  ├─ detect_anomalies() [multi-critério]
  ├─ _analyze_distribuidora() [desvio >30%]
  ├─ _analyze_load_factor() [<0.20 ou >0.95]
  └─ _detect_abnormal_peaks() [>2.5x média]
  ↓
anomalias_automaticas (tabela)
  ↓
API /analise/alertas-historico
API /analise/detectar-anomalias
  ↓
Frontend audit.py
  ├─ render_historico_alertas() [filtros + gráficos]
  └─ render_auditoria() [análise detalhada]
```

### Fluxo 4: Clustering de Subestações
```
usinas_siga + gd_detalhada
  ↓
subestacoes_clustering.py
  ├─ _fetch_gd_locations() [lat/lon GD]
  ├─ _run_dbscan_clustering() [eps_km, min_samples]
  ├─ _generate_subestacao_records() [centróides]
  └─ load_detected_subestacoes() [persistência]
  ↓
subestacoes_detectadas (tabela)
  ↓
API /subestacoes/detectadas
API /subestacoes/geo [GeoJSON]
  ↓
Frontend subestacoes.py
  ├─ Tab: Detectadas [tabela + gráficos]
  └─ Tab: Mapa [visualização geoespacial]
```

### Fluxo 5: Dashboard Multi-Tab
```
Frontend app.py (entry point)
  ↓
├─ render_executive_kpis() [sempre visível]
├─ render_sidebar() [filtros]
└─ 5 Tabs:
   ├─ 📊 Visão Geral
   │  ├─ render_carga_section()
   │  ├─ render_classes_consumo_compact()
   │  └─ render_estabelecimentos_compact()
   │
   ├─ ⚡ Tempo Real
   │  └─ render_realtime_dashboard()
   │
   ├─ 🏭 Subestações
   │  ├─ render_subestacoes_section()
   │  └─ render_analise_local_subestacao()
   │
   ├─ 📈 Perfis & Análise
   │  └─ render_perfis_carga()
   │
   └─ 🔍 Auditoria
      ├─ render_auditoria()
      └─ render_historico_alertas()
```

---

## 🔑 Componentes Principais

### 1. ApiResult Pattern
**Tecnologia:** Python Dataclass

**Estrutura:**
```python
@dataclass
class ApiResult:
    data: Optional[Any]           # dict/list/None
    error: Optional[str]          # mensagem de erro
    status_code: Optional[int]    # código HTTP
```

**Uso Correto:**
```python
result = client.get(path, params)
if result.error:              # 1. Verificar erro
    show_error(result.error)
    return
if not result.data:           # 2. Verificar dados nulos
    return
# 3. Usar result.data (dict/list)
data = result.data
```

### 2. Estimativa de Geração MMGD
**Algoritmo:** Baseado em irradiância solar

**Fórmula:**
```
geração_mw = potencia_instalada_mw × (irradiancia_atual / 1000) × eficiencia
```

**Componentes:**
- `realtime_estimation.py` - Estimativa tempo real
- `synthetic_load.py` - Curvas sintéticas
- `profile_calibration.py` - Calibração automática

**Fontes:**
- Irradiância: Open-Meteo API
- Potência instalada: gd_detalhada (ANEEL)
- Perfis: EPE/ANEEL

### 3. Detecção de Anomalias Multi-Critério
**Algoritmo:** AnomalyDetector

**3 Critérios de Detecção:**

1. **Desvio de Consumo**
   - Threshold: 30% de variação
   - Compara consumo atual vs médio
   - Severidade: baseada em magnitude

2. **Fator de Carga Atípico**
   - Threshold mínimo: 0.20
   - Threshold máximo: 0.95
   - Indica padrões irreais

3. **Picos Anormais**
   - Threshold: >2.5x média
   - Detecta anomalias pontuais
   - Compara pico vs média

**Persistência:**
- Tabela: `anomalias_automaticas`
- Campos: tipo, severidade, status, impacto_kw

### 4. DBSCAN Clustering Geoespacial
**Tecnologia:** scikit-learn DBSCAN

**Parâmetros:**
- `eps_km` (raio): 5 km (ajustável)
- `min_samples` (mínimo): 3 pontos

**Vantagens:**
- ✅ Detecta clusters de qualquer forma
- ✅ Identifica ruído automaticamente
- ✅ Escalável para grandes datasets

**Processo:**
1. Converte coords para Web Mercator
2. Executa DBSCAN
3. Calcula centróide e raio (Haversine)
4. Filtra ruído (label = -1)

### 5. PostGIS Integration
**Geometrias:** Point com EPSG:4326

**Índices Principais:**
- carga_liquida_ons (tempo, subsistema)
- gd_detalhada (distribuidora, classe)
- subestacoes_ons (distribuidora, subsistema)
- subestacoes_detectadas (distribuidora, cluster_id)
- anomalias_automaticas (data_deteccao, status)

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

---

## 🎯 Arquitetura Geral

```
┌─────────────────────────────────────────────────────┐
│               FONTES DE DADOS EXTERNAS              │
├─────────────────────────────────────────────────────┤
│  ONS     │  ANEEL   │  BDGD    │  Open-Meteo       │
│ (carga)  │ (usinas) │ (malha)  │ (irradiância)     │
└────┬────────┬─────────┬──────────┬──────────────────┘
     │        │         │          │
     ▼        ▼         ▼          ▼
┌─────────────────────────────────────────────────────┐
│              ETL PIPELINE (extractors)              │
├─────────────────────────────────────────────────────┤
│  subestacoes_client.py  │  bdgd_client.py          │
│  schedulers/            │  Transformações          │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│         DATABASE (TimescaleDB + PostGIS)            │
├─────────────────────────────────────────────────────┤
│  9 tabelas  │  Índices  │  Hypertables  │  Geo     │
└────────────────────┬────────────────────────────────┘
                     │
     ┌───────────────┼───────────────┐
     │               │               │
     ▼               ▼               ▼
┌──────────┐  ┌──────────┐  ┌──────────────┐
│ Services │  │   API    │  │  Algoritmos  │
├──────────┤  ├──────────┤  ├──────────────┤
│ load_calc│  │ analise  │  │ DBSCAN       │
│ anomaly  │  │ subest.  │  │ Multi-crit.  │
│ realtime │  │ 21+ ep   │  │ Synthetic    │
└────┬─────┘  └────┬─────┘  └──────┬───────┘
     │            │               │
     └────────────┼───────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│            FRONTEND (Streamlit)                     │
├─────────────────────────────────────────────────────┤
│  KPIs (topo)  │  5 Tabs  │  7 Componentes          │
│  Auto-refresh │  Gráficos │  Minimalist UI         │
└─────────────────────────────────────────────────────┘
```

---

**Versão:** 2.0
**Data:** 2026-02-03
**Status:** ✅ Sistema Completo em Produção
