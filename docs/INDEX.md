# 📚 ÍNDICE - Sistema de Monitoramento de Carga Líquida

## 🎯 Comece Aqui

Para usuários que desejam **começar imediatamente**:
→ [QUICKSTART.md](QUICKSTART.md) (5 minutos)

Para desenvolvedores que precisam entender a **arquitetura**:
→ [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md)

Para quem quer o **guia completo de subestações**:
→ [SUBESTACOES_README.md](SUBESTACOES_README.md)

Para ver o **resumo executivo** do que foi implementado:
→ [IMPLEMENTACAO_COMPLETA.md](IMPLEMENTACAO_COMPLETA.md)

Para consultar **diagnóstico do ETL Pipeline**:
→ [ETL_DIAGNOSTICO.md](ETL_DIAGNOSTICO.md)

---

## 📁 Estrutura de Arquivos

### 📄 Documentação
```
QUICKSTART.md                  ← 🚀 COMECE AQUI (5 min)
├─ Instalação rápida
├─ Exemplos de uso
├─ Troubleshooting
└─ Checklist

SUBESTACOES_README.md          ← 📖 Guia Completo
├─ Visão geral
├─ Arquitetura (diagramas)
├─ API Reference (5 endpoints)
├─ Algoritmo DBSCAN
├─ Como usar (múltiplas formas)
└─ Notas de implementação

TECHNICAL_SUMMARY.md           ← 🔧 Referência Técnica
├─ Arquivos modificados/criados
├─ Fluxo de dados
├─ Componentes principais
├─ Parâmetros
└─ Troubleshooting

IMPLEMENTACAO_COMPLETA.md      ← ✅ Resumo Executivo
├─ O que foi entregue
├─ Arquivos entregues
├─ Funcionalidades
├─ Métricas
└─ Status final

CHANGELOG.md                   ← 📝 Registro de Mudanças
├─ Arquivos modificados
├─ Arquivos criados
├─ Estatísticas
├─ Integração
└─ Deploy checklist
```

### 💾 Código-Fonte

#### Backend API (21+ Endpoints)
```
backend/src/api/
├─ analise.py (430 linhas)
│  ├─ GET  /analise/carga-oculta
│  ├─ GET  /analise/estabelecimentos
│  ├─ GET  /analise/classes-consumo
│  ├─ GET  /analise/perfil-carga
│  ├─ GET  /analise/simulacao-fraude
│  ├─ GET  /analise/estado-atual
│  ├─ GET  /analise/alertas-historico
│  └─ POST /analise/detectar-anomalias
│
└─ subestacoes.py (250 linhas)
   ├─ GET  /subestacoes/ons
   ├─ GET  /subestacoes/detectadas
   ├─ POST /subestacoes/detectadas/atualizar
   ├─ GET  /subestacoes/geo
   └─ GET  /subestacoes/resumo

backend/src/services/
├─ load_calc.py (470 linhas)
│  ├─ calculate_mmgd_generation()
│  ├─ get_latest_load_data()
│  ├─ calculate_real_consumption()
│  └─ fetch_fraud_alert()
│
├─ anomaly_detection.py (800 linhas)
│  ├─ detect_anomalies()
│  ├─ generate_historical_alerts()
│  ├─ _analyze_distribuidora()
│  └─ _analyze_load_factor()
│
├─ subestacoes_clustering.py (290 linhas)
│  ├─ detect_subestacoes_by_clustering()
│  ├─ _run_dbscan_clustering()
│  └─ load_detected_subestacoes()
│
├─ synthetic_load.py (250 linhas)
│  ├─ generate_synthetic_load_curve()
│  └─ _apply_noise_and_variation()
│
└─ realtime_estimation.py (180 linhas)
   ├─ estimate_current_state()
   └─ get_current_irradiance()
```

#### ETL Pipeline
```
etl_pipeline/src/extractors/
├─ subestacoes_client.py (280 linhas)
│  ├─ extract_subestacoes_data()
│  ├─ transform_subestacoes_data()
│  └─ run_extraction()
│
├─ bdgd_client.py
│  └─ Extração de dados BDGD (ANEEL)
│
└─ schedulers/
   └─ Agendamento de jobs ETL
```

#### Frontend UI (5 Tabs Principais)
```
frontend/src/components/
├─ kpis.py (260 linhas)
│  ├─ render_executive_kpis()
│  └─ KPI cards no topo do dashboard
│
├─ charts.py (680 linhas)
│  ├─ render_carga_section()
│  ├─ render_classes_consumo()
│  ├─ render_estabelecimentos_section()
│  └─ render_perfis_carga()
│
├─ realtime.py (350 linhas)
│  ├─ render_realtime_dashboard()
│  └─ Auto-refresh opcional
│
├─ subestacoes.py (700 linhas)
│  ├─ render_subestacoes_section()
│  ├─ render_analise_local_subestacao()
│  └─ 3 sub-tabs (ONS, Detectadas, Mapa)
│
├─ audit.py (270 linhas)
│  ├─ render_auditoria()
│  └─ render_historico_alertas()
│
└─ sidebar.py (180 linhas)
   ├─ render_sidebar()
   └─ Controles de filtros
```

#### Database (9 Tabelas + Índices)
```
infrastructure/database/
└─ schema.sql
   ├─ carga_liquida_ons (medições ONS)
   ├─ gd_detalhada (geração distribuída)
   ├─ usinas_siga (usinas ANEEL)
   ├─ estabelecimentos (classes de consumo)
   ├─ subestacoes_ons (subestações oficiais)
   ├─ subestacoes_detectadas (clustering DBSCAN)
   ├─ alertas_fraude (detecção manual/IA)
   ├─ load_profiles (perfis típicos EPE)
   └─ anomalias_automaticas (detecção automática)

└─ migrations/
   └─ Histórico de alterações do schema
```

#### Scripts
```
scripts/
└─ demo_subestacoes.py (180 linhas)
   └─ Teste completo do sistema
```

---

## 🎯 Por Caso de Uso

### "Quero Começar Agora"
1. Abrir: [QUICKSTART.md](QUICKSTART.md)
2. Executar: 5 comandos
3. Resultado: Sistema rodando

### "Preciso Entender a Arquitetura"
1. Ler: [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md)
2. Ver: Diagramas de fluxo
3. Entender: Componentes principais

### "Vou Usar em Produção"
1. Estudar: [SUBESTACOES_README.md](SUBESTACOES_README.md)
2. Configurar: Parâmetros DBSCAN
3. Validar: Dados reais

### "Preciso Debugar"
1. Consultar: Seção troubleshooting em cada doc
2. Executar: [scripts/demo_subestacoes.py](scripts/demo_subestacoes.py)
3. Verificar: Logs do sistema

### "Vou Estender o Sistema"
1. Estudar: [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md)
2. Analisar: Código-fonte (comentado)
3. Modificar: Parâmetros DBSCAN ou adicionar novos endpoints

---

## 📊 Funcionalidades

### ✅ Completamente Implementado

| Feature | Componentes | Status |
|---------|------------|--------|
| **Análise de Carga** | load_calc.py + charts.py | ✅ Carga líquida vs real |
| **Estimativa MMGD** | synthetic_load.py + realtime_estimation.py | ✅ Tempo real com irradiância |
| **Detecção de Anomalias** | anomaly_detection.py + audit.py | ✅ Automática + histórico |
| **Clustering Subestações** | subestacoes_clustering.py | ✅ DBSCAN geoespacial |
| **KPIs Executivos** | kpis.py | ✅ 4 cards sempre visíveis |
| **Dashboard Multi-Tab** | app.py | ✅ 5 tabs principais |
| **Tempo Real** | realtime.py | ✅ Auto-refresh opcional |
| **API REST** | analise.py + subestacoes.py | ✅ 21+ endpoints |
| **Database** | schema.sql + migrations/ | ✅ 9 tabelas + PostGIS |
| **ETL Pipeline** | extractors/ + schedulers/ | ✅ 4 fontes de dados |
| **Documentação** | docs/ | ✅ 6 arquivos principais |

---

## 🔗 Relações Entre Documentos

```
┌─ QUICKSTART.md ─────────────┐
│  ↓ "Quer detalhes?"         │
├─ SUBESTACOES_README.md ─────┤
│  ↓ "Quer arquitetura?"      │
├─ TECHNICAL_SUMMARY.md ──────┤
│  ↓ "Quer resumo?"           │
├─ IMPLEMENTACAO_COMPLETA.md ─┤
│  ↓ "Quer mudanças?"         │
└─ CHANGELOG.md ──────────────┘
```

---

## 🧭 Navegação Rápida

### Para Iniciantes
1. Começar: [QUICKSTART.md - Iniciar Rápido](QUICKSTART.md#-iniciar-rápido-5-minutos)
2. Testar: [QUICKSTART.md - Testar Detecção](QUICKSTART.md#-testar-detecção-automática)
3. Explorar: [QUICKSTART.md - Exemplos](QUICKSTART.md#-exemplos-de-uso)

### Para Arquitetos
1. Visão geral: [TECHNICAL_SUMMARY.md - Arquitetura](TECHNICAL_SUMMARY.md#-arquitetura)
2. Fluxo de dados: [TECHNICAL_SUMMARY.md - Fluxo de Dados](TECHNICAL_SUMMARY.md#-fluxo-de-dados)
3. Componentes: [TECHNICAL_SUMMARY.md - Componentes Principais](TECHNICAL_SUMMARY.md#-componentes-principais)

### Para Engenheiros
1. Algoritmo: [SUBESTACOES_README.md - Algoritmo DBSCAN](SUBESTACOES_README.md#-algoritmo-de-clustering)
2. API: [SUBESTACOES_README.md - Endpoints](SUBESTACOES_README.md#--api-endpoints)
3. Código: [TECHNICAL_SUMMARY.md - Arquivos Criados](TECHNICAL_SUMMARY.md#-arquivos-criados)

### Para DevOps
1. Instalação: [QUICKSTART.md - Iniciar Serviços](QUICKSTART.md#4-iniciar-serviços)
2. Deploy: [CHANGELOG.md - Deploy Checklist](CHANGELOG.md#-deploy-checklist)
3. Requisitos: [SUBESTACOES_README.md - Dependências](SUBESTACOES_README.md#-instalação-de-dependências)

---

## 📈 Conteúdo por Tipo

### 📚 Conceitual
- [IMPLEMENTACAO_COMPLETA.md](IMPLEMENTACAO_COMPLETA.md) - O que foi construído
- [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md) - Como funciona
- [SUBESTACOES_README.md](SUBESTACOES_README.md) - Por que assim

### 🔧 Prático
- [QUICKSTART.md](QUICKSTART.md) - Como começar
- [SUBESTACOES_README.md - Exemplos](SUBESTACOES_README.md#-exemplos) - Código funcionando
- [scripts/demo_subestacoes.py](scripts/demo_subestacoes.py) - Teste live

### 📋 Administrativo
- [CHANGELOG.md](CHANGELOG.md) - Mudanças realizadas
- [IMPLEMENTACAO_COMPLETA.md - Métricas](IMPLEMENTACAO_COMPLETA.md#-métricas) - Estatísticas

---

## 🎓 Tópicos de Aprendizado

### Clustering Geoespacial
- Ler: [TECHNICAL_SUMMARY.md - DBSCAN](TECHNICAL_SUMMARY.md#-dbscan-clustering)
- Estudar: [subestacoes_clustering.py](backend/src/services/subestacoes_clustering.py)
- Praticar: [SUBESTACOES_README.md - Ajuste de Parâmetros](SUBESTACOES_README.md#ajuste-de-parâmetros-dbscan)

### PostGIS/Geoespacial
- Entender: [TECHNICAL_SUMMARY.md - PostGIS](TECHNICAL_SUMMARY.md#postgis-integration)
- Ver: [schema.sql](infrastructure/database/schema.sql)
- Usar: [subestacoes_client.py - GeoDataFrame](etl_pipeline/src/extractors/subestacoes_client.py)

### FastAPI + Streamlit
- Endpoints: [subestacoes.py (backend)](backend/src/api/subestacoes.py)
- UI: [subestacoes.py (frontend)](frontend/src/components/subestacoes.py)

### ETL Pipeline
- Desing: [subestacoes_client.py](etl_pipeline/src/extractors/subestacoes_client.py)
- Integração: [TECHNICAL_SUMMARY.md - Como Usar](TECHNICAL_SUMMARY.md#2-integrar-ao-etl-pipeline)

---

## 💡 FAQ Rápido

**P: Onde começo?**
R: [QUICKSTART.md](QUICKSTART.md)

**P: Como funciona o clustering?**
R: [TECHNICAL_SUMMARY.md - DBSCAN](TECHNICAL_SUMMARY.md#-dbscan-clustering)

**P: Que APIs estão disponíveis?**
R: [SUBESTACOES_README.md - Endpoints](SUBESTACOES_README.md#--api-endpoints)

**P: Como usar em produção?**
R: [SUBESTACOES_README.md - Produção](SUBESTACOES_README.md#-notas-de-implementação)

**P: O que foi modificado?**
R: [CHANGELOG.md](CHANGELOG.md)

**P: O que foi criado?**
R: [IMPLEMENTACAO_COMPLETA.md - Arquivos](IMPLEMENTACAO_COMPLETA.md#-arquivos-entregues)

---

## ✅ Checklist de Leitura

- [ ] Ler QUICKSTART.md
- [ ] Executar scripts/demo_subestacoes.py
- [ ] Estudar TECHNICAL_SUMMARY.md
- [ ] Explorar código-fonte
- [ ] Ler SUBESTACOES_README.md completo
- [ ] Revisar CHANGELOG.md
- [ ] Consultar IMPLEMENTACAO_COMPLETA.md

---

## 🎯 Próximos Passos

1. **Agora:** Abrir [QUICKSTART.md](QUICKSTART.md)
2. **Depois:** Estudar [TECHNICAL_SUMMARY.md](TECHNICAL_SUMMARY.md)
3. **Depois:** Revisar [SUBESTACOES_README.md](SUBESTACOES_README.md)
4. **Finalmente:** Implementar suas extensões!

---

## 📞 Suporte

Cada documento tem uma seção de troubleshooting. Procure por:
- `🐛 Troubleshooting` em qualquer doc
- `⚠️ Considerações` para avisos importantes
- `💡 Dicas` para sugestões práticas

---

---

## 🌟 Visão Geral do Sistema

### 1️⃣ Análise de Carga e Geração
- **Carga Líquida ONS**: Medições oficiais dos pontos de entrega
- **Geração MMGD**: Estimativa de solar distribuída (tempo real)
- **Consumo Real**: Carga líquida + MMGD (consumo total efetivo)
- **Perfis de Carga**: Curvas típicas por classe (residencial, comercial, industrial)

### 2️⃣ Detecção Automática de Anomalias
- **Desvio de Consumo**: Identifica variações >30% do esperado
- **Fator de Carga Atípico**: Detecta padrões anormais (<0.20 ou >0.95)
- **Picos Anormais**: Identifica picos >2.5x a média
- **Histórico Completo**: Tabela com filtros e gráficos de distribuição

### 3️⃣ Monitoramento em Tempo Real
- **Dashboard Operacional**: Estimativas hora a hora
- **Irradiância Solar**: Integração Open-Meteo
- **Auto-Refresh**: Atualização automática configurável
- **KPIs Executivos**: 4 métricas principais sempre visíveis

### 4️⃣ Análise de Subestações
- **Dados ONS**: Subestações oficiais com mock/real
- **Clustering DBSCAN**: Detecção automática geoespacial
- **Visualização Mapa**: GeoJSON para integração
- **Análise Local**: Estimativas por subestação

### 5️⃣ Auditoria e Integridade
- **Detecção Manual**: Alertas via inspeção visual
- **Detecção Automática**: Anomalias identificadas por IA
- **Severidade**: Classificação alto/médio/baixo
- **Status**: Acompanhamento ativo/resolvido

---

**Última atualização:** 2026-02-03
**Versão:** 2.0
**Status:** ✅ Sistema Completo em Produção

🚀 **Vamos começar!**
