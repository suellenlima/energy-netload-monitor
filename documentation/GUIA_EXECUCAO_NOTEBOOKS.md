# 🚀 Guia de Execução - Pipeline Completo

## ⚠️ Problema Atual

Você está tentando executar notebook **08** (Benchmark/Comparação) mas os dados não foram gerados ainda porque os notebooks **06** e **07** não foram executados.

## ✅ Solução: Execute na Ordem Correta

### 1️⃣ **Notebook 06: Transfer Learning Real**
`notebooks/06_transfer_learning_real.ipynb`

**O que faz:**
- Carrega datasets: UC Merced, Lacuna Solar, Solar Panel
- Treina modelo com transfer learning em dados REAIS
- **SALVA:** `./notebooks/data_cache/train_test_split.npz` ← **Dados necessários para 07 e 08**

**Como executar:**
1. Abra o notebook em Jupyter/JupyterLab
2. Clique em: **Run → Run All Cells**
3. Aguarde conclusão (pode levar alguns minutos)
4. Verifique que terminou com:
   ```
   ✅ Dados salvos com sucesso!
   🚀 Pronto para executar notebook 07
   ```

---

### 2️⃣ **Notebook 07: Advanced Detection Techniques**
`notebooks/07_advanced_detection_techniques.ipynb`

**O que faz:**
- Carrega dados gerados pelo notebook 06
- Aplica 7 técnicas avançadas:
  - ⚖️ Class Weighting
  - 🎨 Data Augmentation Agressiva
  - 🔧 Learning Rate Scheduler
  - 🛑 Early Stopping + Reduce LR
  - 📊 Regularização L2
  - 🔍 Threshold Optimization
  - 📈 Métricas Expandidas
- **SALVA:** Modelos treinados em `./modelos/`

**Como executar:**
1. Abra o notebook
2. Clique em: **Run → Run All Cells**
3. Aguarde conclusão (pode levar 15-30 minutos)
4. Verifique conclusão com:
   ```
   ✅ TREINO CONCLUÍDO!
   🎯 RESULTADO FINAL
   ```

---

### 3️⃣ **Notebook 08: Comparison Benchmark** ← VOCÊ ESTÁ AQUI
`notebooks/08_comparison_benchmark.ipynb`

**O que faz:**
- Carrega dados de teste
- Carrega todos os 3 modelos
- Compara performance
- Gera visualizações e relatório

**Como executar:**
1. Abra o notebook
2. Clique em: **Run → Run All Cells**
3. Veja o benchmark com todas as métricas

---

## 📊 Arquivos Gerados em Cada Etapa

```
Notebook 06 cria:
└─ notebooks/data_cache/
   └─ train_test_split.npz  ← Dados compartilhados

Notebook 07 cria:
└─ modelos/
   ├─ modelo_detector_paineis_reais.keras
   ├─ modelo_paineis_otimizado.keras
   └─ resumo_otimizacoes.txt

Notebook 08 cria:
└─ modelos/
   ├─ RELATORIO_BENCHMARK_FINAL.txt
   ├─ resultados_benchmark.csv
   └─ RESUMO_EXECUTIVO.txt
```

---

## 🎯 Tempo Estimado

| Notebook | Tempo | Status |
|----------|-------|--------|
| 06 - Transfer Learning | 5-10 min | ⏳ Executando |
| 07 - Advanced Techniques | 15-30 min | ⏳ Próximo |
| 08 - Benchmark | 2-5 min | ✅ Rápido |
| **TOTAL** | **25-45 min** | |

---

## ✨ Resultado Final Esperado

Ao final você terá:
- ✅ Dados de treino/teste organizados
- ✅ 3 modelos treinados (Sintético, Fine-tuned, Otimizado)
- ✅ Comparação detalhada entre modelos
- ✅ Recomendação do melhor modelo para produção
- ✅ Métricas: Accuracy, Precision, Recall, F1-Score, ROC-AUC
- ✅ Visualizações e gráficos comparativos
- ✅ Relatório em texto e CSV

---

## ⚡ Quick Start (Copie e Cole)

Se você está em Jupyter em `/home/jovyan/work/`, o comando seria:

```bash
# Primeiro, execute notebook 06
jupyter nbconvert --to notebook --execute notebooks/06_transfer_learning_real.ipynb --inplace

# Depois, execute notebook 07
jupyter nbconvert --to notebook --execute notebooks/07_advanced_detection_techniques.ipynb --inplace

# Finalmente, execute notebook 08
jupyter nbconvert --to notebook --execute notebooks/08_comparison_benchmark.ipynb --inplace
```

---

## ❓ Troubleshooting

**Q: Erro "Dados não encontrados"**
- A: Verifique que notebook 06 foi executado completamente
- Verifique arquivo existe em: `./notebooks/data_cache/train_test_split.npz`

**Q: Memória insuficiente**
- A: Reduza batch size nos notebooks (padrão: 32)

**Q: Erro "Modelo não encontrado"**
- A: Verifique que notebook 07 foi executado completamente

---

**Próximo passo:** Execute notebook 06 agora! ▶️
