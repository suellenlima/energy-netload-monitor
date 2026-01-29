# 📊 Notebooks - Detecção de Painéis Solares

## 🎯 Objetivo Geral

Detectar e mapear **painéis solares em telhados urbanos** para análise de potencial de energia renovável em subestações brasileiras.

---

## 📝 Notebooks Principais

### **03 - Detecção Heurística (OpenCV)**

Algoritmo de Detecção Heurística (OpenCV). Painéis solares têm uma assinatura visual muito específica:

- São retangulares
- Têm cor azul-escura/preta (diferente de telhados de barro ou vegetação)
- Têm bordas bem definidas

Notebook que baixa uma imagem de satélite, encontra essas áreas "azuis e retangulares" e calcula a área total. Se a área for maior que o registrado na ANEEL, temos um "Gato" (Carga Oculta Não Registrada).

---

## 📚 Notebooks - Explicação Rápida

### **Notebooks de Deep Learning (IA)**

#### **05 - Treino Sintético (`05_treino_sintetico_modelo_placa.ipynb`)**
- **O que faz**: Treina modelo CNN com painéis gerados por computador (sintéticos)
- **Resultado**: Modelo base que reconhece painéis em geral
- **Problema**: Funciona bem em imagens perfeitas, mas falha em fotos reais

#### **06 - Transfer Learning Real (`06_transfer_learning_real.ipynb`)** ⭐ NOVO
- **O que faz**: Adapta o modelo sintético para funcionar em **dados REAIS**
- **Como**: Fine-tuning + datasets reais (UC Merced, Lacuna Solar, PV Dataset)
- **Resultado**: Modelo pronto para produção em subestações brasileiras

---

## 🧠 Fine-tuning: O que é e por que usamos?

### **Problema**

Modelo treinado em imagens SINTÉTICAS (perfeitas) falha em imagens REAIS (sombras, ângulos, degradação)

### **Solução: Fine-tuning**

1. Pega modelo pré-treinado (sintético)
2. Congela primeiras camadas (conhecimento genérico)
3. Desconegela últimas 4 camadas (adaptação)
4. Treina com dados REAIS de baixa taxa de aprendizado
5. ✅ Modelo adaptado para realidade

### **Vantagens**

| Aspecto           | Sem Fine-tuning      | Com Fine-tuning          |
| ----------------- | -------------------- | ------------------------ |
| Dados necessários | 10.000+ imagens      | ~2.000 imagens           |
| Tempo treino      | 24+ horas            | 30 minutos               |
| Performance       | ~60% acurácia        | ~85%+ acurácia           |
| Generalização     | Fraca (só sintético) | Forte (sintético + real) |

---

## 🎯 Classificação Urbano vs Natural: Por que é crítica?

### **NÃO é apenas detectar painéis!**

- ❌ Objetivo errado: "Detectar painéis em qualquer lugar"
- ✅ Objetivo correto: "Identificar ÁREAS URBANAS com POTENCIAL para painéis"

### **Por que importa**

**Detector sem classificação urbano/natural:**
- Floresta com estrutura → Detecta "painel" (FALSO POSITIVO)
- Telhado urbano → Detecta "painel" (CORRETO)
- Resultado: Sem contexto geográfico, inútil para planejamento

**Detector com classificação urbano/natural:**
- Floresta com estrutura → "NATURAL" (descarta)
- Telhado urbano → "URBANO" (relevante)
- Resultado: Identifica onde investir em energia solar

### **Datasets utilizados**

| Dataset        | Urbano             | Natural        | Propósito                       |
| -------------- | ------------------ | -------------- | ------------------------------- |
| UC Merced      | Prédios, indústria | Floresta, água | Aprende diferença               |
| Lacuna Solar   | Telhados REAIS     | Nenhum         | Dados com rótulos verdadeiros   |
| EuroSAT        | Construções        | Vegetação      | Valida em satélite              |
| **PV Dataset** | **1.325 imagens**  | **Nenhuma**    | **Reforça: urbano = potencial** |

---

## 🚀 Pipeline Completo

1️⃣ **Modelo Base (Sintético)**
   - Pré-treinado em 06_treino_sintetico_modelo_placa.ipynb

2️⃣ **Dados Reais**
   - UC Merced (urbano/natural)
   - Lacuna Solar (rótulos REAIS)
   - EuroSAT (satélite classificado)
   - PV Dataset (1.325 imagens com placas)

3️⃣ **Fine-tuning**
   - Adapta modelo sintético para dados REAIS
   - 20 épocas com augmentation

4️⃣ **Detector Final**
   - Entrada: Imagem de satélite (224x224)
   - Saída: URBANO (potencial) ou NATURAL (ignora)
   - Pronto para subestações brasileiras

### **Resultado esperado**
- F1-Score: ~82%+
- ROC-AUC: ~85%+
- Performance: Pronto para produção ✅
