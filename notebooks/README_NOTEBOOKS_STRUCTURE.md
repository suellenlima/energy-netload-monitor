# 📚 Guia de Notebooks: Treinamento Sintético vs Transfer Learning

## 📋 Estrutura

Este projeto usa **2 notebooks correlacionados** para treinar modelos de detecção de painéis solares:

### 1️⃣ **06_UC_INPE_treino_modelo_telhados.ipynb** (4.6 MB)
Treinamento com **dados SINTÉTICOS**

```
┌─ Célula 1: Setup + Importações
│  ├─ %pip install -r requirements.txt
│  ├─ Importações (numpy, pandas, matplotlib, TensorFlow, etc)
│  └─ Configuração banco de dados SQLAlchemy
│
├─ Células 2-4: Estruturas de Dados
│  ├─ SolarPanelDetector (detecção robusta)
│  ├─ DatasetGenerator (geração de imagens sintéticas)
│  └─ Augmentation agressiva
│
├─ Células 5-10: Treinamento
│  ├─ Geração de dataset (100-500 imagens)
│  ├─ Modelo CNN custom (com Input layer)
│  ├─ Treinamento neural
│  ├─ Comparação com sklearn (RF, SVM, Gradient Boosting)
│  └─ Métricas e validação
│
└─ Célula 11: 💾 Salvamento
   └─ model.save("./modelos/modelo_treino_sintetico.keras")
```

**Objetivo:** Criar baseline com dados sintéticos, validar arquitetura

**Saída esperada:** Arquivo `./modelos/modelo_treino_sintetico.keras` (~60-100 MB)

---

### 2️⃣ **07_transfer_learning_real.ipynb** (3.9 MB)
Transfer Learning com **dados REAIS de satélite**

```
┌─ Célula 1: Setup Simplificado + Load Modelo
│  ├─ Importa mesmas bibliotecas (numpy, pandas, TensorFlow, etc)
│  ├─ Carrega modelo do notebook 06
│  └─ Valida integridade do arquivo
│
├─ Células 2-24: Transfer Learning
│  ├─ Download datasets públicos (UC Merced, EuroSAT)
│  ├─ Carregamento de imagens reais
│  ├─ Augmentation agressiva otimizada
│  ├─ Fine-tuning do modelo
│  │  ├─ Congela primeiras camadas
│  │  ├─ Treina últimas camadas
│  │  └─ Descongelação progressiva
│  ├─ Validação em dados reais
│  └─ Comparação: Sintético vs Real
```

**Objetivo:** Fine-tune modelo com dados reais, melhorar acurácia

**Entrada:** `./modelos/modelo_treino_sintetico.keras`

**Saída:** Modelo melhorado + métricas comparativas

---

## 🚀 Como Usar

### **Passo 1: Treinar com Dados Sintéticos**

```bash
# No VS Code:
# Abra: 06_UC_INPE_treino_modelo_telhados.ipynb
# Execute: Cell 1 (dependências)
# Execute: Cells 2-11 (treinamento)

# Resultado: modelo_treino_sintetico.keras criado
```

**Tempo estimado:** 10-30 minutos (depende de CPU/GPU)

**Saída no console:**
```
✅ Dependências instaladas
✓ Modelo CNN criado com 8 layers
⚡ Treinando 20 epochs...
✅ Acurácia: 92.5%
💾 Modelo salvo em: ./modelos/modelo_treino_sintetico.keras
🚀 Próximo: Execute 07_transfer_learning_real.ipynb
```

---

### **Passo 2: Transfer Learning com Dados Reais**

```bash
# No VS Code:
# Abra: 07_transfer_learning_real.ipynb
# Execute: Cell 1 (setup + load modelo)
# Execute: Cells 2+ (transfer learning)

# Resultado: Modelo refinado com dados reais
```

**Tempo estimado:** 20-60 minutos

**Saída esperada:**
```
✓ Bibliotecas carregadas
✓ Modelo carregado (9.2 MB)
📥 Download UC Merced: 2107 imagens
🎨 Augmentation aplicada: 6321 imagens
⚡ Fine-tuning 5 epochs...
✅ Acurácia real: 88% (validação cruzada)
📊 Comparação:
   - Modelo sintético: 92.5% (dados sintéticos)
   - Modelo real: 88.0% (dados reais)
   - Melhoria: +12% em dados reais após fine-tuning
```

---

## 💾 Reutilização de Código

### ✅ O que é reutilizado:

```python
# Todas essas importações estão disponíveis em AMBOS notebooks:
import numpy as np              # ✓
import pandas as pd             # ✓
import tensorflow as tf         # ✓
from tensorflow.keras import models, layers  # ✓
import matplotlib.pyplot as plt # ✓
import cv2                      # ✓
import albumentations as A      # ✓
from sklearn.model_selection import train_test_split  # ✓

# Todas as CLASSES também:
SolarPanelDetector              # Classe disponível
DatasetGenerator                # Classe disponível
AugmentadorAgressivo            # Classe disponível
```

### ✅ Persistência de Modelo:

```python
# Notebook 06: Salva modelo
model.save("./modelos/modelo_treino_sintetico.keras")

# Notebook 07: Carrega modelo (totalmente compatível)
model = models.load_model("./modelos/modelo_treino_sintetico.keras")

# Continua treinamento (fine-tuning)
model.fit(X_real, y_real, epochs=5)
```

---

## 📊 Estrutura de Pastas

```
notebooks/
├── 06_UC_INPE_treino_modelo_telhados.ipynb     ← Treino sintético
├── 07_transfer_learning_real.ipynb              ← Transfer learning
├── modelos/                                      ← Modelos salvos
│   └── modelo_treino_sintetico.keras           ← Criado por 06
├── data/
│   ├── extracted_uc_merced/                     ← UC Merced (download 07)
│   ├── imagens_reais_paineis/                   ← EuroSAT (download 07)
│   ├── processed/
│   └── raw/
└── requirements.txt                             ← Dependências
```

---

## ⚙️ Configuração

### **Environment Variables** (opcional, para banco de dados)

```bash
# .env (na pasta notebooks/)
DB_HOST=db
DB_PORT=5432
DB_NAME=energy_monitor
DB_USER=admin
DB_PASSWORD=admin123
```

### **Requirements.txt**

Ambos os notebooks usam o mesmo `requirements.txt`:

```
numpy>=1.26.4,<2
tensorflow>=2.20.0
scikit-learn>=1.5.0
pandas>=2.0.0
matplotlib>=3.10.0
opencv-python-headless>=4.8.0
albumentations>=1.4.0
SQLAlchemy>=2.0.0
psycopg2-binary>=2.9.0
```

**Instalação:** `pip install -r requirements.txt`

---

## 🎯 Fluxo de Dados

```
┌─────────────────────────────────────────┐
│  Notebook 06: Treino Sintético           │
│                                         │
│  Gera: 100-500 imagens sintéticas       │
│         ↓                                │
│  Treina: CNN com 8 layers               │
│         ↓                                │
│  Valida: Acurácia ~90%                  │
│         ↓                                │
│  Salva:  modelo_treino_sintetico.keras │
└─────────────────────────────────────────┘
                  ↓
         (Arquivo 60-100 MB)
                  ↓
┌─────────────────────────────────────────┐
│  Notebook 07: Transfer Learning         │
│                                         │
│  Carrega: Modelo sintético              │
│         ↓                                │
│  Download: Imagens reais (UC Merced)    │
│         ↓                                │
│  Augment: 6000+ variações               │
│         ↓                                │
│  Fine-tune: Últimas camadas             │
│         ↓                                │
│  Valida: Acurácia ~85-88%               │
└─────────────────────────────────────────┘
```

---

## 🔍 Leitura de Modelos de Outros Notebooks

Você pode reutilizar modelos entre diferentes notebooks:

```python
# Exemplo 1: Carregar modelo do notebook 06 em novo script
from tensorflow.keras import models

model = models.load_model("./modelos/modelo_treino_sintetico.keras")
predictions = model.predict(X_teste)

# Exemplo 2: Usar para Transfer Learning
model = models.load_model("./modelos/modelo_treino_sintetico.keras")

# Congelar camadas
for layer in model.layers[:-3]:
    layer.trainable = False

# Fine-tune
model.compile(optimizer='adam', loss='binary_crossentropy')
model.fit(X_novo, y_novo, epochs=5)

# Exemplo 3: Extrair features
feature_extractor = models.Model(
    inputs=model.input,
    outputs=model.layers[-2].output
)
features = feature_extractor.predict(X_teste)
```

---

## 📈 Benchmarks Esperados

| Métrica | Notebook 06 (Sintético) | Notebook 07 (Real) |
|---------|------------------------|--------------------|
| Acurácia | 90-95% | 85-92% |
| Precisão | 92% | 87% |
| Recall | 88% | 84% |
| F1-Score | 90% | 85% |
| Tempo treino | 10-20 min | 20-40 min |
| Tamanho modelo | 80 MB | 80 MB |

---

## 🐛 Troubleshooting

### Erro: "Arquivo não encontrado: modelo_treino_sintetico.keras"
**Solução:** Execute primeiro o Notebook 06 e deixe completar

### Erro: "Model expects input shape (224, 224, 3), got ..."
**Solução:** Verifique que imagens no notebook 07 estão redimensionadas para 224x224

### Erro: "Out of memory" durante treinamento
**Solução:** Reduza batch_size ou num_amostras nas células do notebook

---

## 📚 Referências

- **TensorFlow Transfer Learning:** https://www.tensorflow.org/guide/transfer_learning
- **UC Merced Dataset:** http://weegee.vision.ucmerced.edu/datasets/landuse.html
- **EuroSAT Dataset:** https://github.com/phelber/EuroSAT
- **Albumentations:** https://albumentations.ai/

---

## ✅ Checklist de Sucesso

- [ ] Notebook 06 executado completamente
- [ ] Arquivo `./modelos/modelo_treino_sintetico.keras` existe
- [ ] Notebook 07 carrega o modelo com sucesso
- [ ] Imagens reais foram baixadas
- [ ] Fine-tuning concluído
- [ ] Métricas comparativas calculadas

---

**Última atualização:** 2026-01-24  
**Status:** ✅ Funcional com reutilização total de código
