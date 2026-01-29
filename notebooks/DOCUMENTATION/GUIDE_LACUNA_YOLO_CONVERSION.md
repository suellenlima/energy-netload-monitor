# 🎯 GUIA: Converter Lacuna → YOLO Dataset

## 📊 O Que Acontece

A nova **Célula 5** faz tudo automaticamente:

```
lacuna-solar-survey-zindi-2/
├── images/ (imagens originais)
└── train.csv (com polígonos dos painéis)
        ↓ [Conversão]
        ↓
solar_panels_detection_dataset/
├── images/
│   ├── train/ (70%)
│   ├── val/ (15%)
│   └── test/ (15%)
└── labels/
    ├── train/
    ├── val/
    └── test/
```

---

## 🚀 Como Usar

### Passo 1: Execute Célula 5
Simplesmente execute a célula 5. Ela:
1. ✅ Lê `train.csv`
2. ✅ Converte polígonos → bounding boxes YOLO
3. ✅ Copia imagens
4. ✅ Gera labels normalizados
5. ✅ Separa em train/val/test (70/15/15)

**Resultado esperado**:
```
🚀 Convertendo dataset Lacuna Solar Survey para formato YOLO...

📖 Lendo CSV: ./data/lacuna-solar-survey-zindi-2/train.csv
✓ Total de linhas: 5000+
✓ Total de imagens únicas com painéis: 1000+

✅ Imagens encontradas: 1000+
✅ Imagens copiadas: 1000+
✅ Labels criados: 5000+ (múltiplos painéis por imagem)

📊 Reorganizando em train/val/test...

✅ Dataset Lacuna convertido para YOLO:
   📁 Treino: 700+ imagens
   📁 Validação: 150+ imagens
   📁 Teste: 150+ imagens
   💾 Salvo em: ./data/solar_panels_detection_dataset
```

### Passo 2: Use o Dataset
Agora você pode:

```python
# Opção A: Usar na célula 5 (preparação)
dataset_dir = prepare_yolo_dataset(
    images_dir='./data/solar_panels_detection_dataset/images',
    labels_dir='./data/solar_panels_detection_dataset/labels',
    output_dir='./data/yolo_dataset_final'
)

# Opção B: Usar diretamente com YOLO
config_path = create_yolo_config('./data/solar_panels_detection_dataset')
model, results = train_yolo_model(config_path, epochs=100)
```

---

## 📝 O Que a Conversão Faz

### 1. Polígono → Bounding Box
**Entrada (do CSV)**:
```
polygon: "[(2087, 2179.0), (2181, 2191.0), (2171, 2223.0), (2257, 2227.0)]"
```

**Conversão**:
```
x_min=2087, y_min=2179, x_max=2257, y_max=2227
center_x = (2087+2257)/2 = 2172
center_y = (2179+2227)/2 = 2203
width = 2257-2087 = 170
height = 2227-2179 = 48
```

### 2. Normalização YOLO
Converte para escala 0-1:
```
image_size = (2560, 1920)
center_x_norm = 2172 / 2560 = 0.8484
center_y_norm = 2203 / 1920 = 1.1474 → clipped to 1.0
width_norm = 170 / 2560 = 0.0664
height_norm = 48 / 1920 = 0.0250
```

### 3. Label YOLO
**Output (arquivo `.txt`)**:
```
0 0.848438 1.000000 0.066406 0.025000
```
Onde:
- `0` = classe (solar_panel)
- `0.848438` = center_x normalizado
- `1.000000` = center_y normalizado
- `0.066406` = width normalizado
- `0.025000` = height normalizado

---

## ⚙️ Configurações

### Mudar Tamanho de Imagem
Se suas imagens têm tamanho diferente:

```python
result = prepare_lacuna_to_yolo(
    csv_path='./data/lacuna-solar-survey-zindi-2/train.csv',
    images_src_dir='./data/lacuna-solar-survey-zindi-2/images',
    output_yolo_dir='./data/solar_panels_detection_dataset',
    image_size=(4000, 3000)  # Seu tamanho real
)
```

### Mudar Proporção de Split
```python
result = prepare_lacuna_to_yolo(
    csv_path='...',
    images_src_dir='...',
    train_ratio=0.8,  # 80% treino
    val_ratio=0.1     # 10% validação, 10% teste
)
```

---

## 🔍 Verificar Resultado

### Verificar Estrutura
```python
import os
for split in ['train', 'val', 'test']:
    img_count = len(os.listdir(f'./data/solar_panels_detection_dataset/images/{split}'))
    label_count = len(os.listdir(f'./data/solar_panels_detection_dataset/labels/{split}'))
    print(f"{split}: {img_count} imagens, {label_count} labels")
```

### Visualizar Label
```python
# Ler um label
with open('./data/solar_panels_detection_dataset/labels/train/ID00rw8.txt') as f:
    labels = f.readlines()
    print(f"Painéis nessa imagem: {len(labels)}")
    for i, label in enumerate(labels):
        print(f"  Painel {i+1}: {label.strip()}")
```

### Visualizar com YOLO
```python
from ultralytics import YOLO
model = YOLO('yolov8n.pt')  # Modelo pré-treinado

# Ver um exemplo do dataset
results = model.train(
    data='./data/solar_panels_detection_dataset/data.yaml',
    epochs=1,  # Só 1 época para verificar
    imgsz=640
)
```

---

## 🐛 Troubleshooting

### Erro: "Imagens não encontradas"
**Causa**: Extensão das imagens é diferente (JPG vs jpg)

**Solução**: A função já tenta múltiplas extensões. Se ainda falhar:
```python
import os
os.listdir('./data/lacuna-solar-survey-zindi-2/images')[:5]  # Ver extensão real
```

### Erro: "train set will be empty"
**Causa**: Nenhuma imagem foi encontrada/copiada

**Solução**:
1. Verifique se `./data/lacuna-solar-survey-zindi-2/images/` existe
2. Verifique se tem imagens lá
3. Rode a célula novamente

### Labels vazios
**Causa**: Polígonos malformados no CSV

**Solução**: Verifica automaticamente - imagens sem labels são ignoradas

---

## 📊 Exemplo Completo

```python
# 1. Converter Lacuna → YOLO (Célula 5)
result = prepare_lacuna_to_yolo(...)

# 2. Criar data.yaml (Célula 5 / 6)
config_path = create_yolo_config('./data/solar_panels_detection_dataset')

# 3. Treinar modelo (Célula 5)
model, results = train_yolo_model(config_path, model_size='m', epochs=100)

# 4. Usar modelo (Célula 11)
results = full_pipeline(
    'image.jpg', model, classifier, estimator,
    save_results=True, output_dir='./outputs'
)
```

---

## ✅ Verificação Rápida

Após executar Célula 5, verifique:

- [ ] Pasta `./data/solar_panels_detection_dataset/` criada
- [ ] Dentro tem pastas: `images/` e `labels/`
- [ ] Dentro delas: `train/`, `val/`, `test/`
- [ ] Arquivos `.jpg` em `images/train/`, etc
- [ ] Arquivos `.txt` em `labels/train/`, etc
- [ ] Número de imagens ≈ número de labels (por split)

---

## 🎯 Próximo Passo

Após a conversão, você pode:

1. **Treinar modelo** → Célula 6
2. **Usar full_pipeline** → Célula 13
3. **Gerar relatórios** → Célula 11

**Tudo funciona automaticamente com os dados convertidos!**

---

**Status**: ✅ Pronto para usar  
**Data**: Janeiro 2025
