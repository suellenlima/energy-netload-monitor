# 📥 Setup - Lacuna Solar Survey Dataset

## O que é?

**Lacuna Solar Survey** é um dataset do Kaggle com imagens REAIS de telhados com/sem painéis solares rotulados.

- 📸 Imagens: Telhados reais
- 🏷️ Rótulos: BINÁRIOS (0=sem painel, 1=com painel)
- 🌍 Cobertura: Múltiplos países (incluindo Brasil)
- 📊 Tamanho: ~10k+ imagens

## Links

- **Kaggle Dataset**: https://www.kaggle.com/datasets/jimmybarium/lacuna-solar-survey-zindi
- **Zindi Competition**: https://zindi.africa/competitions/lacuna-solar-survey

## Como Baixar

### Opção 1: Via Kaggle CLI (Recomendado)

```bash
# 1. Instalar Kaggle CLI
pip install kaggle

# 2. Configurar credenciais
# Vá para: https://www.kaggle.com/settings/account
# Clique em "Create New API Token"
# Isso baixa kaggle.json
# Coloque em: ~/.kaggle/kaggle.json (Linux/Mac) ou %USERPROFILE%\.kaggle\kaggle.json (Windows)

# 3. Definir permissões (Linux/Mac)
chmod 600 ~/.kaggle/kaggle.json

# 4. Baixar dataset
kaggle datasets download -d jimmybarium/lacuna-solar-survey-zindi

# 5. Descompactar
unzip lacuna-solar-survey-zindi.zip -d ./data/

# 6. Resultado
# ./data/lacuna-solar-survey-zindi/
#   ├── train_agro/         ← imagens
#   ├── train_agro.csv      ← rótulos
#   └── ...
```

### Opção 2: Manual (Kaggle Website)

1. Acesse: https://www.kaggle.com/datasets/jimmybarium/lacuna-solar-survey-zindi
2. Clique em "Download"
3. Extraia em `./data/lacuna-solar-survey-zindi/`

## Estrutura Esperada

```
data/
└── lacuna-solar-survey-zindi/
    ├── train_agro/           ← 📂 Pasta com imagens
    │   ├── image_001.jpg
    │   ├── image_002.jpg
    │   └── ...
    ├── train_agro.csv        ← 📋 Arquivo com rótulos
    │   # Formato: id, label (0=sem painel, 1=com painel)
    │   image_001, 1
    │   image_002, 0
    │   ...
    └── README.md
```

## Validar Download

Após descompactar:

```python
from pathlib import Path
import pandas as pd

# Verificar pasta
dataset_path = Path('./data/lacuna-solar-survey-zindi')
images_dir = dataset_path / 'train_agro'
csv_file = dataset_path / 'train_agro.csv'

print(f"✓ Dataset encontrado: {dataset_path.exists()}")
print(f"✓ Pasta de imagens: {images_dir.exists()}")
print(f"✓ CSV de rótulos: {csv_file.exists()}")

if csv_file.exists():
    df = pd.read_csv(csv_file)
    print(f"\n📊 Dataset:")
    print(f"   • Total de imagens: {len(df)}")
    print(f"   • Com painéis (1): {(df[df.columns[1]] == 1).sum()}")
    print(f"   • Sem painéis (0): {(df[df.columns[1]] == 0).sum()}")
```

## Erros Comuns

### ❌ "Arquivo não encontrado"

```
⚠️ Lacuna Solar Survey não encontrado em ./data/lacuna-solar-survey-zindi
```

**Solução**:
1. Verificar se descompactou no local certo
2. Usar caminho absoluto: `/full/path/to/lacuna-solar-survey-zindi`
3. Verificar permissões de pasta

### ❌ "Estrutura não encontrada"

```
⚠️ Estrutura não encontrada
Procurei em: [PosixPath('./data/lacuna-solar-survey-zindi/train_agro'), ...]
```

**Solução**:
1. Verificar nome da pasta (pode ser `train`, `train_agro`, ou `images`)
2. Verificar extensões de arquivo (`.jpg`, `.png`, `.tif`, etc)
3. Organizar manualmente se necessário

### ❌ Kaggle CLI não funciona

```bash
# Verificar instalação
kaggle --version

# Se erro "command not found":
pip install --upgrade kaggle

# Se erro de credenciais:
# 1. Verificar ~/.kaggle/kaggle.json existe
# 2. Verificar conteúdo: cat ~/.kaggle/kaggle.json
# 3. Verificar permissões: chmod 600 ~/.kaggle/kaggle.json
```

## Tamanho & Tempo de Download

- **Tamanho**: ~500 MB a 2 GB (dependendo versão)
- **Tempo**: 5-30 minutos (depende velocidade internet)
- **Armazenamento**: Reserve 3-4 GB para descompactação

## Alternativas (se Lacuna não funcionar)

Outros datasets de painéis solares:

1. **UC Merced** ✅ (já incluído) - Proxy (urbano/natural)
2. **EuroSAT** - Satélite de alta resolução
3. **DSMS** - Detectron Solar Panel Segmentation
4. **Google Earth Engine** - Imagens customizadas

## Checklist

- [ ] Kaggle CLI instalado (`pip install kaggle`)
- [ ] `kaggle.json` configurado
- [ ] Dataset baixado
- [ ] Pasta descompactada em `./data/lacuna-solar-survey-zindi/`
- [ ] `train_agro/` e `train_agro.csv` existem
- [ ] Executor o script de validação acima ✅

## Próximas Etapas

Após setup:

1. Executar Célula 2 do notebook: `07_transfer_learning_real.ipynb`
2. Combo automático: UC Merced + Lacuna Solar
3. Fine-tuning com ambos os datasets
4. Detector treinado com dados REAIS! 🚀

---

**Dúvidas?** Verifique os logs da célula 2 do notebook para diagnóstico.
