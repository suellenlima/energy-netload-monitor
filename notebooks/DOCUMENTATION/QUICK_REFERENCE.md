# 🚀 Quick Reference Guide - YOLO Solar Panel Notebook

## 📌 Estrutura Rápida (31 Células)

### **SEÇÃO 0: Setup (Células 1-4)**
| # | Nome | Tipo | Propósito |
|---|------|------|----------|
| 1 | Título & Índice | Markdown | Visão geral e navegação |
| 2 | Imports & Config | Python | Carregar todos os pacotes e configurações |
| 3 | Utility Functions | Python | Funções reutilizáveis (load, display, etc) |
| 4 | Setup Guide | Markdown | Instruções de execução |

### **SEÇÃO 1: Dataset & Treinamento (Células 5-6)**
| # | Nome | Função Principal | Input | Output |
|----|------|------------------|-------|--------|
| 5 | Prep & Training | `prepare_yolo_dataset()`, `train_yolo_model()` | Imagens + Labels | Modelo .pt |
| 6 | Detecção & Avaliação | `detect_solar_panels()`, `evaluate_model()` | Modelo + Imagens | Detecções, Métricas |

### **SEÇÃO 2: Classificação (Células 7-10)**
| # | Nome | Propósito |
|----|------|----------|
| 7 | PropertyClassifier | Classificar tipo (residencial/comercial/industrial) |
| 9 | PowerEstimator | Estimar potência em kW |
| 8 | **Markdown** | Instruções de uso |
| 10 | **Exemplo** | Código de teste |

### **SEÇÃO 3: Visualização (Células 11-12)**
| # | Nome | Gera |
|----|------|------|
| 11 | Viz & Reports | Gráficos, dashboards Plotly, relatórios HTML |
| 12 | Full Pipeline | Pipeline completo: detecta → classifica → estima |

---

## 💡 Uso Rápido

### 1️⃣ **Carregar Modelo & Inicializar**
```python
from ultralytics import YOLO

# Carregar modelo treinado
model = YOLO('modelos/yolo_solar_panel.pt')

# Classificador e estimador já estão inicializados
# classifier = PropertyClassifier()
# estimator = PowerEstimator()
```

### 2️⃣ **Processar Uma Imagem**
```python
# Opção A: Pipeline completo (recomendado)
results = full_pipeline(
    'image.jpg',
    model,
    classifier,
    estimator,
    save_results=True,
    output_dir='./outputs'
)

print(f"Potência: {results['power_estimate']['total_power_kw']:.2f} kW")
print(f"Economia anual: R$ {results['annual_production']['annual_savings_brl']:,.2f}")
```

### 3️⃣ **Processar Múltiplas Imagens**
```python
# Batch processing com resumo em CSV
detections_list, summary_df = process_batch_images(
    model,
    'directory/with/images/',
    output_csv_path='resumo.csv',
    confidence_threshold=0.5
)

# Visualizar dashboard
visualize_batch_results(summary_df, save_path='dashboard.html')
```

### 4️⃣ **Visualizar Resultados**
```python
# Opção A: Detecções com bounding boxes
visualize_detections('image.jpg', detections, save_path='vis.png')

# Opção B: Relatório completo interativo
create_analysis_report(
    'image.jpg',
    detections,
    classification,
    power_estimate,
    save_path='relatorio.html'
)
```

---

## 🎯 Funções Principais

### **Detecção**
```python
results = detect_solar_panels(model, 'image.jpg', confidence=0.5)
# Retorna: {image_path, num_panels, detections, yolo_result}
```

### **Batch Processing**
```python
detections_list, df = process_batch_images(
    model,
    'img_dir/',
    output_csv_path='results.csv'
)
```

### **Classificação**
```python
prop_type, confidence, features = classifier.classify(detections)
# Retorna: ('residential' | 'commercial' | 'industrial' | 'substation', float, dict)
```

### **Estimativa de Potência**
```python
power_est = estimator.estimate_power(detections)
# Retorna: {total_power_kw, total_area_m2, num_panels, ...}

annual = estimator.estimate_annual_production(power_kw)
# Retorna: {annual_kwh, daily_kwh, annual_savings_brl, ...}
```

### **Pipeline Completo (MELHOR)**
```python
results = full_pipeline(
    image_path,
    model,
    classifier,
    estimator,
    confidence_threshold=0.5,
    save_results=True,
    output_dir='./outputs'
)
# Retorna: Dict com tudo: detecções, classificação, potência, produção
```

---

## 📊 Saídas Esperadas

### **Detecção Individual**
```json
{
  "image_path": "image.jpg",
  "num_panels_detected": 15,
  "detections": [
    {
      "x_min": 100,
      "y_min": 200,
      "x_max": 300,
      "y_max": 400,
      "confidence": 0.95,
      "area_pixels": 40000
    }
  ]
}
```

### **Classificação**
```
Tipo: COMMERCIAL
Confiança: 85%
Painéis: 12
Área: 24 m²
```

### **Potência**
```
Potência Instalada: 9.60 kW
Método: area_based
Área Total: 24.0 m²
Painéis: 12
```

### **Produção Anual**
```
Produção Anual: 17.280 MWh/ano
Produção Diária: 47.3 kWh/dia
Economia Anual: R$ 13,824.00
Fator de Capacidade: 18%
```

---

## 🔧 Personalizações Comuns

### **Mudar Confiança Mínima**
```python
results = detect_solar_panels(model, 'image.jpg', confidence_threshold=0.7)
```

### **Mudar Tamanho de Modelo YOLO**
```python
# Ao treinar: 'n' (nano) | 's' (small) | 'm' (medium) | 'l' (large) | 'x' (xlarge)
model, results = train_yolo_model(config_path, model_size='l', epochs=100)
```

### **Mudar Densidade de Potência**
```python
# Default: 150 W/m²
power_est = estimator.estimate_power(detections, power_density=200)
```

### **Mudar Tarifa de Eletricidade**
```python
# Default: R$ 0.80/kWh
# Na função estimate_annual_production()
# Editar variável: tariff = 0.95
```

---

## 📁 Estrutura de Diretórios Esperada

```
energy-netload-monitor/
├── notebooks/
│   ├── 09_yolo_solar_panel_detection_classification.ipynb ← VOCÊ ESTÁ AQUI
│   ├── data/
│   │   ├── images/              # Imagens para processar
│   │   ├── labels/              # Labels YOLO (opcional)
│   │   └── solar_panel/         # Dataset Lacuna Solar
│   └── modelos/                 # Modelos treinados (.pt)
└── outputs/                     # Resultados (gerado)
    ├── deteccoes_*.png
    ├── relatorio_*.html
    └── resultados.csv
```

---

## ⚙️ Configurações Padrão

| Parâmetro | Padrão | Ajustável |
|-----------|--------|-----------|
| **Confidence Threshold** | 0.5 | ✅ |
| **Model Size** | 'm' (medium) | ✅ |
| **Epochs** | 100 | ✅ |
| **Batch Size** | 16 | ✅ |
| **Power Density** | 150 W/m² | ✅ |
| **Tariff** | R$ 0.80/kWh | ✅ |
| **Capacity Factor** | 18% | ✅ |
| **Train/Val/Test Split** | 70/15/15 | ✅ |

---

## 🐛 Troubleshooting

### **Erro: "ModuleNotFoundError: No module named 'cv2'"**
→ Garantir que está usando o kernel correto com venv instalado

### **Erro: "CUDA out of memory"**
→ Reduzir `batch_size` em `train_yolo_model()` (de 16 para 8)

### **Modelo não melhora na validação**
→ Aumentar `epochs`, ajustar `augment=True`, adicionar `mosaic=1.0`

### **Detecções com baixa confiança**
→ Aumentar `confidence_threshold` de 0.5 para 0.6+

### **Relatórios HTML não abrindo**
→ Usar: `python -m http.server` para servir localmente

---

## 📚 Exemplos Completos

### **Exemplo 1: Análise Simples**
```python
model = YOLO('modelos/yolo_solar_panel.pt')
results = full_pipeline('image.jpg', model, classifier, estimator)
print(f"✓ {results['num_detections']} painéis detectados")
```

### **Exemplo 2: Batch com Exportação**
```python
# Processar pasta inteira
detections, df = process_batch_images(
    model, 'imagens/', 
    output_csv_path='resultados.csv'
)

# Visualizar
visualize_batch_results(df, save_path='dashboard.html')

# Exportar para GIS
df.to_csv('geo_export.csv')
```

### **Exemplo 3: Customizado**
```python
# Apenas detecção (sem classificação)
det_results = detect_solar_panels(model, 'img.jpg', confidence_threshold=0.7)
print(f"Painéis: {det_results['num_panels_detected']}")

# Apenas potência
power = estimator.estimate_power(det_results['detections'])
annual = estimator.estimate_annual_production(power['total_power_kw'])
print(f"Economia: R$ {annual['annual_savings_brl']:,.0f}")
```

---

## 📞 Suporte Rápido

**Arquivo de Referência**: `NOTEBOOK_REFACTORING_SUMMARY.md`  
**Status**: ✅ Refatorado e Otimizado  
**Última Atualização**: Janeiro 2025  
**Versão**: 1.1
