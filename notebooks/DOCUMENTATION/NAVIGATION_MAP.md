# 🗺️ Mapa de Navegação - YOLO Solar Panel Notebook

## 📍 Você está aqui

**Notebook**: `09_yolo_solar_panel_detection_classification.ipynb`  
**Versão**: 1.1.0 (Refatorado)  
**Status**: ✅ Pronto para Produção  
**Células**: 31 (reduzido de 39)

---

## 🧭 Estrutura Completa do Notebook

### **SEÇÃO 0: Configuração e Preparação (Células 1-4)**

#### Cell 1: 📌 Título & Índice
- **Tipo**: Markdown
- **Conteúdo**: Título "🌞 YOLO Solar Panel Detection", índice e quick start
- **Use quando**: Começar o notebook, entender estrutura
- **Links**: Índice com links para cada seção

#### Cell 2: ⚙️ Environment Setup & Imports
- **Tipo**: Python
- **Conteúdo**: 
  - Imports divididos em 5 seções
  - Configuração de caminhos (BASE_PATH, DATA_PATH, MODELS_PATH, OUTPUT_PATH)
  - Module docstring
  - Type hints (Tuple, Dict, List, Optional)
- **Funções**: Nenhuma (apenas setup)
- **Saída**: Exibe configuração do ambiente
- **Use quando**: Inicializar notebook, verificar imports
- **Modificar**: Para adicionar novo pacotes ou mudar caminhos

#### Cell 3: 🔧 Utility Functions
- **Tipo**: Python
- **Conteúdo**: 4 funções reutilizáveis
  - `load_image(path)` - Carrega imagem com tratamento de erro
  - `display_image(img, title)` - Mostra imagem com matplotlib
  - `ensure_directory(path)` - Cria diretório seguramente
  - `print_section(title)` - Imprime separador formatado
- **Use quando**: Precisar de funções auxiliares simples
- **Modificar**: Para customizar formatação ou funcionalidade

#### Cell 4: 📖 Setup Guide & Objectives
- **Tipo**: Markdown
- **Conteúdo**:
  - Objetivos do notebook (🎯)
  - Pipeline ASCII diagram (📊)
  - Usage instructions (🚀)
- **Use quando**: Entender o fluxo de trabalho, ensinar outros
- **Modificar**: Para adicionar novos objetivos ou instruções

---

### **SEÇÃO 1: Dataset & Treinamento (Células 5-6)**

#### Cell 5: 📊 Dataset Preparation & Training
- **Tipo**: Python
- **Funções**:
  1. **`prepare_yolo_dataset(images_dir, labels_dir, output_dir, train_ratio=0.7, val_ratio=0.15)`**
     - Cria estrutura YOLO com split train/val/test
     - Copia imagens e labels para diretórios corretos
     - Usa `sklearn.model_selection.train_test_split`
     - **Input**: Diretórios com imagens e labels
     - **Output**: Path do dataset estruturado
     - **Use quando**: Preparar dados para treinamento
  
  2. **`create_yolo_config(dataset_path, config_file='data.yaml')`**
     - Cria arquivo data.yaml padrão YOLO
     - Define paths de train/val/test
     - Define classes (apenas solar_panel)
     - **Input**: Path do dataset
     - **Output**: Path do arquivo YAML
     - **Use quando**: Configurar modelo YOLO
  
  3. **`train_yolo_model(config_path, model_size='m', epochs=100, imgsz=640, batch_size=16, patience=20, device=0)`**
     - Treina modelo YOLOv8 com augmentação
     - Suporta tamanhos: 'n', 's', 'm', 'l', 'x'
     - Aplica augmentação: flip, rotate, HSV shifts
     - Early stopping com patience
     - **Input**: Config path, hyperparameters
     - **Output**: Modelo treinado, histórico de resultados
     - **Use quando**: Treinar novo modelo ou ajustar hiperparâmetros

- **Exemplo de Uso**:
  ```python
  # Preparar dados
  dataset_dir = prepare_yolo_dataset(
      images_dir='./data/images',
      labels_dir='./data/labels',
      output_dir='./data/yolo_dataset'
  )
  
  # Configurar
  config_path = create_yolo_config(dataset_dir)
  
  # Treinar
  model, results = train_yolo_model(config_path, model_size='m', epochs=100)
  model.save('modelos/yolo_solar.pt')
  ```

#### Cell 6: 🔍 Detection & Evaluation
- **Tipo**: Python
- **Funções**:
  1. **`detect_solar_panels(model, image_path, confidence_threshold=0.5)`**
     - Detecta painéis em uma imagem individual
     - Calcula features: área, coordenadas, confiança
     - **Input**: Modelo, caminho da imagem
     - **Output**: Dict com detections, num_panels, yolo_result
     - **Use quando**: Processar imagem individual

  2. **`process_batch_images(model, image_directory, output_csv_path=None, confidence_threshold=0.5)`**
     - Processa múltiplas imagens em lote
     - Exporta resumo em CSV
     - **Input**: Modelo, diretório com imagens
     - **Output**: Lista de results, DataFrame com resumo
     - **Use quando**: Processar pasta inteira de imagens

  3. **`evaluate_model(model, test_config_path)`**
     - Avalia modelo em dataset de teste
     - Calcula métricas: mAP@50, mAP@50:95, Precision, Recall
     - **Input**: Modelo treinado, config path
     - **Output**: Dict de métricas, objeto resultado
     - **Use quando**: Validar qualidade do modelo

  4. **`plot_training_results(results_dir)`**
     - Plota gráficos de treinamento (Loss, Precision, Recall)
     - Gera figure Plotly com 4 subgráficos
     - **Input**: Diretório com results.csv
     - **Output**: Figura Plotly interativa
     - **Use quando**: Analisar histórico de treinamento

---

### **SEÇÃO 2: Classificação (Células 7-10)**

#### Cell 7: 🏠 PropertyClassifier
- **Tipo**: Python
- **Classe**: `PropertyClassifier`
- **Métodos**:
  - `extract_features(detections)` - Extrai 10 features dos painéis
  - `classify(detections)` - Classifica tipo (residencial/comercial/industrial/substation)
  - `generate_report(image_path, detections, classification)` - Gera relatório
- **Lógica**: Usa número de painéis e variância de área
- **Saída**: Tipo de propriedade + confiança (0-1)
- **Use quando**: Classificar imagem em categoria de propriedade

#### Cell 8: 📌 Markdown - Instruções
- Explica estimativa de potência
- Descreve metodologia
- Mostra fórmulas de cálculo

#### Cell 9: ⚡ PowerEstimator
- **Tipo**: Python
- **Classe**: `PowerEstimator`
- **Métodos**:
  - `pixels_to_meters(pixel_area)` - Converte pixels → m²
  - `estimate_power(detections, efficiency=0.15, power_density=150)` - Estima potência
  - `estimate_annual_production(power_kw, capacity_factor=0.18)` - Calcula produção anual
- **Saída**: 
  - Potência em kW
  - Produção anual em kWh
  - Economia estimada em R$
- **Use quando**: Estimar potência e economia

#### Cell 10: 💡 Exemplo de Uso
- Código exemplo de como usar os classificadores
- Test simples com dados dummy

---

### **SEÇÃO 3: Visualização (Células 11-12)**

#### Cell 11: 📈 Visualization & Reports
- **Tipo**: Python
- **Funções**:
  1. **`visualize_detections(image_path, detections, title, save_path, figsize)`**
     - Desenha bounding boxes sobre imagem
     - Cores dinâmicas baseadas em confiança
     - Salva em PNG
     - **Use quando**: Visualizar detecções em imagem
  
  2. **`visualize_batch_results(results_df, save_path)`**
     - Dashboard Plotly com 4 gráficos
     - Gráficos: Detecções, Confiança vs Área, Histograma, Área Total
     - Exporta HTML interativo
     - **Use quando**: Resumir resultados de múltiplas imagens
  
  3. **`create_analysis_report(image_path, detections, classification, power_estimate, save_path)`**
     - Relatório completo com 4 subgráficos
     - Subgráficos: Pie (confiança), Bar (área), Scatter (área vs conf), Indicator (potência)
     - Exporta HTML interativo
     - **Use quando**: Gerar relatório visual detalhado

---

### **SEÇÃO 4: Pipeline Completo (Célula 12-13)**

#### Cell 12: 🚀 Full Pipeline
- **Tipo**: Python
- **Função**: **`full_pipeline(image_path, model, classifier, estimator, confidence_threshold=0.5, save_results=False, output_dir=None)`**
- **O que faz**:
  1. ▶️ **Etapa 1**: Detecção com YOLOv8
  2. ▶️ **Etapa 2**: Classificação de propriedade
  3. ▶️ **Etapa 3**: Estimativa de potência
  4. ▶️ **Etapa 4**: Produção anual e economia
- **Saída**: Dict com tudo integrado
  - `num_detections`: Número de painéis
  - `classification`: Tipo + confiança
  - `power_estimate`: Potência em kW
  - `annual_production`: Produção em kWh, economia em R$
  - `timestamps`: Tempos de cada etapa
- **Salva**: 
  - `deteccoes_*.png` - Visualização com bounding boxes
  - `relatorio_*.html` - Relatório interativo
- **Use quando**: Processar uma imagem de forma completa

- **Exemplo**:
  ```python
  results = full_pipeline(
      'sample.jpg', model, classifier, estimator,
      save_results=True, output_dir='./outputs'
  )
  
  # Acessar resultados
  print(f"Painéis: {results['num_detections']}")
  print(f"Potência: {results['power_estimate']['total_power_kw']:.2f} kW")
  print(f"Economia: R$ {results['annual_production']['annual_savings_brl']:.2f}")
  ```

---

### **SEÇÃO 5: Integração (Células 14+)**

#### Cell 14: 📖 Integration Guide
- **Tipo**: Markdown
- **Conteúdo**: Exemplos de integração com dados reais
  - Carregar Lacuna Solar Survey
  - Integrar com dados de subestações
  - Exportar resultados (CSV, JSON, GeoJSON)

#### Cell 15+: Exemplos de Uso
- Código de teste e validação
- Exemplos práticos de cada função

---

## 🎯 Como Usar Este Notebook

### ✅ Para Iniciantes
1. **Leia**: Cell 1 (Índice) + Cell 4 (Guia)
2. **Execute**: Cell 2-3 (Setup)
3. **Experimente**: Cell 13 (Full Pipeline) com imagem de exemplo
4. **Consulte**: `QUICK_REFERENCE.md`

### ⚙️ Para Customização
1. **Modifique**: Cell 2 (mudar caminhos, imports)
2. **Ajuste**: Cell 5 (hyperparâmetros de treinamento)
3. **Configure**: Cell 6 (thresholds de confiança)
4. **Execute**: Full pipeline com seus dados

### 🔧 Para Desenvolvimento
1. **Estude**: Docstrings de cada função (Cell 5-12)
2. **Inspecione**: Type hints e estrutura de dados
3. **Modifique**: Funções individuais conforme necessário
4. **Teste**: Execute cells incrementalmente

### 📊 Para Produção
1. **Refatore**: Funções em módulos `.py` separados
2. **Crie**: API REST com Flask/FastAPI
3. **Containerize**: Docker image para deploy
4. **Monitore**: Logs e métricas de performance

---

## 📂 Estrutura de Arquivos Relacionados

```
notebooks/
├── 09_yolo_solar_panel_detection_classification.ipynb ← AQUI
├── QUICK_REFERENCE.md ← Guia rápido de uso
├── NOTEBOOK_REFACTORING_SUMMARY.md ← Detalhes técnicos
├── CHANGELOG_REFACTORING.md ← Histórico de mudanças
└── data/
    ├── images/ ← Suas imagens para processar
    ├── labels/ ← Labels YOLO (opcional)
    └── solar_panel/ ← Dataset Lacuna Solar Survey
└── modelos/
    └── yolo_solar_panel.pt ← Modelo treinado
```

---

## 🔍 Índice de Funções por Caso de Uso

### **Caso 1: Tenho uma imagem, quero detectar painéis**
```python
detection = detect_solar_panels(model, 'image.jpg')
visualize_detections('image.jpg', detection['detections'])
```
→ Vá para: Cell 6

### **Caso 2: Tenho uma pasta com imagens, quero processar todas**
```python
detections, df = process_batch_images(model, 'folder/')
visualize_batch_results(df, save_path='dashboard.html')
```
→ Vá para: Cell 6, Cell 11

### **Caso 3: Quero relatório completo de uma imagem**
```python
results = full_pipeline('image.jpg', model, classifier, estimator,
                       save_results=True, output_dir='./out')
```
→ Vá para: Cell 12

### **Caso 4: Quero treinar meu próprio modelo**
```python
dataset_dir = prepare_yolo_dataset('imgs/', 'labels/', 'dataset/')
model, results = train_yolo_model(create_yolo_config(dataset_dir))
```
→ Vá para: Cell 5

### **Caso 5: Quero estimar potência e economia**
```python
power = estimator.estimate_power(detections)
annual = estimator.estimate_annual_production(power['total_power_kw'])
```
→ Vá para: Cell 9

---

## 📞 Suporte Rápido

| Pergunta | Resposta | Link |
|----------|----------|------|
| Como começo? | Leia Cell 1 + Cell 4 | Cell 1 |
| Qual função usar? | Veja "Índice de Funções" acima | ↑ |
| Exemplos de código? | Consulte QUICK_REFERENCE.md | Arquivo |
| Como modificar? | Leia docstrings em cada função | Cell 5-12 |
| Troubleshooting? | Veja CHANGELOG_REFACTORING.md | Arquivo |

---

## ✨ Versão & Status

- **Versão**: 1.1.0
- **Status**: ✅ Refatorado e Pronto para Produção
- **Data**: Janeiro 2025
- **Células**: 31 (consolidadas de 39)
- **Documentação**: Completa
- **Type Hints**: 100%
- **Docstrings**: 100%

---

**🚀 Você está pronto para usar este notebook!**

Escolha seu caso de uso acima e comece.
