# Pipeline de Segmentação de Telhados - Guia de Integração com Notebooks YOLO

**Última atualização:** 29/01/2025  
**Status:** Pronto para integração  
**Versão:** 1.0

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Arquitetura da Solução](#arquitetura-da-solução)
3. [Como Funciona](#como-funciona)
4. [Integração com Notebooks](#integração-com-notebooks)
5. [API REST](#api-rest)
6. [Workflow Completo](#workflow-completo)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

O pipeline de segmentação de telhados resolve o problema de **como extrair imagens individuais de telhados de imagens de satélite para processar com seus modelos YOLO**.

### O Problema

```
Imagem Satélite Grande (Sentinel-2/Landsat)
    ↓
    Como extrair telhados individuais?
    Como segmentar quais pixels pertencem a cada telhado?
    Como preparar ROIs para seus modelos?
    ↓
Seus Modelos YOLO (painéis solares, cobertura, etc)
```

### A Solução

```
Sentine-2/Landsat
    ↓ download_imagem_satelite()
Imagem Baixada (numpy array)
    ↓ detectar_telhados() [YOLOv8n-seg]
Telhados Detectados (bounding boxes + segmentação)
    ↓ segmentar_telhados() [OpenCV + Morphology]
Telhados Segmentados (máscaras refinadas)
    ↓ extrair_rois_telhados() [Crop + Padding]
ROIs Individuais (imagens pequenas, pronto para YOLO)
    ↓
Seus Modelos YOLO (inferência)
    ↓
Painéis Solares, Cobertura, etc (DETECTADOS!)
    ↓ Salvar em PostgreSQL
Dashboard/Análise
```

---

## 🏗️ Arquitetura da Solução

### Componentes Principais

```
┌─────────────────────────────────────────────────────────┐
│                  BACKEND FASTAPI                         │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │  API REST (/telhados/*)                         │   │
│  │  - POST /segmentar-subestacao                   │   │
│  │  - GET /lista                                   │   │
│  │  - POST /processar-lote                         │   │
│  │  - POST /processar-com-yolo                     │   │
│  └─────────────────────────────────────────────────┘   │
│                      ↓                                   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  TelhadoSegmentationService                      │   │
│  │  - download_imagem_satelite()                    │   │
│  │  - detectar_telhados()                           │   │
│  │  - segmentar_telhados()                          │   │
│  │  - extrair_rois_telhados()                       │   │
│  │  - processar_telhados_lote()                     │   │
│  └─────────────────────────────────────────────────┘   │
│       ↓                        ↓                         │
│   OpenCV               YOLOv8n-seg                      │
│   NumPy                (Detecção)                       │
│   PIL                                                   │
│                                                          │
└─────────────────────────────────────────────────────────┘
          ↓                              ↓
      Disco Local                   PostgreSQL
      /data/rois/                   telhado_deteccoes
      (ROIs em PNG)                 telhado_rois
                                    telhado_processamento_yolo
```

### Stack Tecnológico

| Componente | Tecnologia | Versão | Descrição |
|-----------|-----------|--------|-----------|
| **Framework Web** | FastAPI | 0.100+ | REST API |
| **Detecção de Edifícios** | YOLOv8n-seg | 8.0+ | Detecta e segmenta telhados |
| **Processamento de Imagem** | OpenCV | 4.8+ | Refinamento de segmentação |
| **Cálculos Numéricos** | NumPy | 1.24+ | Operações em array |
| **Database** | PostgreSQL | 14+ | Armazenamento de metadados |
| **Validação** | Pydantic | 2.0+ | Schemas REST |
| **ML Model** | Ultralytics | 8.0+ | YOLOv8 framework |

---

## ⚙️ Como Funciona

### Passo 1: Download da Imagem

```python
def download_imagem_satelite(url_imagem: str) -> np.ndarray:
    """
    Baixa imagem de Sentinel-2 ou Landsat via URL
    
    Entrada: URL de uma imagem satélite
    Saída: Numpy array em BGR (OpenCV format)
    """
```

**Entrada esperada:**
```
URL do Planetary Computer, USGS, ou similar:
https://sentinel-hub-public.s3.amazonaws.com/tiles/S2/.../TCI.tif
```

**Saída:**
```python
array de shape (altura, largura, 3)  # BGR colorido
```

---

### Passo 2: Detecção de Telhados com YOLOv8

```python
def detectar_telhados(imagem: np.ndarray, 
                     confianca_minima: float = 0.5) -> List[TelhadoDetectado]:
    """
    Detecta telhados usando YOLOv8n-seg (nano segmentation)
    
    Entrada: Imagem numpy (BGR)
    Saída: Lista de objetos TelhadoDetectado com:
        - bbox em pixels (x, y, w, h)
        - confiança (0-1)
        - máscara de segmentação
    """
```

**O que retorna:**
```python
[
    TelhadoDetectado(
        id_telhado="telhado_0_0",
        bbox={"x": 100, "y": 150, "w": 50, "h": 40},
        confianca=0.92,
        area_pixeis=2000,
        ...
    ),
    # ... mais telhados
]
```

---

### Passo 3: Segmentação com OpenCV

```python
def segmentar_telhados(imagem: np.ndarray,
                       telhados: List[TelhadoDetectado]) -> List[TelhadoDetectado]:
    """
    Refina a detecção com segmentação por edge detection + morphology
    
    Para cada telhado:
    1. Extrai ROI da detecção
    2. Aplica Canny edge detection
    3. Refina bordas com morphological operations
    4. Encontra contorno mais preciso
    5. Cria máscara binária refinada
    """
```

**Operações:**
- Conversão RGB→Grayscale
- Histogram Equalization (melhor contraste)
- Bilateral Filter (suaviza mantendo bordas)
- Canny Edge Detection
- Morphological Close/Open (fechar lacunas)
- Contour Finding (contorno mais preciso)

---

### Passo 4: Extração de ROIs

```python
def extrair_rois_telhados(imagem: np.ndarray,
                         telhados: List[TelhadoDetectado],
                         resolucao_m_por_pixel: float = 3.0,
                         padding_percentual: float = 0.1) -> List[TelhadoSegmentado]:
    """
    Extrai imagem individual de cada telhado
    
    Para cada telhado:
    1. Aplicar padding (10% por padrão)
    2. Crop da imagem original
    3. Preservar máscara do telhado
    4. Salvar metadados
    5. Retornar objeto pronto para YOLO
    """
```

**Resultado:**
```python
TelhadoSegmentado(
    id_telhado="telhado_0_0",
    imagem_roi=array([[[B,G,R], ...], ...]),  # Imagem cropada
    mascara=array([[0,0,0,255,255,...], ...]), # Máscara do telhado
    tamanho_pixeis=(55, 65),
    resolucao_m_por_pixel=3.0,
    percentual_cobertura=95.5,
    caminho_arquivo="/data/rois/sub_001_telhado_0_0.png"
)
```

---

## 🔌 Integração com Notebooks

### Opção 1: Usar a API REST (Recomendado)

**Vantagens:**
- ✅ Desacoplado (não precisa do Notebook conhecer detalhes)
- ✅ Pode rodar em servidor separado (CPU/GPU)
- ✅ Escalável (múltiplas requisições paralelas)
- ✅ Fácil de cachear resultados

**Como usar no Notebook:**

```python
import requests
import json

# URL do backend (localhost se desenvolvendo localmente)
BASE_URL = "http://localhost:8000"

# 1. Segmentar telhados de uma subestação
response = requests.post(
    f"{BASE_URL}/telhados/segmentar-subestacao",
    json={
        "id_subestacao": "sub_001",
        "url_imagem_satelite": "https://...",
        "resolucao_m_por_pixel": 10.0,
        "confianca_minima": 0.5,
        "salvar_rois": True,
        "diretorio_saida": "./data/rois"
    }
)

resultado = response.json()

print(f"Telhados detectados: {resultado['telhados_detectados']}")
print(f"Telhados segmentados: {resultado['telhados_segmentados']}")

# 2. Listar ROIs geradas
for telhado in resultado['telhados']:
    roi_id = telhado['id_telhado']
    print(f"ROI: {roi_id}")

# 3. Para cada ROI, processar com seu modelo YOLO
for telhado_seg in resultado['telhados_segmentados']:
    caminho_roi = telhado_seg['caminho_arquivo_local']
    
    # Sua lógica de inferência YOLO aqui
    # ...
```

---

### Opção 2: Importar Diretamente no Notebook

**Vantagens:**
- ✅ Roda tudo no mesmo Notebook
- ✅ Fácil debug
- ❌ Menos escalável

**Como usar:**

```python
# No Notebook
import sys
sys.path.insert(0, '/path/to/energy-netload-monitor')

from backend.src.services.telhado_segmentation_service import TelhadoSegmentationService

# Criar serviço
servico = TelhadoSegmentationService(use_gpu=True)

# Processar
resultado = servico.processar_telhados_lote(
    url_imagem="https://...",
    id_subestacao="sub_001",
    id_imagem_satelite="sentinel2_20250129",
    resolucao_m_por_pixel=10.0,
    confianca_minima=0.5,
    diretorio_saida="./data/rois"
)

# Acessar ROIs
for roi in resultado.telhados_segmentados:
    print(f"ROI: {roi.id_telhado}")
    print(f"Caminho: {roi.caminho_arquivo}")
    # Processar com seu YOLO
```

---

## 📡 API REST

### POST /telhados/segmentar-subestacao

**Descrição:** Processa imagem de satélite e segmenta telhados

**Requisição:**
```json
{
  "id_subestacao": "sub_001",
  "url_imagem_satelite": "https://sentinel-hub...",
  "resolucao_m_por_pixel": 10.0,
  "confianca_minima": 0.5,
  "salvar_rois": true,
  "diretorio_saida": "./data/rois"
}
```

**Resposta (200):**
```json
{
  "id_subestacao": "sub_001",
  "telhados_detectados": 42,
  "telhados_segmentados": 40,
  "tempo_processamento_segundos": 45.3,
  "telhados": [
    {
      "id_telhado": "telhado_0_0",
      "bbox": {"x": 100, "y": 150, "largura": 50, "altura": 40},
      "area_m2": 18.0,
      "confianca": 0.92,
      "tipo_edificio": "residencial"
    },
    // ... mais telhados
  ],
  "telhados_segmentados": [
    {
      "id_telhado": "telhado_0_0",
      "tamanho_roi": [55, 65],
      "caminho_arquivo_local": "/data/rois/sub_001_telhado_0_0.png",
      "percentual_cobertura": 95.5,
      "indice_qualidade": 0.87
    }
  ]
}
```

---

### GET /telhados/lista

**Descrição:** Lista telhados com filtros e paginação

**Query Parameters:**
- `id_subestacao` (opcional): Filtrar por subestação
- `tipo_edificio` (opcional): residencial, comercial, industrial
- `confianca_minima` (float): 0-1, padrão 0.0
- `pagina` (int): padrão 1
- `limite` (int): 1-10000, padrão 100

**Exemplo:**
```
GET /telhados/lista?id_subestacao=sub_001&confianca_minima=0.8&pagina=1&limite=50
```

---

### POST /telhados/processar-lote

**Descrição:** Processa múltiplas subestações

**Requisição:**
```json
{
  "subestacoes": ["sub_001", "sub_002", "sub_003"],
  "imagens_por_subestacao": {
    "sub_001": "https://...",
    "sub_002": "https://...",
    "sub_003": "https://..."
  },
  "resolucao_m_por_pixel": 10.0,
  "confianca_minima": 0.5,
  "processar_com_yolo": false
}
```

---

### POST /telhados/processar-com-yolo

**Descrição:** Processa ROI com modelo YOLO registrado

**Requisição:**
```json
{
  "id_telhado": "telhado_0_0",
  "caminho_roi_local": "/data/rois/sub_001_telhado_0_0.png",
  "modelo_yolo_id": "solar-panels-v1",
  "parametros_modelo": {
    "confianca": 0.5,
    "iou": 0.45
  }
}
```

**Resposta:**
```json
{
  "id_telhado": "telhado_0_0",
  "modelo_yolo": "solar-panels-v1",
  "numero_paineis_detectados": 24,
  "confianca_media": 0.87,
  "area_coberta_percentual": 45.2,
  "sucesso": true
}
```

---

## 🔄 Workflow Completo

### Integração Notebook + API + YOLO

```
┌────────────────────────────────────────────────────────────┐
│                     SEU NOTEBOOK (Jupyter)                 │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  Cell 1: Importações                                        │
│  ────────────────────                                       │
│  import requests, cv2, numpy as np                          │
│  from ultralytics import YOLO                              │
│                                                             │
│  Cell 2: Função auxiliar para processar ROI                │
│  ────────────────────────────────────────                  │
│  def processar_roi_yolo(caminho_roi, modelo_yolo):         │
│      imagem = cv2.imread(caminho_roi)                      │
│      resultados = modelo_yolo(imagem)                      │
│      # extrair detecções, calcular métricas                │
│      return deteccoes, metricas                            │
│                                                             │
│  Cell 3: Chamar API de segmentação                         │
│  ────────────────────────────────────                      │
│  response = requests.post(                                 │
│      "http://localhost:8000/telhados/segmentar-subestacao",│
│      json={...}                                            │
│  )                                                          │
│  resultado = response.json()                               │
│                                                             │
│  Cell 4: Loop sobre ROIs e processar com YOLO             │
│  ────────────────────────────────────────────            │
│  modelo_solar = YOLO("modelos/solar-panels.pt")           │
│  resultados_finais = []                                    │
│                                                             │
│  for roi in resultado['telhados_segmentados']:             │
│      caminho_roi = roi['caminho_arquivo_local']            │
│      deteccoes, metricas = processar_roi_yolo(             │
│          caminho_roi, modelo_solar                         │
│      )                                                      │
│      resultados_finais.append({                            │
│          'id_telhado': roi['id_telhado'],                  │
│          'deteccoes': deteccoes,                           │
│          'metricas': metricas                              │
│      })                                                     │
│                                                             │
│  Cell 5: Visualizar e salvar resultados                    │
│  ──────────────────────────────────────                    │
│  # Gráficos, tabelas, etc.                                 │
│  # Salvar em CSV, JSON, etc.                               │
│                                                             │
└────────────────────────────────────────────────────────────┘
                           ↓
        ┌─────────────────────────────────┐
        │    Backend FastAPI + PostgreSQL │
        │                                 │
        │  POST /telhados/segmentar...   │
        │  ├─ Download imagem            │
        │  ├─ Detectar com YOLOv8        │
        │  ├─ Segmentar com OpenCV       │
        │  ├─ Extrair ROIs              │
        │  ├─ Salvar metadados          │
        │  └─ Retornar caminhos         │
        │                                 │
        └─────────────────────────────────┘
                           ↓
        Arquivo local: /data/rois/*.png
        Database: telhado_deteccoes, telhado_rois
```

---

## 📊 Exemplo Real: Detecção de Painéis Solares

```python
# NOTEBOOK: Detectar painéis solares em telhados de uma subestação

import requests
import cv2
import numpy as np
from ultralytics import YOLO
import pandas as pd

# ========== SETUP ==========
API_URL = "http://localhost:8000"
ID_SUBESTACAO = "sub_santos_001"
URL_IMAGEM = "https://sentinel-hub.../S2_TCI.tif"
MODELO_SOLAR = YOLO("modelos/yolov8n-solar-panels.pt")

# ========== PASSO 1: Segmentar telhados ==========
print("1️⃣ Segmentando telhados...")

response = requests.post(
    f"{API_URL}/telhados/segmentar-subestacao",
    json={
        "id_subestacao": ID_SUBESTACAO,
        "url_imagem_satelite": URL_IMAGEM,
        "resolucao_m_por_pixel": 10.0,
        "confianca_minima": 0.6,
        "salvar_rois": True,
        "diretorio_saida": "./data/rois_processadas"
    }
)

resultado_segmentacao = response.json()
print(f"✓ {resultado_segmentacao['telhados_detectados']} telhados detectados")
print(f"✓ {resultado_segmentacao['telhados_segmentados']} segmentados")

# ========== PASSO 2: Processar cada ROI com YOLO ==========
print("\n2️⃣ Detectando painéis solares...")

dados_finais = []

for roi in resultado_segmentacao['telhados_segmentados']:
    id_telhado = roi['id_telhado']
    caminho_roi = roi['caminho_arquivo_local']
    area_m2 = roi['tamanho_roi'][0] * roi['tamanho_roi'][1] * (roi['resolucao_m_por_pixel'] ** 2)
    
    # Carregar ROI
    img_roi = cv2.imread(caminho_roi)
    if img_roi is None:
        print(f"  ✗ Erro ao ler {caminho_roi}")
        continue
    
    # Detectar painéis com YOLO
    resultados_yolo = MODELO_SOLAR(img_roi)
    
    numero_paineis = 0
    area_paineis_total = 0
    confiancas = []
    
    for resultado in resultados_yolo:
        boxes = resultado.boxes
        if boxes is not None:
            for box in boxes:
                numero_paineis += 1
                confianca = float(box.conf[0])
                confiancas.append(confianca)
                
                # Calcular área do painel detectado
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                area_pixels_painel = (x2 - x1) * (y2 - y1)
                area_m2_painel = area_pixels_painel * (roi['resolucao_m_por_pixel'] ** 2)
                area_paineis_total += area_m2_painel
    
    # Calcular métricas
    confianca_media = np.mean(confiancas) if confiancas else 0
    percentual_coberto = (area_paineis_total / area_m2 * 100) if area_m2 > 0 else 0
    potencial_kw = numero_paineis * 0.4  # Assumindo 400W por painel
    
    dados_finais.append({
        'id_telhado': id_telhado,
        'numero_paineis': numero_paineis,
        'confianca_media': confianca_media,
        'area_telhado_m2': area_m2,
        'area_paineis_m2': area_paineis_total,
        'percentual_coberto': percentual_coberto,
        'potencial_kw': potencial_kw
    })
    
    print(f"  ✓ {id_telhado}: {numero_paineis} painéis ({percentual_coberto:.1f}% cobertura)")

# ========== PASSO 3: Resumo e análise ==========
print("\n3️⃣ Resumo da análise:")

df = pd.DataFrame(dados_finais)

print(f"\nTotal de telhados com painéis: {(df['numero_paineis'] > 0).sum()}")
print(f"Total de painéis detectados: {df['numero_paineis'].sum()}")
print(f"Potencial solar total: {df['potencial_kw'].sum():.1f} kW")
print(f"\nTelhados com maior potencial:")
print(df.nlargest(5, 'potencial_kw')[['id_telhado', 'numero_paineis', 'potencial_kw']])

# Salvar resultados
df.to_csv(f"{ID_SUBESTACAO}_analise_paineis.csv", index=False)
print(f"\n✓ Resultados salvos em {ID_SUBESTACAO}_analise_paineis.csv")
```

---

## 🆘 Troubleshooting

### Problema: "CUDA out of memory"

**Causa:** Imagem muito grande ou modelo requer muita VRAM

**Soluções:**
```python
# 1. Usar modelo menor (nano ao invés de small)
servico = TelhadoSegmentationService(model_path="yolov8n-seg.pt")

# 2. Reduzir resolução da imagem
# (adição ao código do serviço)

# 3. Processar em lotes menores
# Dividir subestações em grupos
```

### Problema: "No telhados detectados"

**Causas potenciais:**
1. Confiança muito alta (aumentar de 0.5 para 0.3)
2. Imagem com baixa qualidade/nublada
3. Modelo YOLO não treinado para este tipo de area

**Soluções:**
```python
# Tentar com confiança mais baixa
resultado = servico.processar_telhados_lote(
    ...,
    confianca_minima=0.3  # Reduzir de 0.5
)

# Verificar imagem
import cv2
img = cv2.imread("sua_imagem.png")
print(f"Shape: {img.shape}, Min: {img.min()}, Max: {img.max()}")
```

### Problema: ROIs com qualidade ruim

**Soluções:**
```python
# 1. Aumentar padding
rois = servico.extrair_rois_telhados(
    ...,
    padding_percentual=0.2  # Aumentar de 0.1
)

# 2. Filtrar por indice_qualidade
boas_rois = [r for r in rois if r.indice_qualidade > 0.7]
```

### Problema: Erro ao conectar com API

```bash
# Verificar se Backend está rodando
curl http://localhost:8000/docs

# Se não, iniciar:
cd backend
python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

---

## 📦 Requisitos de Instalação

Adicionar ao `requirements.txt`:

```
ultralytics>=8.0.0          # YOLOv8
opencv-python>=4.8.0        # OpenCV
torch>=2.0.0                # PyTorch (CPU/GPU)
torchvision>=0.15.0         # Vision tools
pillow>=10.0.0              # Image processing
requests>=2.31.0            # HTTP client
numpy>=1.24.0               # Numerical computing
```

Instalar GPU (opcional mas recomendado):
```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

---

## 🚀 Próximos Passos

1. **Deploy do Backend**
   - Executar em servidor separado com GPU
   - Configurar Redis para cache distribuído
   - Setup de PostgreSQL

2. **Treinamento de Modelo YOLO Customizado**
   - Coletar dataset de painéis solares brasileiros
   - Fine-tune de modelo pré-treinado
   - Validação em produção

3. **Fila de Processamento**
   - Implementar Celery para processamento assíncrono
   - Escalar para múltiplas GPUs

4. **Dashboard**
   - Visualizar ROIs detectadas
   - Mostrar estatísticas por subestação
   - Alertas de anomalias

---

## 📞 Suporte

Para dúvidas ou problemas:
1. Consulte os exemplos em `scripts/exemplo_telhados_workflow.py`
2. Verifique logs em `telhados_pipeline.log`
3. Acesse documentação da API em `http://localhost:8000/docs`

---

**Documento Preparado:** Energy Netload Monitor Team  
**Última Atualização:** 29 de Janeiro de 2025  
**Versão:** 1.0 - Beta
