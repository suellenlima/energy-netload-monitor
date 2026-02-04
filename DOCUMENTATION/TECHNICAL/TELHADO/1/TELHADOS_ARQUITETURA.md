# 🏗️ Arquitetura do Pipeline de Segmentação de Telhados

## Visão Geral do Sistema

```
┌─────────────────────────────────────────────────────────────────────┐
│                          ENTRADA                                     │
├─────────────────────────────────────────────────────────────────────┤
│  Imagem de Satélite (Sentinel-2, Landsat, etc)                      │
│  - Resolução: 10-30 metros por pixel                                │
│  - Bandas: RGB + NIR + SWIR                                         │
│  - Tamanho: 100MB - 1GB por cena                                    │
│  - Acesso: URLs públicas (HTTP/Cloud APIs)                         │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │  BACKEND FastAPI (backend/src/main.py)           │
        └──────────────────────────────────────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │  API REST Endpoints (backend/src/api/telhado.py)│
        │                                                  │
        │  ├─ POST /telhados/segmentar-subestacao        │
        │  ├─ GET  /telhados/lista                       │
        │  ├─ POST /telhados/processar-lote              │
        │  ├─ GET  /telhados/estatisticas                │
        │  ├─ POST /telhados/processar-com-yolo          │
        │  └─ POST /telhados/registrar-modelo-yolo       │
        │                                                  │
        └──────────────────────────────────────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │  TelhadoSegmentationService                      │
        │  (backend/src/services/telhado_segmentation_    │
        │   service.py)                                    │
        └──────────────────────────────────────────────────┘
                ↓               ↓               ↓
    ┌───────────────┐  ┌──────────────┐  ┌──────────────┐
    │ PASSO 1:      │  │ PASSO 2:     │  │ PASSO 3:     │
    │ Download      │  │ Detecção     │  │ Segmentação  │
    │               │  │              │  │              │
    │ requests lib  │  │ YOLOv8n-seg  │  │ OpenCV       │
    │ PIL / NumPy   │  │ (detecção +  │  │ - Canny edge │
    │               │  │  segmentação)│  │ - Morphology │
    │ Output:       │  │              │  │ - Contours   │
    │ numpy array   │  │ Output:      │  │              │
    │ (BGR)         │  │ TelhadoDetec │  │ Output:      │
    │               │  │ -tado list   │  │ Máscaras     │
    └───────────────┘  └──────────────┘  └──────────────┘
                                ↓
                        ┌────────────────┐
                        │ PASSO 4:       │
                        │ Extração ROIs  │
                        │                │
                        │ - Crop imagem  │
                        │ - Aplicar pad  │
                        │ - Salvar PNG   │
                        │                │
                        │ Output:        │
                        │ TelhadoSegm    │
                        │ -entado list   │
                        └────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │                    SAÍDA                          │
        ├──────────────────────────────────────────────────┤
        │                                                  │
        │  1. METADADOS (Banco de Dados)                 │
        │  ├─ telhado_deteccoes                         │
        │  │  (42 telhados × subestação)                │
        │  ├─ telhado_rois                              │
        │  │  (40 ROIs extraídas)                        │
        │  ├─ telhado_processamento_yolo                │
        │  │  (resultados de modelos)                    │
        │  └─ telhado_modelos_yolo                      │
        │     (modelos registrados)                      │
        │                                                  │
        │  2. ARQUIVOS (Disco Local)                     │
        │  ├─ /data/rois/sub_001_telhado_0_0.png       │
        │  ├─ /data/rois/sub_001_telhado_0_1.png       │
        │  ├─ ... (40 arquivos por subestação)          │
        │  └─ resultado_*.json                           │
        │                                                  │
        │  3. CACHE                                       │
        │  └─ telhado_cache_segmentacao                 │
        │     (evitar reprocessamento)                    │
        │                                                  │
        └──────────────────────────────────────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │       SEUS MODELOS YOLO (Seu Notebook)          │
        │                                                  │
        │  ├─ Solar Panel Detection (painéis)            │
        │  ├─ Roof Type Classification (tipo cobertura)  │
        │  ├─ Structure Detection (antenas, chaminés)    │
        │  └─ Damage Assessment (inspeção visual)        │
        │                                                  │
        │  Entrada: Imagens PNG dos /data/rois/         │
        │  Saída: Detecções + classificações            │
        │                                                  │
        └──────────────────────────────────────────────────┘
                                ↓
        ┌──────────────────────────────────────────────────┐
        │              RESULTADOS FINAIS                   │
        │                                                  │
        │  ├─ Número de painéis por telhado             │
        │  ├─ Percentual de cobertura                    │
        │  ├─ Potencial solar estimado                   │
        │  ├─ Tipo de cobertura predominante            │
        │  ├─ Anomalias e riscos estruturais            │
        │  └─ Visualizações e dashboards                │
        │                                                  │
        └──────────────────────────────────────────────────┘
```

---

## 🔄 Fluxo de Dados Detalhado

### Passo 1: Download

```
URL da Imagem (HTTP)
    ↓
requests.get(url)
    ↓
PIL.Image.open(BytesIO(response.content))
    ↓
numpy array RGB [altura, largura, 3]
    ↓
cv2.cvtColor(RGB → BGR) [compatibilidade OpenCV]
    ↓
numpy array BGR pronto para processamento
```

### Passo 2: Detecção com YOLOv8

```
Input: Imagem BGR [512x512 típico]
    ↓
YOLOv8n-seg.predict(imagem)
    ↓
[
  Detecção 0: {
    bbox: [x1, y1, x2, y2],
    confiança: 0.92,
    classe: "edifício",
    máscara: [bytes]
  },
  Detecção 1: {...},
  ...
]
    ↓
TelhadoDetectado objects criados
    ↓
Total: 40-100 telhados por imagem típica
```

### Passo 3: Segmentação com OpenCV

```
Para cada TelhadoDetectado:
    ↓
1. Extrair ROI: imagem[y:y+h, x:x+w]
    ↓
2. Converter para escala de cinza: cv2.cvtColor(BGR → GRAY)
    ↓
3. Equalizar histograma: cv2.equalizeHist()
    ↓
4. Suavizar: cv2.bilateralFilter()
    ↓
5. Detector de bordas: cv2.Canny()
    ↓
6. Morfologia: cv2.morphologyEx(CLOSE, OPEN)
    ↓
7. Encontrar contornos: cv2.findContours()
    ↓
8. Maior contorno → máscara refinada
    ↓
TelhadoDetectado.mascara_segmentacao atualizada
```

### Passo 4: Extração de ROIs

```
Para cada TelhadoDetectado segmentado:
    ↓
1. Aplicar padding: padding = bbox_width * padding_percentual (10%)
    ↓
2. Crop com padding: 
   roi_imagem = imagem[y-pad:y+h+pad, x-pad:x+w+pad]
    ↓
3. Criar máscara correspondente
    ↓
4. Salvar em PNG:
   /data/rois/[id_subestacao]_[id_telhado].png
    ↓
5. Calcular estatísticas:
   - tamanho real em m² (resolução × pixels)
   - índice de qualidade (contraste/sharpness)
   - percentual de cobertura
    ↓
TelhadoSegmentado object retornado
```

---

## 💾 Estrutura de Dados

### Input: TelhadoDetectado

```python
@dataclass
class TelhadoDetectado:
    # Identificação
    id_telhado: str              # "telhado_0_0"
    id_subestacao: str           # "sub_001"
    id_imagem_satelite: str      # "sentinel2_20250129"
    
    # Localização em pixels
    bbox: Dict = {
        "x": 100,                # Posição X
        "y": 150,                # Posição Y
        "w": 50,                 # Largura
        "h": 40                  # Altura
    }
    
    # Localização normalizada (0-1)
    bbox_normalizado: Dict = {
        "x": 0.195,
        "y": 0.293,
        "w": 0.098,
        "h": 0.078
    }
    
    # Centróide
    centroide: Dict = {
        "x": 125,                # Centro X
        "y": 170                 # Centro Y
    }
    
    # Coordenadas geográficas
    lat: float = -23.550        # Latitude
    lon: float = -46.633        # Longitude
    
    # Propriedades
    area_pixeis: int = 2000     # Pixels totais
    area_m2: float = 18.0       # Metros quadrados
    confianca: float = 0.92     # Confiança YOLOv8
    tipo_edificio: str = "residencial"
    
    # Segmentação
    mascara_segmentacao: np.ndarray  # Máscara binária
    contorno: List[Tuple] = [...]    # Pontos do contorno
    
    # Metadados
    timestamp_deteccao: datetime
    modelo_deteccao: str = "yolov8n-seg"
    propriedades_adicionais: Dict = {
        "indice_qualidade": 0.87,
        "percentual_cobertura": 95.5
    }
```

### Output: TelhadoSegmentado

```python
@dataclass
class TelhadoSegmentado:
    # Identificação
    id_telhado: str              # "telhado_0_0"
    
    # Imagem e máscara
    imagem_roi: np.ndarray       # Imagem cropada (55x65, RGB)
    mascara: np.ndarray          # Máscara binária (55x65, 0-255)
    
    # Geometria
    bbox_original: Dict = {
        "x": 100, "y": 150,
        "w": 50, "h": 40
    }
    
    # Resolução
    tamanho_pixeis: Tuple = (55, 65)        # (altura, largura)
    resolucao_m_por_pixel: float = 10.0     # 10m Sentinel-2
    
    # Qualidade
    percentual_cobertura: float = 95.5      # % que é telhado
    indice_qualidade: float = 0.87          # 0-1 baseado em contraste
    
    # Armazenamento
    caminho_arquivo: str = "/data/rois/sub_001_telhado_0_0.png"
```

---

## 🗄️ Modelo de Dados PostgreSQL

### Tabela: telhado_deteccoes

```sql
CREATE TABLE telhado_deteccoes (
    id_deteccao SERIAL PRIMARY KEY,
    id_telhado VARCHAR(100) NOT NULL UNIQUE,
    id_subestacao INTEGER,
    
    -- Bounding box em pixels
    bbox_x INTEGER,              -- 100
    bbox_y INTEGER,              -- 150
    bbox_largura INTEGER,        -- 50
    bbox_altura INTEGER,         -- 40
    
    -- Bounding box normalizado (0-1)
    bbox_x_norm FLOAT,           -- 0.195
    bbox_y_norm FLOAT,           -- 0.293
    bbox_w_norm FLOAT,           -- 0.098
    bbox_h_norm FLOAT,           -- 0.078
    
    -- Centróide
    centroide_x FLOAT,           -- 125.0
    centroide_y FLOAT,           -- 170.0
    
    -- Coordenadas geográficas
    latitude FLOAT,              -- -23.550
    longitude FLOAT,             -- -46.633
    
    -- Propriedades
    area_pixeis INTEGER,         -- 2000
    area_m2 FLOAT,               -- 18.0
    confianca_deteccao FLOAT,    -- 0.92
    tipo_edificio VARCHAR(50),   -- 'residencial'
    
    -- Qualidade
    percentual_cobertura FLOAT,  -- 95.5
    indice_qualidade FLOAT,      -- 0.87
    
    -- Máscara (binária comprimida)
    mascara_contorno BYTEA,
    
    -- Metadados
    modelo_deteccao VARCHAR(100),
    timestamp_deteccao TIMESTAMP,
    propriedades_json JSONB,
    
    -- Controle
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT TRUE,
    
    -- Índices
    INDEX idx_subestacao (id_subestacao),
    INDEX idx_timestamp (timestamp_deteccao DESC),
    INDEX idx_geo (latitude, longitude)
);
```

### Tabela: telhado_rois

```sql
CREATE TABLE telhado_rois (
    id_roi SERIAL PRIMARY KEY,
    id_deteccao INTEGER NOT NULL,  -- FK para telhado_deteccoes
    
    -- Tamanho
    tamanho_altura INTEGER,        -- 55 pixels
    tamanho_largura INTEGER,       -- 65 pixels
    
    -- Resolução
    resolucao_m_por_pixel FLOAT,   -- 10.0
    area_aproximada_m2 FLOAT,      -- 36.25
    
    -- Qualidade
    percentual_cobertura FLOAT,    -- 95.5%
    indice_qualidade_roi FLOAT,    -- 0.87
    
    -- Armazenamento
    caminho_arquivo_local VARCHAR(500),
    url_storage_s3 VARCHAR(500),
    tamanho_arquivo_kb FLOAT,
    hash_arquivo VARCHAR(64),      -- SHA256
    
    -- Metadados
    timestamp_criacao TIMESTAMP,
    timestamp_expiracao TIMESTAMP, -- TTL
    processada BOOLEAN DEFAULT FALSE,
    
    -- Índices
    INDEX idx_deteccao (id_deteccao),
    INDEX idx_processada (processada),
    INDEX idx_hash (hash_arquivo)
);
```

### Tabela: telhado_processamento_yolo

```sql
CREATE TABLE telhado_processamento_yolo (
    id_processamento SERIAL PRIMARY KEY,
    id_roi INTEGER NOT NULL,       -- FK para telhado_rois
    
    -- Modelo
    modelo_yolo_id VARCHAR(100),   -- "solar-panels-v1"
    modelo_yolo_versao VARCHAR(20),
    tipo_deteccao VARCHAR(50),     -- "solar-panels"
    
    -- Resultados
    numero_objetos_detectados INTEGER,
    numero_paineis_solares_detectados INTEGER,
    confianca_media FLOAT,         -- 0-1
    area_coberta_percentual FLOAT, -- 45.2%
    
    -- Performance
    tempo_inferencia_ms FLOAT,     -- 125.5
    timestamp_processamento TIMESTAMP,
    
    -- Detalhes JSON
    deteccoes_json JSONB,
    propriedades_calculadas JSONB, -- {"potencial_mw": 7.2, ...}
    
    -- Status
    sucesso BOOLEAN,
    mensagem_erro VARCHAR(500),
    
    -- Índices
    INDEX idx_roi (id_roi),
    INDEX idx_modelo (modelo_yolo_id),
    INDEX idx_tipo (tipo_deteccao)
);
```

---

## 🚀 Fluxo de Processamento Paralelo

```
Lote: [sub_001, sub_002, sub_003, sub_004, sub_005]
    ↓
    ├─ Thread 1: sub_001 ──→ 42 telhados → 40 ROIs
    ├─ Thread 2: sub_002 ──→ 38 telhados → 36 ROIs
    ├─ Thread 3: sub_003 ──→ 45 telhados → 43 ROIs
    ├─ Thread 4: sub_004 ──→ 41 telhados → 39 ROIs
    └─ Thread 5: sub_005 ──→ 39 telhados → 37 ROIs
    
    Tempo: ~40s por thread × 5 threads = 40s total (paralelo)
    vs. 200s total (sequencial)
    
    Speedup: 5x com processamento paralelo
```

---

## 📊 Integrações e Dependências

```
┌──────────────────────────────────────┐
│  Camada de Apresentação              │
│  (Frontend/Notebooks)                │
└──────────────────────────────────────┘
                    ↑
┌──────────────────────────────────────┐
│  API REST (FastAPI)                  │
│  - Documentação automática (Swagger) │
│  - Validação Pydantic               │
│  - CORS habilitado                   │
└──────────────────────────────────────┘
                    ↑
┌──────────────────────────────────────┐
│  Serviço de Segmentação              │
│  - Download de imagens               │
│  - Orquestração do pipeline          │
│  - Salvar resultados                 │
└──────────────────────────────────────┘
                ↙        ↓
    ┌────────────┐  ┌────────────┐
    │ YOLOv8n-   │  │  OpenCV +  │
    │ seg (GPU)  │  │  NumPy     │
    │            │  │  PIL       │
    │ PyTorch    │  │  Requests  │
    │ CUDA       │  │            │
    └────────────┘  └────────────┘
            ↓              ↓
    ┌────────────────────────────┐
    │     Armazenamento          │
    │                            │
    │  ├─ Disco Local (/data/)  │
    │  ├─ PostgreSQL Database   │
    │  └─ Cache (Redis opção)   │
    │                            │
    └────────────────────────────┘
```

---

## ⚡ Otimizações Implementadas

1. **Cache de STAC Queries** - Evita chamar API múltiplas vezes
2. **Batch Processing** - Processa múltiplas subestações paralelo
3. **GPU Acceleration** - YOLOv8 rodando em GPU
4. **Memory Efficiency** - Libera memória após processar
5. **Lazy Loading** - Carrega imagens sob demanda
6. **Index Optimization** - Índices de banco de dados estratégicos

---

## 🔍 Monitoramento & Logging

```
Pipeline Execution Log:
├─ [INFO] Iniciando segmentação para sub_001
├─ [DEBUG] Baixando imagem (15 MB)...
├─ [DEBUG] YOLOv8 detectou 42 telhados em 12.3s
├─ [DEBUG] Segmentando 42 telhados...
├─ [DEBUG] OpenCV processou em 5.2s
├─ [DEBUG] Extraindo 40 ROIs (85 MB)...
├─ [DEBUG] Salvas em /data/rois/ em 3.1s
├─ [INFO] ✓ Segmentação concluída em 40.6s
│   - Telhados detectados: 42
│   - Telhados segmentados: 40
│   - Taxa de sucesso: 95.2%
└─ [DEBUG] Resultado armazenado em banco de dados
```

---

Este diagrama é atualizado conforme a arquitetura evolui.  
**Última atualização:** 29/01/2025
