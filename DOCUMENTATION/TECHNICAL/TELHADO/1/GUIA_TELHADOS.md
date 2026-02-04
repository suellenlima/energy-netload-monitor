# 🏠 Guia Completo - Segmentação de Telhados

## 📋 Visão Geral

Este sistema integra **Sentinel-2** (imagens de satélite) com **YOLOv8** (detecção) e **OpenCV** (segmentação) para detectar e analisar telhados/edifícios.

### Fluxo Completo:
```
1. Sentinel-2 → Download de imagem
2. YOLOv8 → Detecção de edifícios
3. OpenCV → Segmentação de telhados
4. Análise → Extração de métricas
5. Detecção de Painéis Solares (opcional)
```

---

## 🚀 Quick Start

### 1️⃣ **Iniciar o Backend**
```bash
cd backend
python -m uvicorn src.main:app --host 127.0.0.1 --port 8000
```

### 2️⃣ **Buscar Imagens Sentinel para uma Subestação**
```bash
# Obter imagens disponíveis da subestação ID 1
curl -X POST "http://localhost:8000/satelite/planetary-computer/1?data_inicio=2025-11-01&data_fim=2026-01-30" | jq
```

**Resposta:**
```json
{
  "subestacao_id": 1,
  "imagens_encontradas": 3,
  "imagens": [
    {
      "id": "S2C_MSIL2A_...",
      "data": "2025-12-10T14:27:21",
      "sensor": "Sentinel-2",
      "cobertura_nuvem": 22.7,
      "url": "https://sentinel2l2a01.blob.core.windows.net/..."
    }
  ]
}
```

### 3️⃣ **Segmentar Telhados da Imagem**
```bash
curl -X POST "http://localhost:8000/telhados/segmentar-subestacao" \
  -H "Content-Type: application/json" \
  -d '{
    "id_subestacao": 1,
    "url_imagem_satelite": "URL_DA_IMAGEM_SENTINEL",
    "resolucao_m_por_pixel": 10,
    "confianca_minima": 0.5,
    "salvar_rois": true,
    "diretorio_saida": "./data/processed/telhados"
  }' | jq
```

**Resposta:**
```json
{
  "id_subestacao": 1,
  "telhados_detectados": 12,
  "telhados_segmentados": 10,
  "tempo_processamento_segundos": 45.3,
  "sucesso": true,
  "telhados": [
    {
      "id_telhado": "telhado_1_001",
      "bbox": [100, 200, 350, 450],
      "confianca": 0.89,
      "area_m2": 625.0,
      "coordenadas_geograficas": {
        "latitude": -2.8928,
        "longitude": -60.0321
      }
    }
  ]
}
```

---

## 🎯 Endpoints Disponíveis

### 1. **Buscar Imagens Sentinel**
```http
POST /satelite/planetary-computer/{subestacao_id}
```
**Parâmetros:**
- `data_inicio` (YYYY-MM-DD) - Data inicial
- `data_fim` (YYYY-MM-DD) - Data final
- `raio_km` (float) - Raio de busca, default: 5
- `cobertura_nuvem_max` (int) - Nuvens máximas (0-100), default: 30

### 2. **Segmentar Telhados de Subestação**
```http
POST /telhados/segmentar-subestacao
```
**Body:**
```json
{
  "id_subestacao": 1,
  "url_imagem_satelite": "https://...",
  "resolucao_m_por_pixel": 10,
  "confianca_minima": 0.5,
  "salvar_rois": true,
  "diretorio_saida": "./data/processed/telhados"
}
```

### 3. **Listar Telhados Processados**
```http
GET /telhados/lista?id_subestacao=1&limite=50
```

### 4. **Processar Lote de Subestações**
```http
POST /telhados/processar-lote
```
**Body:**
```json
{
  "ids_subestacoes": [1, 2, 3],
  "periodo_dias": 90,
  "confianca_minima": 0.5,
  "processar_em_paralelo": true,
  "max_workers": 3
}
```

### 5. **Estatísticas de Segmentação**
```http
GET /telhados/estatisticas?periodo_dias=30
```

### 6. **Processar ROI com YOLO (Painéis Solares)**
```http
POST /telhados/processar-com-yolo
```
**Body:**
```json
{
  "id_telhado": "telhado_1_001",
  "modelo_yolo_id": "yolo_solar_panels_v1",
  "confianca_minima": 0.6
}
```

---

## 🛠️ Workflow Completo (Passo a Passo)

### **Cenário: Detectar painéis solares em subestações**

#### Passo 1: Listar Subestações Disponíveis
```bash
curl "http://localhost:8000/subestacoes/detectadas?limite=10" | jq '.[] | {id, nome, latitude, longitude}'
```

#### Passo 2: Buscar Imagens Sentinel de uma Subestação
```powershell
$subestacao_id = 1
$response = Invoke-RestMethod -Uri "http://localhost:8000/satelite/planetary-computer/$subestacao_id?data_inicio=2025-11-01&data_fim=2026-01-30" -Method POST
$imagem_url = $response.imagens[0].url
Write-Host "URL da imagem: $imagem_url"
```

#### Passo 3: Segmentar Telhados
```powershell
$body = @{
    id_subestacao = $subestacao_id
    url_imagem_satelite = $imagem_url
    resolucao_m_por_pixel = 10
    confianca_minima = 0.5
    salvar_rois = $true
    diretorio_saida = "data/processed/telhados"
} | ConvertTo-Json

$resultado = Invoke-RestMethod -Uri "http://localhost:8000/telhados/segmentar-subestacao" -Method POST -Body $body -ContentType "application/json"

Write-Host "Telhados detectados: $($resultado.telhados_detectados)"
Write-Host "Telhados segmentados: $($resultado.telhados_segmentados)"
```

#### Passo 4: Analisar Painéis Solares (se houver modelo YOLO)
```powershell
foreach ($telhado in $resultado.telhados) {
    $body = @{
        id_telhado = $telhado.id_telhado
        modelo_yolo_id = "yolo_solar_panels_v1"
        confianca_minima = 0.6
    } | ConvertTo-Json
    
    $paineis = Invoke-RestMethod -Uri "http://localhost:8000/telhados/processar-com-yolo" -Method POST -Body $body -ContentType "application/json"
    
    Write-Host "Telhado $($telhado.id_telhado): $($paineis.numero_paineis_detectados) painéis solares"
}
```

#### Passo 5: Ver Estatísticas Gerais
```bash
curl "http://localhost:8000/telhados/estatisticas?periodo_dias=30" | jq
```

---

## 🔧 Configuração de Modelos YOLO

### Registrar Modelo de Painéis Solares
```bash
curl -X POST "http://localhost:8000/telhados/registrar-modelo-yolo" \
  -H "Content-Type: application/json" \
  -d '{
    "modelo_id": "yolo_solar_panels_v1",
    "nome_modelo": "YOLOv8 Painéis Solares",
    "descricao": "Detecção de painéis solares em telhados",
    "caminho_arquivo": "./backend/yolov8n-seg.pt",
    "tipo_deteccao": "paineis_solares",
    "versao": "1.0",
    "metricas": {
      "precision": 0.92,
      "recall": 0.87,
      "mAP50": 0.89
    }
  }'
```

---

## 📊 Exemplos de Resposta

### Exemplo 1: Segmentação Bem-Sucedida
```json
{
  "id_subestacao": 1,
  "id_imagem_satelite": "sat_20260130_153000",
  "timestamp_processamento": "2026-01-30T15:30:45",
  "telhados_detectados": 12,
  "telhados_segmentados": 10,
  "tempo_processamento_segundos": 45.3,
  "sucesso": true,
  "telhados": [
    {
      "id_telhado": "telhado_1_001",
      "bbox": [100, 200, 350, 450],
      "confianca": 0.89,
      "area_m2": 625.0,
      "area_pixels": 6250,
      "tipo_edificio": "residencial",
      "roi_path": "./data/processed/telhados/telhado_1_001.jpg",
      "coordenadas_geograficas": {
        "latitude": -2.8928,
        "longitude": -60.0321
      },
      "indice_qualidade": 0.85
    }
  ],
  "erros": [],
  "avisos": ["2 telhados não puderam ser segmentados (área muito pequena)"]
}
```

### Exemplo 2: Estatísticas Gerais
```json
{
  "periodo": {
    "inicio": "2026-01-01T00:00:00",
    "fim": "2026-01-30T23:59:59"
  },
  "total_subestacoes_processadas": 45,
  "total_telhados_detectados": 542,
  "total_telhados_segmentados": 489,
  "total_imagens_processadas": 48,
  "media_telhados_por_subestacao": 12.04,
  "media_confianca_deteccao": 0.87,
  "media_area_telhado_m2": 487.5,
  "distribuicao_tipo_edificio": {
    "residencial": 320,
    "comercial": 98,
    "industrial": 71
  },
  "tempo_medio_processamento_segundos": 42.5,
  "taxa_sucesso_percentual": 90.2
}
```

---

## 🐛 Troubleshooting

### Erro: "Modelo YOLO não encontrado"
```bash
# Verificar se o arquivo existe
ls backend/yolov8n-seg.pt

# Se não existir, baixar:
cd backend
wget https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n-seg.pt
```

### Erro: "URL de imagem inválida"
- Verifique se a URL do Sentinel-2 foi obtida corretamente
- URLs do Planetary Computer expiram após algumas horas
- Execute novamente o endpoint `/satelite/planetary-computer/{id}` para obter URL atualizada

### Erro: "Sem telhados detectados"
- Aumente a área de busca (`raio_km`)
- Reduza `confianca_minima` para 0.3
- Verifique se a imagem tem boa resolução e pouca cobertura de nuvens

---

## 📈 Performance e Otimização

### Processamento Paralelo
```json
{
  "ids_subestacoes": [1, 2, 3, 4, 5],
  "processar_em_paralelo": true,
  "max_workers": 3
}
```
- **max_workers**: Número de threads simultâneas (padrão: 3)
- **Tempo estimado**: ~30-60s por subestação
- **GPU recomendada**: RTX 3060 ou superior

### Configurações Recomendadas
```json
{
  "resolucao_m_por_pixel": 10,     // Sentinel-2 padrão
  "confianca_minima": 0.5,         // Equilíbrio entre precisão e recall
  "cobertura_nuvem_max": 30,       // Máximo de nuvens aceitável
  "raio_km": 5                      // Área ao redor da subestação
}
```

---

## 🎓 Casos de Uso

### 1. **Mapeamento de Potencial Solar**
- Detectar telhados em área de distribuidora
- Estimar área disponível para painéis
- Calcular potencial de geração distribuída

### 2. **Monitoramento de Infraestrutura**
- Acompanhar mudanças em edificações
- Detectar novos empreendimentos
- Atualizar mapas de carga

### 3. **Análise de Carga Potencial**
- Correlacionar área construída com demanda
- Identificar áreas de expansão
- Planejar reforços na rede

---

## 📚 Próximos Passos

1. ✅ Autenticação Sentinel funcionando
2. ⏳ **Testar segmentação de telhados**
3. ⏳ Treinar modelo YOLO para painéis solares
4. ⏳ Integrar com banco de dados
5. ⏳ Dashboard de visualização

**Você está na etapa 2!** 🎯
