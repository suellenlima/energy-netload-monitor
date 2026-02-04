# ✅ COMO FAZER OS OUTROS ENDPOINTS FUNCIONAREM E SEGMENTAR TELHADOS

## 🎯 Status Atual

### ✅ **O que já está funcionando:**
1. ✅ Autenticação com Sentinel-2 (Planetary Computer)
2. ✅ Busca de imagens de satélite por localização
3. ✅ API Backend rodando em http://localhost:8000
4. ✅ Modelo YOLOv8 disponível
5. ✅ Bibliotecas necessárias instaladas

### 📋 **O que você pode fazer AGORA:**

---

## 🚀 Opção 1: Teste Rápido (Recomendado)

### **Passo 1: Execute o script de verificação**
```bash
python verificar_componentes_telhados.py
```

**Resultado esperado:** ✅ TODOS OS COMPONENTES ESTÃO DISPONÍVEIS!

### **Passo 2: Execute o workflow completo**
```bash
python test_workflow_telhados.py
```

**O que acontece:**
1. Busca imagens Sentinel-2 da subestação
2. Faz download da melhor imagem (menor cobertura de nuvens)
3. Detecta edifícios com YOLOv8
4. Segmenta telhados com OpenCV
5. Calcula métricas (área, coordenadas, etc)
6. Salva ROIs em `data/processed/telhados/`

**⏱️ Tempo estimado:** 2-5 minutos

---

## 🎯 Opção 2: Usar API Diretamente (Swagger UI)

### **Passo 1: Abrir documentação interativa**
```
http://localhost:8000/docs
```

### **Passo 2: Buscar imagem Sentinel**
1. Vá para: **Satélite** > `POST /satelite/planetary-computer/{subestacao_id}`
2. Clique em **"Try it out"**
3. Configure:
   - `subestacao_id`: 1
   - `data_inicio`: 2025-11-01
   - `data_fim`: 2026-01-30
4. Clique em **"Execute"**
5. **Copie a URL** da primeira imagem da resposta

### **Passo 3: Segmentar telhados**
1. Vá para: **Telhados - Segmentação** > `POST /telhados/segmentar-subestacao`
2. Clique em **"Try it out"**
3. Cole este JSON (substitua a URL):
```json
{
  "id_subestacao": 1,
  "url_imagem_satelite": "COLE_A_URL_AQUI",
  "resolucao_m_por_pixel": 10,
  "confianca_minima": 0.5,
  "salvar_rois": true,
  "diretorio_saida": "data/processed/telhados"
}
```
4. Clique em **"Execute"**
5. **Aguarde 2-5 minutos** (processamento pesado)

---

## ⚠️ **LIMITAÇÃO IMPORTANTE DO SENTINEL-2**

### 🔴 **Problema: Imagens Sentinel-2 estão muito distantes!**

**Resolução do Sentinel-2: 10 metros por pixel**
- Um telhado de casa (10x10m) = **apenas 1 pixel!** ❌
- **NÃO é possível** ver telhados individuais claramente
- **NÃO adequado** para detecção de painéis solares residenciais

### ✅ **Solução: Use imagens de ALTA RESOLUÇÃO**

| Fonte | Resolução | Pixels em telhado 10x10m | Adequado? | Custo |
|-------|-----------|--------------------------|-----------|-------|
| **Sentinel-2** | 10m/pixel | 1 pixel | ❌ NÃO | Grátis |
| **CBERS-4A (INPE)** 🇧🇷 | **2m/pixel** | **25 pixels** | ✅ **SIM** | **Grátis** |
| **Google Maps** | 0.15-0.6m/pixel | 281-4498 pixels | ✅ **ÓTIMO** | 25k/mês |
| **Bing Maps** | 0.3-1m/pixel | 100-1000 pixels | ✅ SIM | Limitado |
| **Maxar** | 0.3m/pixel | 1024 pixels | ✅ SIM | Caro |

### 🚀 **Soluções Recomendadas**

#### **Opção 1: Google Maps (MELHOR)** 🥇
**1. Obtenha API Key:**
- Acesse: https://console.cloud.google.com/
- Crie projeto e ative "Maps Static API"
- Gere API Key

**2. Configure no .env:**
```bash
# backend/.env
GOOGLE_MAPS_API_KEY=sua_chave_aqui
```

**3. Teste:**
```bash
python test_google_maps_resolucao.py
```

**💰 Custo:** Grátis até 25.000 imagens/mês, depois $0.002 por imagem

---

#### **Opção 2: CBERS-4A do INPE (Alternativa Brasileira)** 🇧🇷 🥈

**Vantagens:**
- ✅ Resolução de **2 metros** (muito melhor que Sentinel-2!)
- ✅ **Totalmente gratuito** sem limites
- ✅ Dados brasileiros (INPE)
- ✅ Bom para grandes telhados industriais/comerciais

**Limitações:**
- ⚠️ Não detecta painéis solares pequenos (precisa >2m)
- ⚠️ Revisita de 31 dias (menos frequente)

**Como usar:**
```bash
# 1. Testar acesso
python test_inpe_cbers4a.py

# 2. Acessar catálogo
# http://www2.dgi.inpe.br/catalogo/
```

**📚 Guias completos:** 
- [IMAGENS_INPE.md](IMAGENS_INPE.md) - CBERS-4A do INPE
- [LIMITACOES_SENTINEL2.md](LIMITACOES_SENTINEL2.md) - Comparação completa

---

### **Resultado esperado:**
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
      "confianca": 0.89,
      "area_m2": 625.0,
      "tipo_edificio": "residencial",
      "coordenadas_geograficas": {
        "latitude": -2.8928,
        "longitude": -60.0321
      }
    }
  ]
}
```

---

## 🔧 Opção 3: Via PowerShell (Windows)

### **Passo 1: Executar teste automático**
```powershell
.\teste_telhados.ps1
```

### **Passo 2: Executar manualmente**
```powershell
# 1. Buscar imagem
$sentinel = Invoke-RestMethod -Uri "http://localhost:8000/satelite/planetary-computer/1?data_inicio=2025-11-01&data_fim=2026-01-30" -Method POST

$url = $sentinel.imagens[0].url

# 2. Segmentar telhados
$body = @{
    id_subestacao = 1
    url_imagem_satelite = $url
    resolucao_m_por_pixel = 10
    confianca_minima = 0.5
    salvar_rois = $true
    diretorio_saida = "data/processed/telhados"
} | ConvertTo-Json

$resultado = Invoke-RestMethod -Uri "http://localhost:8000/telhados/segmentar-subestacao" -Method POST -Body $body -ContentType "application/json"

# 3. Ver resultados
$resultado | ConvertTo-Json -Depth 3
Write-Host "Telhados detectados: $($resultado.telhados_detectados)"
```

---

## 📊 Outros Endpoints Disponíveis

### **1. Listar telhados processados**
```bash
curl "http://localhost:8000/telhados/lista?id_subestacao=1&limite=10"
```

### **2. Processar múltiplas subestações**
```bash
curl -X POST "http://localhost:8000/telhados/processar-lote" \
  -H "Content-Type: application/json" \
  -d '{
    "ids_subestacoes": [1, 2, 3],
    "periodo_dias": 90,
    "processar_em_paralelo": true,
    "max_workers": 3
  }'
```

### **3. Ver estatísticas gerais**
```bash
curl "http://localhost:8000/telhados/estatisticas?periodo_dias=30"
```

### **4. Detalhes de uma subestação**
```bash
curl "http://localhost:8000/telhados/subestacao/1"
```

### **5. Detectar painéis solares (se modelo YOLO estiver registrado)**
```bash
curl -X POST "http://localhost:8000/telhados/processar-com-yolo" \
  -H "Content-Type: application/json" \
  -d '{
    "id_telhado": "telhado_1_001",
    "modelo_yolo_id": "yolo_solar_panels_v1",
    "confianca_minima": 0.6
  }'
```

---

## 🎓 Pipeline Completo Explicado

### **Arquitetura do Sistema:**
```
┌─────────────────┐
│  Planetary      │  1. Buscar imagens
│  Computer       │     (Sentinel-2)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Download       │  2. Baixar imagem
│  Imagem         │     GeoTIFF
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  YOLOv8         │  3. Detectar edifícios
│  Detecção       │     (bounding boxes)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  OpenCV         │  4. Segmentar telhados
│  Segmentação    │     (contornos precisos)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Análise        │  5. Calcular métricas
│  Geográfica     │     (área, coordenadas)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Banco de       │  6. Armazenar resultados
│  Dados          │
└─────────────────┘
```

### **Fluxo de Dados:**
1. **Input**: ID da subestação + período
2. **Saída**: Lista de telhados com:
   - Coordenadas geográficas
   - Área em m²
   - Imagem ROI do telhado
   - Confiança da detecção
   - Tipo de edificação

---

## 🐛 Solução de Problemas

### **Erro: "Servidor não está rodando"**
```bash
cd backend
python -m uvicorn src.main:app --host 127.0.0.1 --port 8000
```

### **Erro: "Modelo YOLO não encontrado"**
```bash
cd backend
# Windows PowerShell:
Invoke-WebRequest -Uri "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n-seg.pt" -OutFile "yolov8n-seg.pt"

# Linux/Mac:
wget https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n-seg.pt
```

### **Erro: "Nenhuma imagem Sentinel encontrada"**
- Aumente o período de busca (ex: 180 dias)
- Aumente o raio de busca (ex: 10 km)
- Aumente a tolerância de nuvens (ex: 50%)

### **Erro: "Timeout ao processar"**
- A imagem é muito grande
- Use GPU se disponível
- Reduza a resolução (ex: 20m por pixel)
- Processe em lote menor

---

## 📁 Estrutura de Arquivos Gerados

```
data/
└── processed/
    └── telhados/
        ├── telhado_1_001.jpg          # ROI do telhado
        ├── telhado_1_002.jpg
        ├── telhado_1_003.jpg
        └── ...
```

---

## 🎯 Casos de Uso

### **1. Identificar potencial solar**
```python
# Buscar todas as subestações
# Para cada subestação:
#   - Buscar imagens Sentinel
#   - Segmentar telhados
#   - Calcular área total
#   - Estimar potencial de painéis solares
```

### **2. Monitorar expansão urbana**
```python
# Comparar imagens de períodos diferentes
# Detectar novos edifícios
# Atualizar mapa de carga
```

### **3. Planejar reforços na rede**
```python
# Correlacionar área construída com demanda
# Identificar áreas de crescimento
# Priorizar investimentos
```

---

## 📚 Documentação Completa

### **Arquivos de referência:**
1. `GUIA_TELHADOS.md` - Guia completo de uso
2. `test_workflow_telhados.py` - Script de teste end-to-end
3. `verificar_componentes_telhados.py` - Verificação de dependências
4. `teste_telhados.ps1` - Script PowerShell de teste
5. `http://localhost:8000/docs` - API Swagger UI

---

## ✅ Checklist para Começar

- [x] ✅ Servidor backend rodando
- [x] ✅ Autenticação Sentinel OK
- [x] ✅ Modelo YOLOv8 disponível
- [x] ✅ Bibliotecas instaladas
- [ ] ⏳ **Executar primeiro teste**: `python test_workflow_telhados.py`
- [ ] ⏳ Explorar outros endpoints
- [ ] ⏳ Processar múltiplas subestações
- [ ] ⏳ Integrar com banco de dados
- [ ] ⏳ Criar dashboard de visualização

---

## 🎉 Pronto para Usar!

**Você está na etapa:** Todos os componentes prontos ✅

**Próximo passo recomendado:**
```bash
python test_workflow_telhados.py
```

**Tempo estimado:** 2-5 minutos para primeira execução

**Resultado esperado:** 
- Telhados detectados e segmentados
- Imagens ROI salvas
- Métricas calculadas
- Coordenadas geográficas

---

## 💡 Dicas

1. **GPU vs CPU:** Se tiver GPU NVIDIA, o processamento será 5-10x mais rápido
2. **Cache:** Imagens Sentinel são grandes (~50-100MB), use cache local
3. **Lote:** Processe múltiplas subestações em paralelo para eficiência
4. **Qualidade:** Confiança mínima 0.5 é bom equilíbrio entre precisão e recall
5. **Nuvens:** Prefira imagens com <30% de cobertura de nuvens

---

**🚀 Comece agora:** `python test_workflow_telhados.py`
