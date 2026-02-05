# 🛰️ Planetary Computer Integration - COMPLETO

## ✅ O que foi implementado

### 1. **Autenticação via .env** 
```bash
# backend/.env
USE_PLANETARY_COMPUTER=true
AZURE_SAS_TOKEN=
```

### 2. **Métodos de autenticação em `telhado_segmentation_service.py`**
- `_get_azure_headers()` - Headers com autenticação Azure
- `_build_authenticated_url()` - URLs com SAS token
- `download_imagem_satelite()` - Download com autenticação automática

### 3. **Novo Endpoint REST**
```
POST /satelite/planetary-computer/{subestacao_id}
```

**Parâmetros:**
- `data_inicio` (YYYY-MM-DD) - Data inicial, default: 90 dias atrás
- `data_fim` (YYYY-MM-DD) - Data final, default: hoje
- `raio_km` (float) - Raio de busca em km, default: 5
- `cobertura_nuvem_max` (int) - Nuvens máximas (0-100), default: 30

**Resposta:**
```json
{
  "subestacao_id": 1,
  "subestacao": {
    "id": 1,
    "nome": "SE_DETECTADA_0",
    "latitude": -19.925,
    "longitude": -43.938,
    "distribuidora": "CEMIG DISTRIBUICAO S.A"
  },
  "imagens_encontradas": 5,
  "imagens": [
    {
      "id": "S2C_MSIL2A_20251210T142721_R053_T20MRB_20251210T181015",
      "data": "2025-12-10T14:27:21.025000+00:00",
      "sensor": "Sentinel-2",
      "cobertura_nuvem": 22.716047,
      "url": "https://sentinel2l2a01.blob.core.windows.net/...",
      "bbox": [-60.3021545, -3.7034039, -59.9172308, -2.7101294]
    }
  ],
  "periodo": {
    "data_inicio": "2025-11-01",
    "data_fim": "2026-01-30"
  },
  "nota": "URLs já possuem assinatura automática do Planetary Computer"
}
```

### 4. **Script CLI para testes**
```bash
# Consultar Manaus
python scripts/fetch_sentinel_data.py -3.1190 -60.0217 5

# Resultado: 3 imagens Sentinel-2 encontradas
# Salvo em: data/sentinel2_results.json
```

### 5. **Dependências adicionadas ao requirements.txt**
```
pystac-client==0.9.0          # STAC API client
planetary-computer==1.0.0     # Autenticação automática
opencv-python==4.8.1.78       # Processamento de imagens
ultralytics==8.4.9            # YOLOv8
torch==2.10.0                 # Deep learning
torchvision==0.25.0           # Vision models
pillow==12.1.0                # Image manipulation
python-dotenv==1.2.1          # Variáveis de ambiente
```

## 🚀 Como usar

### 1. **Via API REST (Recomendado)**
```bash
# Terminal PowerShell
$response = Invoke-RestMethod `
  -Uri "http://localhost:8000/satelite/planetary-computer/1?data_inicio=2025-11-01&data_fim=2026-01-30&raio_km=5" `
  -Method POST

$response.imagens | ForEach-Object { 
  Write-Host "$($_.id)" 
  Write-Host "  Data: $($_.data)"
  Write-Host "  Nuvens: $($_.cobertura_nuvem)%"
  Write-Host ""
}
```

### 2. **Via Script Python**
```bash
cd c:\Hackathon\Git\energy-netload-monitor
python scripts/fetch_sentinel_data.py -3.1190 -60.0217 5
```

### 3. **Swagger UI**
- Acesse: http://localhost:8000/docs
- Procure por: `/satelite/planetary-computer/{subestacao_id}`
- Clique em "Try it out"

## 📊 Fluxo Completo

```
1. Consultar STAC (Planetary Computer)
   ↓
2. Obter URLs Sentinel-2 com assinatura automática
   ↓
3. Registrar imagens no banco de dados
   ↓
4. Download com autenticação automática
   ↓
5. YOLOv8: Detectar edifícios
   ↓
6. OpenCV: Segmentar telhados
   ↓
7. Extrair ROIs por telhado
   ↓
8. Processar com modelos YOLO customizados
```

## 🔑 Características

✅ **Autenticação automática** - Planetary Computer assina URLs automaticamente  
✅ **Sem SAS Token necessário** - URLs públicas com acesso garantido  
✅ **Versionamento de dependências** - Todas as versões fixadas (numpy/pandas compatíveis)  
✅ **Tratamento de erros** - Logging detalhado de erros  
✅ **Raio de busca configurável** - 0.5 a 50 km  
✅ **Filtro de nuvens** - 0 a 100%  
✅ **Múltiplas imagens** - Até 20 imagens por consulta  

## 🧪 Teste Agora

```bash
# 1. Abrir Swagger
http://localhost:8000/docs

# 2. Ou fazer POST direto
curl -X POST "http://localhost:8000/satelite/planetary-computer/1?data_inicio=2025-11-01&data_fim=2026-01-30&raio_km=5&cobertura_nuvem_max=30"

# 3. Resultado: URLs assinadas prontas para download
```

## 📝 Próximos Passos

1. **Registrar imagens no banco:**
   ```bash
   POST /satelite/subestacao/{id}/registrar-imagem
   ```

2. **Segmentar telhados:**
   ```bash
   POST /telhados/segmentar-subestacao
   ```

3. **Processar com YOLO:**
   ```bash
   POST /telhados/processar-com-yolo
   ```

## 📚 Referências

- [Planetary Computer API](https://planetarycomputer.microsoft.com/)
- [STAC Specification](https://stacspec.org/)
- [Sentinel-2 Data](https://sentinel.esa.int/web/sentinel/missions/sentinel-2)
- [PySTAC Client](https://pystac-client.readthedocs.io/)
