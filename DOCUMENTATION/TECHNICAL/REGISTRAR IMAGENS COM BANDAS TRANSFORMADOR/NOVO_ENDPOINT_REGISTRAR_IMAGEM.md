# 📝 Novo Endpoint: Registrar Imagem CBERS-4A com 5 Bandas

**Data:** 31 de Janeiro de 2026  
**Status:** ✅ **Implementado**

---

## 🎯 O Problema

Antes:
- ❌ Endpoint POST `/satelite/subestacao/{id}/registrar-imagem` salva apenas 1 URL
- ❌ Não existia versão para transformador
- ❌ Era necessário fazer múltiplas chamadas para registrar todas as bandas

---

## ✅ A Solução

Novo endpoint POST que salva a imagem **com as 5 bandas de uma vez**:

```
POST /satelite/v2/transformador/{transformador_id}/registrar-imagem
```

---

## 📋 Especificação Completa

### Request Body

```json
{
  "transformador_id": 47,
  "subestacao_id": 5,
  "imagem_id": "CBERS_4A_WPM_20260131_225_117_L4",
  "data_aquisicao": "2026-01-31T13:12:41",
  "sensor": "CBERS-4A",
  "resolucao_m": 2.0,
  "cobertura_nuvem_pct": 15.5,
  "urls_bandas": {
    "blue": "https://data.inpe.br/.../BAND0.tif",
    "green": "https://data.inpe.br/.../BAND1.tif",
    "red": "https://data.inpe.br/.../BAND2.tif",
    "nir": "https://data.inpe.br/.../BAND3.tif",
    "swir": "https://data.inpe.br/.../BAND4.tif"
  }
}
```

### Response (Sucesso)

```json
{
  "sucesso": true,
  "imagem_id": 123,
  "bandas_registradas": 5,
  "mensagem": "Imagem registrada com sucesso (5 bandas)"
}
```

### Response (Erro)

```json
{
  "sucesso": false,
  "imagem_id": null,
  "bandas_registradas": 0,
  "mensagem": "Falha ao registrar metadados da imagem"
}
```

---

## 🔄 Fluxo de Dados

```
POST /satelite/v2/transformador/{id}/registrar-imagem
    ↓
1. Valida transformador_id
    ↓
2. Registra metadados em satelite_imagens
    ├─ imagem_id_inpe
    ├─ data_imagem
    ├─ cobertura_nuvem
    ├─ resolucao_m
    ├─ sensor
    └─ URL primeira banda (blue)
    ↓
3. Retorna imagem_id do banco
    ↓
4. Registra cada banda em satelite_bandas
    ├─ BLUE (numero=0)
    ├─ GREEN (numero=1)
    ├─ RED (numero=2)
    ├─ NIR (numero=3)
    └─ SWIR (numero=4)
    ↓
5. Retorna número de bandas registradas
    ↓
Response {sucesso, imagem_id, bandas_registradas}
```

---

## 💻 Exemplo de Uso

### Python

```python
import requests

url = "http://127.0.0.1:8000/satelite/v2/transformador/47/registrar-imagem"

payload = {
    "transformador_id": 47,
    "subestacao_id": 5,
    "imagem_id": "CBERS_4A_WPM_20260131_225_117_L4",
    "data_aquisicao": "2026-01-31T13:12:41",
    "sensor": "CBERS-4A",
    "resolucao_m": 2.0,
    "cobertura_nuvem_pct": 15.5,
    "urls_bandas": {
        "blue": "https://...",
        "green": "https://...",
        "red": "https://...",
        "nir": "https://...",
        "swir": "https://..."
    }
}

response = requests.post(url, json=payload)
print(response.json())
# {
#   "sucesso": true,
#   "imagem_id": 123,
#   "bandas_registradas": 5
# }
```

### CURL

```bash
curl -X POST http://127.0.0.1:8000/satelite/v2/transformador/47/registrar-imagem \
  -H "Content-Type: application/json" \
  -d '{
    "transformador_id": 47,
    "subestacao_id": 5,
    "imagem_id": "CBERS_4A_WPM_20260131_225_117_L4",
    "data_aquisicao": "2026-01-31T13:12:41",
    "sensor": "CBERS-4A",
    "resolucao_m": 2.0,
    "cobertura_nuvem_pct": 15.5,
    "urls_bandas": {
      "blue": "https://...",
      "green": "https://...",
      "red": "https://...",
      "nir": "https://...",
      "swir": "https://..."
    }
  }'
```

### PowerShell

```powershell
$url = "http://127.0.0.1:8000/satelite/v2/transformador/47/registrar-imagem"

$body = @{
    transformador_id = 47
    subestacao_id = 5
    imagem_id = "CBERS_4A_WPM_20260131_225_117_L4"
    data_aquisicao = "2026-01-31T13:12:41"
    sensor = "CBERS-4A"
    resolucao_m = 2.0
    cobertura_nuvem_pct = 15.5
    urls_bandas = @{
        blue = "https://..."
        green = "https://..."
        red = "https://..."
        nir = "https://..."
        swir = "https://..."
    }
} | ConvertTo-Json

Invoke-WebRequest -Uri $url -Method POST -ContentType "application/json" -Body $body
```

---

## 📊 O Que É Salvo no Banco

### Tabela: `satelite_imagens`
```
id: 123
imagem_id_inpe: CBERS_4A_WPM_20260131_225_117_L4
data_imagem: 2026-01-31 13:12:41
cobertura_nuvem: 15.5
resolucao_m: 2.0
sensor: CBERS-4A
url: https://...(primeira banda - blue)
subestacao_id: 5
```

### Tabela: `satelite_bandas` (5 linhas)
```
id  imagem_id  numero_banda  nome_banda  url
1   123        0             BLUE        https://...BAND0.tif
2   123        1             GREEN       https://...BAND1.tif
3   123        2             RED         https://...BAND2.tif
4   123        3             NIR         https://...BAND3.tif
5   123        4             SWIR        https://...BAND4.tif
```

---

## 🔗 Relacionamento com Outros Endpoints

### Fluxo Completo de Uso

```
1. GET /satelite/v2/transformador/{id}/imagens
   └─ Busca imagens disponíveis no CBERS-4A

2. POST /satelite/v2/transformador/{id}/registrar-imagem
   └─ Salva as bandas no banco ✨ NOVO

3. GET /satelite/v2/transformador/{id}/bandas (futuro)
   └─ Lista bandas salvas

4. POST /telhados/transformador/detectar-telhados
   └─ Processa com ImagemMultibandaLoader
```

---

## ⚙️ Implementação

### Arquivo Modificado
- `backend/src/api/satelite_v2.py`
  - Schemas: `RegistrarImagemTransformadorRequest`, `RegistrarImagemTransformadorResponse`
  - Endpoint: `@router.post("/transformador/{transformador_id}/registrar-imagem")`

### Métodos Utilizados
- `INPESatelliteService.armazenar_metadata_subestacao()`
- `INPESatelliteService.registrar_banda()`

### Dependências
- SQLAlchemy (ORM)
- Pydantic (validação)
- FastAPI (routing)

---

## 🧪 Teste Prático

### Script de Teste
```bash
python exemplo_registrar_imagem_transformador.py
```

### Output Esperado
```
================================================================================
EXEMPLO 1: Registrar uma imagem com 5 bandas
================================================================================

Endpoint: POST http://127.0.0.1:8000/satelite/v2/transformador/47/registrar-imagem

Payload:
{
  "transformador_id": 47,
  "subestacao_id": 5,
  ...
}

📤 Enviando requisição...
Status: 200

✅ Sucesso!
{
  "sucesso": true,
  "imagem_id": 123,
  "bandas_registradas": 5,
  "mensagem": "Imagem registrada com sucesso (5 bandas)"
}
```

---

## 📈 Ganhos

| Aspecto | Antes | Depois |
|---------|-------|--------|
| URLs Salvas por Imagem | 1 | 5 |
| Chamadas de API | 6+ | 1 ✨ |
| Completude de Dados | Parcial | Total |
| Pronto para YOLO | Não | Sim ✅ |

---

## 🔒 Validações

✅ Validação de `transformador_id`  
✅ Validação de `subestacao_id`  
✅ Validação de bandas (blue, green, red, nir, swir)  
✅ Validação de URLs (HTTPS)  
✅ Error handling completo  
✅ Logging detalhado  

---

## 🚀 Próximos Passos

1. Testar com URLs reais do CBERS-4A
2. Criar endpoint GET para listar bandas salvas
3. Integrar automaticamente com detecção YOLO
4. Adicionar pré-processamento em background

---

## 📞 Dúvidas Comuns

**P: E se uma banda não estiver disponível?**  
R: Registra apenas as bandas disponíveis, retorna `bandas_registradas < 5`.

**P: Posso atualizar uma imagem já registrada?**  
R: Não, use DELETE e POST novo. Futura feature: PUT para atualizar.

**P: Qual o tamanho máximo de requisição?**  
R: FastAPI padrão: 25MB, suficiente para URLs (~1KB cada).

**P: Preciso registrar todas as 5 bandas?**  
R: Não, 3+ bandas (RGB) já permitem YOLO. NIR/SWIR são opcionais para NDVI.

---

**Status:** ✅ **PRONTO PARA USAR**

*Novo endpoint implementado: 31 de Janeiro de 2026*
