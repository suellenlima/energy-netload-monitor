# Configuração de Autenticação Azure Sentinel-2

## Visão Geral

O sistema foi configurado para suportar autenticação com imagens Sentinel-2 do Azure Storage via variáveis de ambiente (`.env`).

## Opções de Autenticação

### 1. **SAS Token (Shared Access Signature)** - RECOMENDADO

A forma mais simples e segura de autenticar no Azure Storage.

#### Como obter um SAS Token:

1. Acesse [Azure Portal](https://portal.azure.com)
2. Vá para **Storage Accounts** → `sentinel2l2a01`
3. Na barra lateral, clique em **Shared access signature**
4. Configure:
   - **Allowed services**: Blob
   - **Allowed resource types**: Object
   - **Allowed permissions**: Read
   - **Expiry**: Data desejada (ex: 1 ano)
5. Clique em **Generate SAS and connection string**
6. Copie a **SAS token** (começa com `sv=...`)

#### Configurar no .env:

```bash
AZURE_SAS_TOKEN=sv=2023-11-09&ss=b&srt=o&sp=r&se=2025-12-31T23:59:59Z&st=2025-01-01T00:00:00Z&spr=https&sig=XXXXX...
```

### 2. **Connection String**

Alternativa usando chave de armazenamento:

```bash
AZURE_STORAGE_KEY=DefaultEndpointsProtocol=https;AccountName=sentinel2l2a01;AccountKey=XXXXX...
```

### 3. **Planetary Computer (Sem Autenticação)**

Para testar sem configurar Azure, use URLs públicas do Planetary Computer:

```bash
USE_PLANETARY_COMPUTER=true
```

## Arquivo .env Configurado

```bash
DATABASE_URL=postgresql://admin:admin123@127.0.0.1:5432/energy_monitor

# Azure Sentinel-2 Credentials
AZURE_STORAGE_ACCOUNT=sentinel2l2a01
AZURE_STORAGE_CONTAINER=sentinel2-l2
AZURE_SAS_TOKEN=<seu-token-aqui>
AZURE_STORAGE_KEY=

# Planetary Computer (alternativa sem autenticação)
USE_PLANETARY_COMPUTER=false
```

## Implementação no Código

### Método: `_get_azure_headers()`

Retorna headers com autenticação para requisições HTTP:

```python
def _get_azure_headers(self) -> Dict[str, str]:
    """Retorna headers com autenticação Azure para Sentinel-2"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Verificar se SAS token está configurado
    sas_token = os.getenv('AZURE_SAS_TOKEN', '').strip()
    if sas_token:
        headers['Authorization'] = f'Bearer {sas_token}'
        logger.debug("Usando autenticação Azure SAS token")
    
    return headers
```

### Método: `_build_authenticated_url()`

Constrói URL autenticada adicionando SAS token como parâmetro:

```python
def _build_authenticated_url(self, url: str) -> str:
    """Constrói URL autenticada com SAS token se necessário"""
    if 'blob.core.windows.net' in url:
        sas_token = os.getenv('AZURE_SAS_TOKEN', '').strip()
        if sas_token and '?' not in url:
            # Adicionar SAS token como query parameter
            return f"{url}?{sas_token}"
    
    return url
```

### Método: `download_imagem_satelite()` - Atualizado

```python
# Construir headers com autenticação Azure
headers = self._get_azure_headers()

# Construir URL autenticada (com SAS token se houver)
url_autenticada = self._build_authenticated_url(url_imagem)

# Baixar de URL
logger.info(f"Baixando imagem com autenticação: {url_imagem[:80]}...")
response = requests.get(url_autenticada, timeout=timeout, headers=headers)
response.raise_for_status()
imagem = Image.open(BytesIO(response.content))
```

## Testando Autenticação

### 1. Verificar variáveis de ambiente:

```bash
# No backend
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('AZURE_SAS_TOKEN')[:20])"
```

### 2. Testar endpoint com Sentinel-2:

```bash
# Consular imagens disponíveis
curl -X POST "http://localhost:8000/satelite/subestacao/1/consultar-e-registrar?data_inicio=2025-01-01&data_fim=2025-12-31"

# Segmentar com Sentinel-2 registrada
curl -X POST "http://localhost:8000/telhados/segmentar-subestacao" \
  -H "Content-Type: application/json" \
  -d '{
    "id_subestacao": 1,
    "confianca_minima": 0.5,
    "limpar_anterior": true,
    "extrair_rois": true
  }'
```

### 3. Verificar logs:

```bash
# Backend deve mostrar
# INFO | src.services.telhado_segmentation_service:XXX | Baixando imagem com autenticação: https://sentinel2l2a01...
```

## Fluxo Completo

1. **Query STAC** → Find Sentinel-2 images
2. **Register Images** → Store URLs in database
3. **Download** → Use AZURE_SAS_TOKEN from .env
4. **Detect & Segment** → YOLO + OpenCV processing
5. **Extract ROIs** → Individual roof images

## Segurança

⚠️ **IMPORTANTE:**
- Nunca commitar `.env` com tokens reais
- SAS Tokens expiram - renovar periodicamente
- Usar apenas permissão `Read` (sp=r) para segurança
- Se token for comprometido, regenerar no Azure Portal

## Troubleshooting

### Erro: "409 Client Error: Public access is not permitted"

**Causa**: SAS token não configurado ou inválido

**Solução**:
1. Verificar se `AZURE_SAS_TOKEN` está preenchido no `.env`
2. Regenerar token no Azure Portal
3. Verificar data de expiração do token

### Erro: "Authentication failed"

**Causa**: Token expirado ou com permissões insuficientes

**Solução**:
1. Regenerar token com data de expiração futura
2. Verificar se tem permissão `Read`

### Erro: "Connection timeout"

**Causa**: Rede ou token muito longo

**Solução**:
1. Usar Planetary Computer URLs (não exigem token)
2. Usar método de Storage Key em vez de SAS Token

## Próximos Passos

1. Obter SAS Token do Azure Portal
2. Adicionar ao `.env`
3. Testar com: `POST /satelite/subestacao/1/consultar-e-registrar`
4. Executar segmentação: `POST /telhados/segmentar-subestacao`
