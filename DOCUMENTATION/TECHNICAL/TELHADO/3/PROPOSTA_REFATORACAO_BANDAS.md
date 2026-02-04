# Proposta de Refatoração: URLs de Bandas Multiespectrais no Banco

## 🎯 Problema Atual

Atualmente o sistema armazena apenas **uma URL por imagem** na tabela `satelite_imagens`:

```sql
CREATE TABLE satelite_imagens (
    id SERIAL PRIMARY KEY,
    url TEXT,           ← Uma única URL
    ...
)
```

Quando temos uma imagem CBERS-4A, na verdade há **5 bandas separadas** com URLs diferentes:
- Band 0 (Blue):  `.../BAND0.tif`
- Band 1 (Green): `.../BAND1.tif`
- Band 2 (Red):   `.../BAND2.tif`
- Band 3 (NIR):   `.../BAND3.tif`
- Band 4 (SWIR):  `.../BAND4.tif`

**Problema:**
- Precisamos fazer 5 requests HTTP separados cada vez que queremos processar
- Sem cache efetivo das URLs
- Ineficiente e repetitivo

---

## ✅ Solução Proposta

### Opção A: Estender `propriedades_json` (Mínima)

Usar o campo `propriedades_json` existente para armazenar todas as URLs:

```json
{
  "urls_bandas": {
    "band_0_blue": "https://data.inpe.br/.../BAND0.tif",
    "band_1_green": "https://data.inpe.br/.../BAND1.tif",
    "band_2_red": "https://data.inpe.br/.../BAND2.tif",
    "band_3_nir": "https://data.inpe.br/.../BAND3.tif",
    "band_4_swir": "https://data.inpe.br/.../BAND4.tif"
  },
  "tipo_satelite": "cbers4a",
  "otras_propriedades": "..."
}
```

**Vantagens:**
- Sem migração de schema (compatível com existente)
- Pronto para usar agora

**Desvantagens:**
- Não é normalizado (dados aninhados)
- Difícil de filtrar por banda
- Menos eficiente em grandes volumes

---

### Opção B: Tabela Separada (Recomendado)

Criar tabela `satelite_bandas` para armazenar URLs de bandas:

```sql
CREATE TABLE satelite_imagens (
    id SERIAL PRIMARY KEY,
    subestacao_id INTEGER,
    sensor VARCHAR(50),
    data_aquisicao TIMESTAMPTZ,
    ...
    url TEXT,  ← Manter compatibilidade (aponta para banda RGB ou principal)
    propriedades_json JSONB
);

CREATE TABLE satelite_bandas (
    id SERIAL PRIMARY KEY,
    imagem_id INTEGER REFERENCES satelite_imagens(id) ON DELETE CASCADE,
    numero_banda INTEGER,      -- 0, 1, 2, 3, 4
    nome_banda VARCHAR(20),    -- 'blue', 'green', 'red', 'nir', 'swir'
    url TEXT NOT NULL,         -- URL da banda
    resolucao_m INTEGER,       -- Resolução específica da banda (opcional)
    data_registro TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(imagem_id, numero_banda)
);

CREATE INDEX idx_bandas_imagem ON satelite_bandas(imagem_id);
```

**Vantagens:**
- Arquitetura limpa e normalizada
- Fácil fazer queries específicas por banda
- Escalável para novos satélites
- Suporta bandas opcionais

**Desvantagens:**
- Requer migração de banco
- Ligeiramente mais complexo

---

## 🏗️ Plano de Implementação

### Fase 1: Schema Database (Opção B Recomendada)

#### 1.1 Criar Nova Tabela
```sql
CREATE TABLE IF NOT EXISTS satelite_bandas (
    id SERIAL PRIMARY KEY,
    imagem_id INTEGER NOT NULL REFERENCES satelite_imagens(id) ON DELETE CASCADE,
    numero_banda INTEGER NOT NULL CHECK (numero_banda >= 0 AND numero_banda <= 4),
    nome_banda VARCHAR(20) NOT NULL,  -- 'blue', 'green', 'red', 'nir', 'swir'
    url TEXT NOT NULL,
    resolucao_m INTEGER,
    data_registro TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(imagem_id, numero_banda),
    CHECK (nome_banda IN ('blue', 'green', 'red', 'nir', 'swir'))
);

CREATE INDEX idx_bandas_imagem ON satelite_bandas(imagem_id);
CREATE INDEX idx_bandas_nome ON satelite_bandas(nome_banda);
```

#### 1.2 Função Helper para Inserir Imagem com Bandas
```python
def registrar_imagem_com_bandas(
    engine: Engine,
    subestacao_id: int,
    metadata: ImagemSateliteMetadata,
    urls_bandas: Dict[str, str]  # {'blue': '...', 'green': '...', ...}
) -> int:
    """
    Registra uma imagem e suas bandas.
    
    Returns:
        ID da imagem registrada
    """
    # Inserir imagem
    imagem_id = insertar_imagen(engine, subestacao_id, metadata)
    
    # Inserir bandas
    bands_mapping = {
        'blue': (0, 'blue'),
        'green': (1, 'green'),
        'red': (2, 'red'),
        'nir': (3, 'nir'),
        'swir': (4, 'swir')
    }
    
    for band_key, (band_num, band_name) in bands_mapping.items():
        if band_key in urls_bandas:
            insertar_banda(engine, imagem_id, band_num, band_name, urls_bandas[band_key])
    
    return imagem_id
```

### Fase 2: Ajustar Schemas Pydantic

#### 2.1 Novo Schema para Banda
```python
class BandaSatelite(BaseModel):
    numero_banda: int = Field(..., ge=0, le=4)
    nome_banda: str = Field(..., description="'blue', 'green', 'red', 'nir', 'swir'")
    url: str = Field(..., description="URL para download da banda")
    resolucao_m: Optional[int] = None

class ImagemSateliteMetadataEnhanced(BaseModel):
    """Versão estendida com suporte a múltiplas bandas."""
    id: int
    sensor: str
    data_aquisicao: datetime
    resolucao_m: int
    cobertura_nuvem_pct: float
    url: str  # Mantém compatibilidade - banda RGB ou principal
    bandas: List[BandaSatelite] = Field(default_factory=list)
    propriedades: Dict[str, Any] = Field(default_factory=dict)
```

#### 2.2 Response para GET
```python
class ListaImagensSateliteEnhanced(BaseModel):
    subestacao_id: int
    total_imagens: int
    imagens: List[ImagemSateliteMetadataEnhanced]
```

### Fase 3: Ajustar Endpoints

#### 3.1 GET `/satelite/v2/transformador/{transformador_id}/imagens`
```python
@router.get("/satelite/v2/transformador/{transformador_id}/imagens")
async def listar_imagens_satelite(transformador_id: int):
    """
    Lista imagens com TODAS as URLs de bandas disponíveis.
    """
    # Query:
    query = """
        SELECT 
            si.id, si.sensor, si.data_aquisicao, si.resolucao_m,
            si.cobertura_nuvem_pct, si.url,
            sb.numero_banda, sb.nome_banda, sb.url as band_url
        FROM satelite_imagens si
        LEFT JOIN satelite_bandas sb ON si.id = sb.imagem_id
        WHERE si.subestacao_id IN (
            SELECT id FROM subestacoes_detectadas WHERE transformador_id = :tid
        )
        ORDER BY si.data_aquisicao DESC
    """
    
    # Retorna estrutura com bandas aninhadas
    response = {
        "imagens": [
            {
                "id": 1,
                "sensor": "cbers4a",
                "url": "...",  # Compatibilidade
                "bandas": [
                    {"numero_banda": 0, "nome_banda": "blue", "url": "...BAND0.tif"},
                    {"numero_banda": 1, "nome_banda": "green", "url": "...BAND1.tif"},
                    {"numero_banda": 2, "nome_banda": "red", "url": "...BAND2.tif"},
                    {"numero_banda": 3, "nome_banda": "nir", "url": "...BAND3.tif"},
                ]
            }
        ]
    }
```

### Fase 4: Adaptar Código de Detecção

#### 4.1 Novo Serviço de Download Multi-Banda
```python
class ImagemMultibandaLoader:
    @staticmethod
    def baixar_bandas(urls_bandas: Dict[str, str]) -> Dict[str, np.ndarray]:
        """
        Baixa múltiplas bandas de uma imagem.
        
        Args:
            urls_bandas: {'blue': '...', 'green': '...', ...}
        
        Returns:
            {'blue': array, 'green': array, ...}
        """
        bandas = {}
        
        for nome, url in urls_bandas.items():
            response = requests.get(url, timeout=30)
            
            with MemoryFile(response.content) as memfile:
                with memfile.open() as dataset:
                    bandas[nome] = dataset.read(1)
        
        return bandas
    
    @staticmethod
    def processar_rgb_clahe(bandas: Dict) -> np.ndarray:
        """
        Processa RGB com CLAHE.
        """
        def normalizar(banda):
            banda = banda.astype(np.float32)
            p2, p98 = np.percentile(banda, [2, 98])
            if p98 > p2:
                banda = (banda - p2) / (p98 - p2) * 255
            else:
                banda = np.full_like(banda, 128)
            return np.clip(banda, 0, 255).astype(np.uint8)
        
        b = normalizar(bandas.get('blue', bandas['red']))
        g = normalizar(bandas.get('green', bandas['red']))
        r = normalizar(bandas.get('red', bandas['red']))
        
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        b = clahe.apply(b)
        g = clahe.apply(g)
        r = clahe.apply(r)
        
        return cv2.merge([b, g, r])
    
    @staticmethod
    def calcular_ndvi(bandas: Dict) -> Optional[np.ndarray]:
        """
        Calcula NDVI se NIR disponível.
        """
        if 'nir' not in bandas or 'red' not in bandas:
            return None
        
        nir = bandas['nir'].astype(np.float32)
        red = bandas['red'].astype(np.float32)
        
        denominador = nir + red + 1e-10
        ndvi = (nir - red) / denominador
        return np.clip(ndvi, -1, 1)
```

#### 4.2 Modificar Endpoint de Detecção
```python
@router.post("/telhados/transformador/detectar-telhados")
def detectar_telhados(requisicao: ProcessarComYOLORequest):
    """
    Versão nova que recebe URLs de bandas.
    """
    # urls_bandas vêm do banco ou da request
    urls_bandas = requisicao.urls_bandas or {
        'blue': requisicao.url_imagem  # fallback
    }
    
    # Baixar todas as bandas
    bandas = ImagemMultibandaLoader.baixar_bandas(urls_bandas)
    
    # Processar RGB
    imagem_rgb = ImagemMultibandaLoader.processar_rgb_clahe(bandas)
    
    # Calcular NDVI
    ndvi = ImagemMultibandaLoader.calcular_ndvi(bandas)
    
    # Passar para YOLO com NDVI mask
    # ...
```

### Fase 5: Backward Compatibility

Manter endpoint antigo funcionando:
- Se vem URL única → inferir que é a banda principal
- Se vem múltiplas URLs → usar todas

```python
# Compatibilidade
if isinstance(url_imagem, str):
    # Antigo: url única
    urls_bandas = {'red': url_imagem}
elif isinstance(url_imagem, dict):
    # Novo: múltiplas bandas
    urls_bandas = url_imagem
```

---

## 📋 Arquivos a Modificar

1. **Database Schema**
   - [backend/src/services/inpe_satellite_service.py](backend/src/services/inpe_satellite_service.py) - Add `satelite_bandas` table
   
2. **Schemas Pydantic**
   - [backend/src/schemas/satelite.py](backend/src/schemas/satelite.py) - Add `BandaSatelite`, extend `ImagemSateliteMetadata`
   
3. **Endpoints**
   - [backend/src/api/satelite_v2.py](backend/src/api/satelite_v2.py) - Extend GET to return bandas
   
4. **Services**
   - [backend/src/services/satellite_service_v2.py](backend/src/services/satellite_service_v2.py) - New `ImagemMultibandaLoader`
   
5. **Detecção de Telhados**
   - [backend/src/api/telhado.py](backend/src/api/telhado.py) - Usar URLs de bandas do banco

---

## 🎁 Benefícios

### Curto Prazo
- ✅ Cache de URLs (5x menos requests)
- ✅ Melhor suporte a RGB real
- ✅ NDVI automático quando disponível

### Médio Prazo
- ✅ Suporte multi-satélite fácil
- ✅ Queries eficientes por banda
- ✅ Auditoria de qual banda foi usada

### Longo Prazo
- ✅ Arquitetura preparada para análises temporais
- ✅ Fusão de sensores (Sentinel-2 + CBERS)
- ✅ Análises espectrais avançadas

---

## 📊 Impacto de Performance

### Antes (Atual)
```
Request 1: GET /imagens → 1 URL (Red only)
Request 2-5: Download Blue, Green, Red, NIR (se necessário)
Total: 5+ requests
Tempo: 15-30s para processar
```

### Depois (Proposto)
```
Request 1: GET /imagens → 5 URLs (todas as bandas já no response)
Download: Uma chamada única para todas as bandas
Total: 2-3 requests  (50-60% melhoria)
Tempo: 8-15s para processar
```

---

**Recomendação: Implementar Opção B (Tabela Separada)**

Quer que eu implemente este plano?
