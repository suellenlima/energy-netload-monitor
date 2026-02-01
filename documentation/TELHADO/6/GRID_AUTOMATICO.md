# Grid Automático em Endpoints de Telhados

## 📋 Visão Geral

Os endpoints de detecção e segmentação de telhados agora possuem a capacidade de **buscar e salvar automaticamente** imagens grid do Google Maps caso elas não existam no banco de dados.

## 🎯 Funcionalidade

### Comportamento Padrão (Anterior)
- Requer URL da imagem no corpo da requisição
- Processa apenas uma imagem por vez
- Não verifica banco de dados

### Novo Comportamento (Grid Automático)
- **Verifica** se existem imagens grid salvas no banco
- **Busca e salva** grid automaticamente se não existir
- **Usa** primeira imagem do grid para processamento
- **Mantém** compatibilidade com modo tradicional (URL direta)

## 🔧 Como Usar

### 1. Endpoint: Detectar Telhados

**URL:** `POST /telhados/transformador/detectar-telhados`

#### Modo Tradicional (URL direta)
```bash
curl -X POST "http://localhost:8000/telhados/transformador/detectar-telhados" \
  -H "Content-Type: application/json" \
  -d '{
    "transformador_id": 47,
    "subestacao_id": 1,
    "url_imagem": "https://maps.googleapis.com/...",
    "fonte_imagem": "google_maps",
    "confianca_minima": 0.5
  }'
```

#### Modo Grid Automático (NOVO)
```bash
curl -X POST "http://localhost:8000/telhados/transformador/detectar-telhados?usar_grid_auto=true" \
  -H "Content-Type: application/json" \
  -d '{
    "transformador_id": 47,
    "subestacao_id": 1,
    "fonte_imagem": "google_maps",
    "confianca_minima": 0.5
  }'
```

**Nota:** `url_imagem` pode ser omitida quando `usar_grid_auto=true`

#### Parâmetros

| Parâmetro | Tipo | Obrigatório | Default | Descrição |
|-----------|------|-------------|---------|-----------|
| `usar_grid_auto` | Query | Não | `false` | Se `true`, busca grid automaticamente |
| `transformador_id` | Body | Sim | - | ID do transformador |
| `subestacao_id` | Body | Sim | - | ID da subestação |
| `url_imagem` | Body | Condicional | - | URL da imagem (obrigatória se `usar_grid_auto=false`) |
| `fonte_imagem` | Body | Sim | - | Fonte: `"google_maps"` ou `"cbers4a"` |
| `confianca_minima` | Body | Não | `0.5` | Confiança mínima (0-1) |

#### Resposta com Grid Automático

```json
{
  "transformador_id": 47,
  "subestacao_id": 1,
  "sucesso": true,
  "total_telhados": 5,
  "area_total_m2": 650.0,
  "confianca_media": 0.78,
  "telhados": [
    {
      "telhado_id": 1,
      "bbox": {
        "x_min": 120,
        "y_min": 80,
        "x_max": 280,
        "y_max": 200
      },
      "confianca": 0.85,
      "area_m2": 150.0
    }
  ],
  "tempo_processamento_ms": 1234,
  "fonte_imagem": "google_maps",
  "origem_imagem": "grid_automatico_banco_dados",  // ← NOVO CAMPO
  "timestamp": "2024-01-15T10:30:00"
}
```

**Novos Campos:**
- `origem_imagem`: Indica se imagem veio de `"url_fornecida"`, `"grid_automatico_banco_dados"` ou `"grid_automatico_busca_automatica"`

---

### 2. Endpoint: Segmentar Telhados

**URL:** `POST /telhados/segmentar-subestacao`

#### Modo Tradicional (URL direta)
```bash
curl -X POST "http://localhost:8000/telhados/segmentar-subestacao" \
  -H "Content-Type: application/json" \
  -d '{
    "id_subestacao": 1,
    "url_imagem_satelite": "https://maps.googleapis.com/...",
    "resolucao_m_por_pixel": 0.5,
    "confianca_minima": 0.5,
    "salvar_rois": true,
    "diretorio_saida": "data/rois"
  }'
```

#### Modo Grid Automático (NOVO)
```bash
curl -X POST "http://localhost:8000/telhados/segmentar-subestacao?usar_grid_auto=true" \
  -H "Content-Type: application/json" \
  -d '{
    "id_subestacao": 1,
    "resolucao_m_por_pixel": 0.5,
    "confianca_minima": 0.5,
    "salvar_rois": true,
    "diretorio_saida": "data/rois"
  }'
```

**Nota:** `url_imagem_satelite` pode ser omitida quando `usar_grid_auto=true`

#### Parâmetros

| Parâmetro | Tipo | Obrigatório | Default | Descrição |
|-----------|------|-------------|---------|-----------|
| `usar_grid_auto` | Query | Não | `false` | Se `true`, busca grid automaticamente |
| `id_subestacao` | Body | Sim | - | ID da subestação |
| `url_imagem_satelite` | Body | Condicional | - | URL da imagem (obrigatória se `usar_grid_auto=false`) |
| `resolucao_m_por_pixel` | Body | Não | `0.5` | Resolução da imagem |
| `confianca_minima` | Body | Não | `0.5` | Confiança mínima (0-1) |
| `salvar_rois` | Body | Não | `false` | Salvar ROIs extraídos |
| `diretorio_saida` | Body | Não | - | Diretório de saída para ROIs |

---

### 3. Endpoint: Segmentar Transformador V2

**URL:** `POST /telhados/segmentar-transformador-v2`

Este endpoint pode usar **CBERS-4A** (se `imagem_id` fornecido) ou **Grid Google Maps** (se `imagem_id` omitido).

#### Modo Grid Automático (NOVO)
```bash
curl -X POST "http://localhost:8000/telhados/segmentar-transformador-v2" \
  -H "Content-Type: application/json" \
  -d '{
    "transformador_id": 47,
    "confianca_minima": 0.5,
    "salvar_rois": true,
    "diretorio_saida": "data/rois",
    "aplicar_filtro_ndvi": false
  }'
```

**Nota:** `imagem_id` omitido → usa grid automático do Google Maps

#### Modo CBERS-4A (Tradicional)
```bash
curl -X POST "http://localhost:8000/telhados/segmentar-transformador-v2" \
  -H "Content-Type: application/json" \
  -d '{
    "transformador_id": 47,
    "imagem_id": 13,
    "confianca_minima": 0.5,
    "salvar_rois": true,
    "aplicar_filtro_ndvi": true,
    "limiar_ndvi": 0.3
  }'
```

**Nota:** `imagem_id` fornecido → usa imagem CBERS-4A do banco

#### Parâmetros

| Parâmetro | Tipo | Obrigatório | Default | Descrição |
|-----------|------|-------------|---------|-----------|
| `transformador_id` | Body | Sim | - | ID do transformador |
| `imagem_id` | Body | Não | `null` | ID da imagem CBERS-4A (se omitido, usa grid) |
| `confianca_minima` | Body | Não | `0.5` | Confiança mínima (0-1) |
| `salvar_rois` | Body | Não | `true` | Salvar ROIs extraídos |
| `diretorio_saida` | Body | Não | - | Diretório de saída para ROIs |
| `aplicar_filtro_ndvi` | Body | Não | `true` | Filtro NDVI (apenas CBERS-4A) |
| `limiar_ndvi` | Body | Não | `0.3` | Limiar NDVI (apenas CBERS-4A) |

#### Resposta com Grid Automático

```json
{
  "transformador_id": 47,
  "imagem_id": 1234,
  "id_imagem_satelite": "grid_trafo_47_0_0",
  "timestamp_processamento": "2024-01-15T10:30:00",
  "telhados_detectados": 5,
  "total_telhados_segmentados": 5,
  "tempo_processamento_segundos": 2.5,
  "telhados": [...],
  "telhados_segmentados": [...],
  "bandas_processadas": ["rgb_google_maps"],
  "filtro_ndvi_aplicado": false,
  "limiar_ndvi_utilizado": null,
  "erros": [],
  "avisos": [],
  "sucesso": true,
  "mensagem": "Grid automático: 9 imagens (origem: banco_dados)"
}
```

---

## 🔄 Fluxo de Processamento

### Modo Grid Automático Ativado

```mermaid
graph TD
    A[Requisição com usar_grid_auto=true] --> B{URL fornecida?}
    B -->|Sim| C[Usar URL fornecida]
    B -->|Não| D[Verificar banco de dados]
    D --> E{Imagens grid<br/>existem?}
    E -->|Sim| F[Recuperar do banco]
    E -->|Não| G[Buscar Google Maps API]
    G --> H[Salvar no banco]
    H --> F
    F --> I[Processar primeira imagem grid]
    C --> J[Processar imagem]
    I --> K[Retornar resultado]
    J --> K
```

### Lógica de Fallback

1. **Prioridade 1:** Se `url_imagem` fornecida → usar URL direta
2. **Prioridade 2:** Se `usar_grid_auto=true` e grid existe no banco → usar banco
3. **Prioridade 3:** Se `usar_grid_auto=true` e grid não existe → buscar e salvar
4. **Erro:** Se `usar_grid_auto=false` e sem URL → erro 422

---

## 📊 Exemplos de Uso

### Python

```python
import requests

# Modo Grid Automático
url = "http://localhost:8000/telhados/transformador/detectar-telhados"
params = {"usar_grid_auto": True}
payload = {
    "transformador_id": 47,
    "subestacao_id": 1,
    "fonte_imagem": "google_maps",
    "confianca_minima": 0.6
}

response = requests.post(url, params=params, json=payload)
resultado = response.json()

print(f"Total telhados: {resultado['total_telhados']}")
print(f"Origem: {resultado['origem_imagem']}")
```

### JavaScript/TypeScript

```typescript
const detectarTelhados = async (transformadorId: number, subestacaoId: number) => {
  const response = await fetch(
    'http://localhost:8000/telhados/transformador/detectar-telhados?usar_grid_auto=true',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        transformador_id: transformadorId,
        subestacao_id: subestacaoId,
        fonte_imagem: 'google_maps',
        confianca_minima: 0.5
      })
    }
  );
  
  const resultado = await response.json();
  console.log(`Total: ${resultado.total_telhados}`);
  console.log(`Origem: ${resultado.origem_imagem}`);
  
  return resultado;
};
```

---

## ⚙️ Configuração

### Variáveis de Ambiente

```bash
# Google Maps API Key (obrigatória para grid automático)
GOOGLE_MAPS_API_KEY=your_api_key_here

# Database
DATABASE_URL=postgresql://user:password@localhost:5432/energia
```

### Parâmetros de Grid

Os parâmetros de grid são fixos no código:

```python
zoom_grid = 20      # Zoom do Google Maps
tamanho = "640x640" # Tamanho de cada célula
```

Para alterar, modifique a função `_verificar_ou_buscar_imagens_grid()` em [telhado.py](../../backend/src/api/telhado.py#L90-L220).

---

## 🗄️ Banco de Dados

### Tabela: `satelite_imagens`

Imagens grid são salvas com:

```sql
sensor = 'Google_Maps_Grid_Z20'
propriedades_json = {
  "transformador_id": "47",
  "subestacao_id": "1",
  "grid_posicao": {
    "linha": 0,
    "coluna": 1
  },
  "bbox": {...},
  "resolucao": {...},
  "estatisticas": {...}
}
```

### Consultar Imagens Grid

```sql
SELECT 
    id,
    url,
    sensor,
    propriedades_json->'grid_posicao'->>'linha' AS linha,
    propriedades_json->'grid_posicao'->>'coluna' AS coluna
FROM satelite_imagens
WHERE sensor LIKE 'Google_Maps_Grid%'
  AND propriedades_json->>'transformador_id' = '47'
ORDER BY 
    (propriedades_json->'grid_posicao'->>'linha')::int,
    (propriedades_json->'grid_posicao'->>'coluna')::int;
```

---

## 🔍 Monitoramento e Logs

### Logs de Grid Automático

```log
[INFO] [TELHADO] Modo grid automático ativado para transformador 47
[INFO] ⚠ Nenhuma imagem grid encontrada para transformador 47
[INFO] 📊 Buscando e salvando grid automaticamente (zoom=20, tamanho=640x640)...
[INFO] ✅ Grid salvo automaticamente: 9 imagens
[INFO] ✓ Usando 9 imagens do grid (primeira: linha=0, coluna=0)
[INFO] [TELHADO] Detectando telhados para transformador 47
```

### Verificar Uso

```python
# Contar requisições com grid automático
import logging

handler = logging.StreamHandler()
handler.addFilter(lambda record: "grid_automatico" in record.getMessage())
logger.addHandler(handler)
```

---

## ⚠️ Considerações

### Vantagens
✅ **Sem duplicação:** Reutiliza imagens já salvas  
✅ **Automático:** Busca e salva grid transparentemente  
✅ **Compatível:** Mantém funcionamento com URL direta  
✅ **Eficiente:** Reduz chamadas à API do Google Maps

### Limitações
⚠️ **Apenas primeira imagem:** Atualmente processa apenas a primeira célula do grid  
⚠️ **Zoom fixo:** Zoom 20 hardcoded (pode ser parametrizado)  
⚠️ **Quota Google Maps:** Consome quota ao buscar novos grids

### Melhorias Futuras
- [ ] Processar todas as células do grid (não apenas primeira)
- [ ] Agregar resultados de múltiplas células
- [ ] Parametrizar zoom e tamanho via query params
- [ ] Cache de imagens em memória/Redis
- [ ] Processamento paralelo de células

---

## 📚 Referências

- [GRID_IMAGENS_BANCO.md](./GRID_IMAGENS_BANCO.md) - Documentação completa sobre grid
- [telhado.py](../../backend/src/api/telhado.py) - Código fonte dos endpoints
- [Google Maps Static API](https://developers.google.com/maps/documentation/maps-static) - Documentação oficial

---

## 🐛 Troubleshooting

### Erro: "Nenhum transformador encontrado"
```json
{
  "detail": "Nenhum transformador encontrado para subestação 1"
}
```
**Solução:** Verificar se subestação tem transformadores cadastrados.

### Erro: "Erro ao obter grid"
```json
{
  "motivo": "Erro ao obter grid: Transformador 47 não encontrado"
}
```
**Solução:** Verificar se `transformador_id` existe no banco.

### Erro: "API key inválida"
```json
{
  "erro": "Erro ao gerar grid: Invalid API key"
}
```
**Solução:** Configurar `GOOGLE_MAPS_API_KEY` no arquivo `.env`.

### Grid vazio
```json
{
  "sucesso": false,
  "motivo": "Grid não contém imagens"
}
```
**Solução:** Verificar se transformador tem coordenadas válidas no banco.

---

**Última atualização:** 2024-01-15  
**Versão:** 1.0
