# 📚 DOCUMENTAÇÃO: COMPONENTES DO SISTEMA DE TRANSFORMADORES

Documento que descreve **o que cada arquivo faz** no sistema de transformadores.

---

# 📍 ÍNDICE

1. [API Transformadores](#-api-transformadores)
2. [Repository de Transformadores](#-repository-de-transformadores)
3. [Service de Transformadores](#-service-de-transformadores)
4. [Fluxo de Dados](#-fluxo-de-dados)
5. [Integração dos Componentes](#-integração-dos-componentes)

---

# 🌐 API Transformadores

**Arquivo:** `backend/src/api/transformadores.py` (390 linhas)

**Responsabilidade:** Definir endpoints HTTP e orquestrar requisições

## 🎯 O que faz?

Expõe **14 endpoints REST** para acesso aos dados de transformadores e seus consumidores associados. Cada endpoint:
- Recebe requisições HTTP
- Valida parâmetros de entrada
- Chama o Service apropriado
- Formata e retorna respostas JSON
- Trata erros HTTP (404, 400, 500)

## 📊 Endpoints (14 total)

### 1. Detalhes e Áreas (3 endpoints)

#### `GET /api/v1/transformadores/{id}`
- **Propósito:** Obter detalhes completos de um transformador
- **Parâmetro:** `id` (integer)
- **Retorna:** 
  ```json
  {
    "status": "success",
    "data": {
      "id": 1,
      "codigo": "2_3434",
      "nome": "TR-123",
      "latitude": -26.9561,
      "longitude": -52.5180,
      "potencia_kva": 75.0,
      "tipo_tensao": "MT",
      "distribuidora": "IENERGIA_87_2021",
      "area_cobertura": {...}
    }
  }
  ```

#### `GET /api/v1/transformadores/{id}/area`
- **Propósito:** Retorna área de cobertura em 3 formatos
- **Parâmetro:** `formato` (geojson|wkt|json)
- **Casos de uso:** 
  - GeoJSON: QGIS, Mapbox, ArcGIS
  - WKT: Bancos de dados
  - JSON: APIs genéricas
- **Retorna:** Geometria em formato solicitado

#### `GET /api/v1/transformadores/{id}/bbox`
- **Propósito:** Bounding box para download de satélite
- **Parâmetro:** `margem_km` (0.1-50 km, padrão: 2)
- **Casos de uso:**
  - Sentinel-2
  - Planetary Computer
  - Google Earth Engine
- **Retorna:**
  ```json
  {
    "bbox": {
      "min_lat": -26.97,
      "min_lon": -52.53,
      "max_lat": -26.94,
      "max_lon": -52.51
    },
    "margem_km": 2.0
  }
  ```

### 2. Listagem (4 endpoints)

#### `GET /api/v1/transformadores`
- **Propósito:** Listar TODOS os transformadores
- **Parâmetros:** `skip` (padrão: 0), `limit` (padrão: 100, máx: 1000)
- **Paginação:** Suportada
- **Retorna:** Lista com total e metadados

#### `GET /api/v1/transformadores/subestacao/{subestacao_codigo}`
- **Propósito:** Listar transformadores de UMA subestação
- **Parâmetro:** `subestacao_codigo` (string)
- **Paginação:** Suportada
- **Exemplo:** `GET /api/v1/transformadores/subestacao/SUB-001?limit=50`

#### `GET /api/v1/transformadores/distribuidora/{distribuidora}`
- **Propósito:** Listar transformadores de UMA distribuidora
- **Parâmetro:** `distribuidora` (partial match)
- **Paginação:** Suportada
- **Exemplo:** `GET /api/v1/transformadores/distribuidora/IENERGIA?limit=50`

#### `GET /api/v1/transformadores/tipo-tensao/{tipo_tensao}`
- **Propósito:** Listar transformadores por tensão
- **Parâmetro:** `tipo_tensao` (BT|MT|AT)
- **Paginação:** Suportada
- **Exemplo:** `GET /api/v1/transformadores/tipo-tensao/BT?limit=100`

### 3. Estatísticas (2 endpoints)

#### `GET /api/v1/transformadores/stats/geral`
- **Propósito:** Estatísticas consolidadas de transformadores
- **Retorna:**
  ```json
  {
    "total": 3548,
    "total_bt": 0,
    "total_mt": 0,
    "total_at": 0,
    "potencia_media_kva": 56.93,
    "potencia_total_kva": 202149.84
  }
  ```

#### `GET /api/v1/transformadores/stats/areas`
- **Propósito:** Estatísticas de áreas de cobertura
- **Retorna:**
  ```json
  {
    "total_areas": 0,
    "areas_convex_hull": 0,
    "areas_buffer": 0,
    "area_media_km2": 0,
    "area_total_km2": 0
  }
  ```

### 4. Busca Espacial (1 endpoint)

#### `GET /api/v1/transformadores/regiao/buscar`
- **Propósito:** Buscar transformadores em região (bbox)
- **Parâmetros:** `min_lat`, `min_lon`, `max_lat`, `max_lon`
- **Paginação:** Suportada
- **Validações:** Coordenadas válidas (-90 a 90 lat, -180 a 180 lon)
- **Exemplo:** `GET /api/v1/transformadores/regiao/buscar?min_lat=-23.7&min_lon=-46.8&max_lat=-23.4&max_lon=-46.4`

### 5. Export (1 endpoint)

#### `GET /api/v1/transformadores/export/{formato}`
- **Propósito:** Exportar TODOS os transformadores
- **Formatos:**
  - `csv` → Arquivo para Excel/Sheets
  - `json` → JSON com lista
  - `geojson` → GeoJSON FeatureCollection
- **Exemplos:**
  ```
  GET /api/v1/transformadores/export/csv
  GET /api/v1/transformadores/export/geojson
  GET /api/v1/transformadores/export/json
  ```

### 6. Consumidores Associados (3 endpoints) ✨

#### `GET /api/v1/transformadores/{id}/consumidores/resumo`
- **Propósito:** Contagem de consumidores BT/MT/AT
- **Parâmetro:** `id` (integer)
- **Retorna:**
  ```json
  {
    "transformador_id": 1,
    "transformador_codigo": "2_3434",
    "data": {
      "consumidores_bt": 45,
      "consumidores_mt": 3,
      "consumidores_at": 0,
      "total_consumidores": 48
    }
  }
  ```

#### `GET /api/v1/transformadores/{id}/consumidores/bt`
- **Propósito:** Listar consumidores de BAIXA tensão
- **Parâmetros:** `id`, `limit` (padrão: 100, máx: 1000)
- **Retorna:** Lista de consumidores BT com campos:
  - codigo, municipio_codigo, carga_instalada_kw
  - latitude, longitude, data_conexao
- **Ordenação:** Por carga_instalada_kw DESC

#### `GET /api/v1/transformadores/{id}/consumidores/mt`
- **Propósito:** Listar consumidores de MÉDIA tensão
- **Parâmetros:** `id`, `limit` (padrão: 100, máx: 1000)
- **Retorna:** Lista de consumidores MT com campos:
  - codigo, municipio_codigo, carga_instalada_kw, demanda_contratada_kw
  - latitude, longitude, data_conexao
- **Ordenação:** Por demanda_contratada_kw DESC

## 🔄 Fluxo

```
Requisição HTTP
    ↓
Validação de parâmetros
    ↓
Instanciação de TransformadorService
    ↓
Chamada de método do Service
    ↓
Tratamento de erros
    ↓
Formatação de resposta JSON
    ↓
Resposta HTTP
```

---

# 🗄️ Repository de Transformadores

**Arquivo:** `backend/src/repositories/transformador_repository.py` (316 linhas)

**Responsabilidade:** Acesso ao banco de dados (SELECT, filtros, consultas SQL)

## 🎯 O que faz?

Encapsula **TODAS** as operações SQL relacionadas a transformadores. Oferece interface limpa para:
- Buscar transformadores
- Listar com filtros
- Contar registros
- Gerar estatísticas
- Exportar para DataFrame

## 📊 Métodos (13 total)

### Métodos de Busca (5)

#### `obter_por_id(transformador_id: int) → Dict`
- **SQL:** SELECT com WHERE id = ?
- **Retorna:** Dicionário com dados do transformador ou None
- **Campos:** id, codigo, nome, lat, lon, potencia_kva, tipo_tensao, etc.

#### `obter_area_cobertura(transformador_id: int) → Dict`
- **SQL:** JOIN com transformador_area_cobertura
- **Retorna:** Dados da área (metodo_calculo, area_km2, geom_wkt, geom_geojson)
- **Nota:** Pode retornar None se área não foi calculada

#### `listar_todos(skip=0, limit=100) → pd.DataFrame`
- **SQL:** SELECT com LIMIT/OFFSET
- **Retorna:** DataFrame com todos os transformadores ativos

#### `listar_por_subestacao(subestacao_codigo: str, skip=0, limit=100) → pd.DataFrame`
- **SQL:** WHERE subestacao_codigo = ?
- **Retorna:** DataFrame filtrado por subestação

#### `listar_por_distribuidora(distribuidora: str, skip=0, limit=100) → pd.DataFrame`
- **SQL:** WHERE distribuidora ILIKE ?
- **Retorna:** DataFrame filtrado por distribuidora (partial match)

### Métodos de Filtro (2)

#### `buscar_por_regiao(min_lat, min_lon, max_lat, max_lon) → pd.DataFrame`
- **SQL:** WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ?
- **Retorna:** DataFrame com transformadores na região
- **Caso de uso:** Busca por bounding box

#### `buscar_por_tipo_tensao(tipo_tensao: str, skip=0, limit=100) → pd.DataFrame`
- **SQL:** WHERE tipo_tensao = ?
- **Parâmetro:** BT, MT ou AT
- **Retorna:** DataFrame filtrado

### Métodos de Contagem (2)

#### `contar_total() → int`
- **SQL:** SELECT COUNT(*) WHERE ativo = true
- **Retorna:** Total de transformadores

#### `contar_por_subestacao(subestacao_codigo: str) → int`
- **SQL:** SELECT COUNT(*) WHERE subestacao_codigo = ?
- **Retorna:** Total de transformadores em subestação

### Métodos de Estatísticas (3)

#### `obter_estadisticas_gerais() → Dict`
- **SQL:** SELECT com agregações (COUNT, AVG, MIN, MAX, SUM)
- **Retorna:**
  ```json
  {
    "total": 3548,
    "total_bt": 0,
    "total_mt": 0,
    "total_at": 0,
    "total_subestacoes": 1,
    "potencia_media_kva": 56.93,
    "potencia_minima_kva": 5.0,
    "potencia_maxima_kva": 1000.0,
    "potencia_total_kva": 202149.84
  }
  ```

#### `obter_estatisticas_areas() → Dict`
- **SQL:** SELECT com GROUP BY metodo_calculo
- **Retorna:** Stats de áreas (total, ConvexHull vs Buffer, área média/total)

#### `exportar_como_dataframe() → pd.DataFrame`
- **SQL:** SELECT com LEFT JOIN transformador_area_cobertura
- **Retorna:** DataFrame com transformadores + áreas

### Métodos de Consumidores (3) ✨

#### `contar_consumidores_por_transformador(transformador_codigo: str) → Dict`
- **SQL:** Subconsultas em consumidores_bt/mt/at_aneel
- **Retorna:**
  ```json
  {
    "consumidores_bt": 45,
    "consumidores_mt": 3,
    "consumidores_at": 0,
    "total_consumidores": 48
  }
  ```

#### `obter_consumidores_bt_por_transformador(transformador_codigo: str, limit=100) → pd.DataFrame`
- **SQL:** SELECT FROM consumidores_bt_aneel WHERE transformador_mt_codigo = ?
- **Retorna:** DataFrame com consumidores BT
- **Ordenação:** carga_instalada_kw DESC

#### `obter_consumidores_mt_por_transformador(transformador_codigo: str, limit=100) → pd.DataFrame`
- **SQL:** SELECT FROM consumidores_mt_aneel WHERE circuito_mt_codigo = ?
- **Retorna:** DataFrame com consumidores MT
- **Ordenação:** demanda_contratada_kw DESC

## 🔄 Fluxo

```
Método chamado
    ↓
SQL construída
    ↓
Parâmetros bindados (sanitização)
    ↓
Conexão com banco executada
    ↓
Resultado convertido (Dict ou DataFrame)
    ↓
Retorno
```

---

# 💼 Service de Transformadores

**Arquivo:** `backend/src/services/transformador_service.py` (455 linhas)

**Responsabilidade:** Lógica de negócio (validações, formatações, orquestração)

## 🎯 O que faz?

Implementa **regras de negócio** e **orquestração** entre Repository e API. Oferece interface de alto nível com:
- Validações de entrada
- Transformações de dados
- Formatações (GeoJSON, WKT, etc.)
- Composição de objetos complexos
- Tratamento de casos especiais

## 📊 Métodos (15 total)

### Métodos de Detalhes (3)

#### `obter_detalhes(transformador_id: int) → Dict`
- **Lógica:**
  1. Busca transformador no Repository
  2. Busca área de cobertura
  3. Compõe resultado com ambos
- **Retorna:** Dicionário com transformador + área
- **Tratamento:** Log se não encontrado

#### `obter_area_cobertura_geojson(transformador_id: int, formato: str) → Dict`
- **Lógica:** Busca área e formata conforme solicitado
- **Formatos:**
  - `geojson` → GeoJSON Feature com properties
  - `wkt` → WKT + metadados
  - `json` → Dados estruturados
- **Retorna:** Dict formatado ou None

#### `obter_bbox_para_satelite(transformador_id: int, margem_km: float) → Dict`
- **Lógica:**
  1. Busca transformador
  2. Extrai lat/lon
  3. Converte margem (km → graus)
  4. Calcula bbox
- **Fórmula:** delta = margem_km / 111.0 (1° ≈ 111 km)
- **Retorna:** Dict com bbox e margens

### Métodos de Listagem (5)

#### `listar_todos(skip=0, limit=100) → Dict`
- **Lógica:** Chama Repository, adiciona metadados
- **Retorna:**
  ```json
  {
    "data": [...],
    "total": 3548,
    "skip": 0,
    "limit": 100,
    "tem_proxima": true
  }
  ```

#### `listar_por_subestacao(subestacao_codigo: str, skip=0, limit=100) → Dict`
- **Lógica:** Filtra + adiciona metadados
- **Retorna:** Dict com subestacao_codigo e dados

#### `listar_por_distribuidora(distribuidora: str, skip=0, limit=100) → Dict`
- **Lógica:** Filtra + adiciona metadados
- **Retorna:** Dict com distribuidora e dados

#### `listar_por_tipo_tensao(tipo_tensao: str, skip=0, limit=100) → Dict`
- **Validação:** Verifica se BT/MT/AT válido
- **Erro:** ValueError se inválido
- **Retorna:** Dict com tipo_tensao e dados

#### `buscar_por_regiao(min_lat, min_lon, max_lat, max_lon, skip=0, limit=100) → Dict`
- **Validação:** Coordenadas dentro dos limites
- **Erro:** ValueError se coordenadas inválidas
- **Retorna:** Dict com bbox e dados paginados

### Métodos de Estatísticas (2)

#### `obter_estatisticas_gerais() → Dict`
- **Lógica:** Chama Repository sem transformação
- **Retorna:** Dict com stats (total, por tipo, potência, etc.)

#### `obter_estatisticas_areas() → Dict`
- **Lógica:** Chama Repository sem transformação
- **Retorna:** Dict com stats de áreas

### Métodos de Export (3)

#### `exportar_csv() → str`
- **Lógica:** DataFrame → CSV string
- **Retorna:** String CSV (headers + dados)

#### `exportar_geojson() → Dict`
- **Lógica:** DataFrame → GeoJSON FeatureCollection
- **Filtro:** Apenas registros com lat/lon
- **Retorna:** Dict GeoJSON com features

#### `exportar_json() → Dict`
- **Lógica:** DataFrame → List of dicts
- **Retorna:** Dict com lista de transformadores

#### `exportar(formato: str) → str | Dict`
- **Interface unificada:** csv, json, geojson
- **Erro:** ValueError se formato inválido
- **Retorna:** CSV string ou Dict

### Métodos de Consumidores (3) ✨

#### `obter_consumidores_associados(transformador_codigo: str) → Dict`
- **Lógica:** Chama Repository.contar_consumidores_por_transformador()
- **Retorna:**
  ```json
  {
    "consumidores_bt": 45,
    "consumidores_mt": 3,
    "consumidores_at": 0,
    "total_consumidores": 48
  }
  ```

#### `listar_consumidores_bt_do_transformador(transformador_codigo: str, limit=100) → Dict`
- **Lógica:** Busca BT + formata resultado
- **Retorna:**
  ```json
  {
    "transformador_codigo": "2_3434",
    "tipo_consumidor": "BT (Baixa Tensão)",
    "data": [...],
    "total": 45
  }
  ```

#### `listar_consumidores_mt_do_transformador(transformador_codigo: str, limit=100) → Dict`
- **Lógica:** Busca MT + formata resultado
- **Retorna:** Dict similar ao BT com dados MT

## 🔄 Fluxo

```
Chamada de método
    ↓
Validação de entrada
    ↓
Chamada ao Repository
    ↓
Transformação de dados
    ↓
Adição de metadados
    ↓
Formatação da resposta
    ↓
Retorno
```

---

# 🔀 Fluxo de Dados

## Cenário 1: Buscar Transformador Específico

```
GET /api/v1/transformadores/1
    ↓ (API)
get_transformador_detalhes(id=1)
    ↓
service.obter_detalhes(1)
    ├→ repository.obter_por_id(1)
    │  └─ SQL: SELECT * FROM transformadores_aneel WHERE id = 1
    │     Retorna: Dict com dados
    ├→ repository.obter_area_cobertura(1)
    │  └─ SQL: SELECT * FROM transformador_area_cobertura WHERE transformador_codigo = ...
    │     Retorna: Dict com área
    └─ Compõe resultado: {**transformador, 'area_cobertura': area}
    ↓
Response JSON:
{
  "status": "success",
  "data": {
    "id": 1,
    "codigo": "2_3434",
    "nome": "TR-001",
    "area_cobertura": {...}
  }
}
```

## Cenário 2: Listar com Filtro

```
GET /api/v1/transformadores/distribuidora/IENERGIA?limit=50
    ↓ (API)
listar_transformadores_distribuidora(distribuidora="IENERGIA", skip=0, limit=50)
    ↓
service.listar_por_distribuidora("IENERGIA", 0, 50)
    ↓
repository.listar_por_distribuidora("IENERGIA", 0, 50)
    └─ SQL: SELECT * FROM transformadores_aneel 
            WHERE distribuidora ILIKE '%IENERGIA%' AND ativo = true
            LIMIT 50 OFFSET 0
    └─ Retorna: DataFrame com até 50 registros
    ↓
repository.contar_por_distribuidora("IENERGIA")
    └─ SQL: SELECT COUNT(*) FROM transformadores_aneel
            WHERE distribuidora ILIKE '%IENERGIA%' AND ativo = true
    └─ Retorna: int (total)
    ↓
Service compõe resposta com metadados
    ↓
Response JSON:
{
  "status": "success",
  "distribuidora": "IENERGIA",
  "data": [...],
  "total": 3548,
  "skip": 0,
  "limit": 50,
  "tem_proxima": true
}
```

## Cenário 3: Consumidores Associados

```
GET /api/v1/transformadores/1/consumidores/resumo
    ↓ (API)
obter_resumo_consumidores(id=1)
    ├→ service.obter_detalhes(1)
    │  └─ Busca transformador (vê: codigo = "2_3434")
    ├→ service.obter_consumidores_associados("2_3434")
    │  ├─ repository.contar_consumidores_por_transformador("2_3434")
    │  │  ├─ SQL: SELECT COUNT(*) FROM consumidores_bt_aneel 
    │  │  │        WHERE transformador_mt_codigo = '2_3434'
    │  │  ├─ SQL: SELECT COUNT(*) FROM consumidores_mt_aneel 
    │  │  │        WHERE circuito_mt_codigo = '2_3434'
    │  │  └─ SQL: SELECT COUNT(*) FROM consumidores_at_aneel 
    │  │           WHERE circuito_at_codigo = '2_3434'
    │  └─ Retorna: Dict com contagem
    └─ Compõe resposta
    ↓
Response JSON:
{
  "status": "success",
  "transformador_id": 1,
  "transformador_codigo": "2_3434",
  "data": {
    "consumidores_bt": 45,
    "consumidores_mt": 3,
    "consumidores_at": 0,
    "total_consumidores": 48
  }
}
```

---

# 🔗 Integração dos Componentes

## Diagrama de Chamadas

```
┌─────────────────────────────────────────────────────┐
│ API (transformadores.py)                            │
│                                                     │
│ Endpoints:                                          │
│ - GET /{id}                                         │
│ - GET /{id}/area                                    │
│ - GET /{id}/bbox                                    │
│ - GET /subestacao/{codigo}                          │
│ - GET /distribuidora/{nome}                         │
│ - GET /tipo-tensao/{tipo}                           │
│ - GET /stats/geral                                  │
│ - GET /stats/areas                                  │
│ - GET /regiao/buscar                                │
│ - GET /export/{formato}                             │
│ - GET /{id}/consumidores/resumo                     │
│ - GET /{id}/consumidores/bt                         │
│ - GET /{id}/consumidores/mt                         │
│ - GET (listar todos)                                │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Depende de
                   │
┌──────────────────▼──────────────────────────────────┐
│ Service (transformador_service.py)                  │
│                                                     │
│ Métodos (15):                                       │
│ - obter_detalhes()                                  │
│ - obter_area_cobertura_geojson()                    │
│ - obter_bbox_para_satelite()                        │
│ - listar_todos()                                    │
│ - listar_por_subestacao()                           │
│ - listar_por_distribuidora()                        │
│ - listar_por_tipo_tensao()                          │
│ - buscar_por_regiao()                               │
│ - obter_estatisticas_gerais()                       │
│ - obter_estatisticas_areas()                        │
│ - exportar_csv()                                    │
│ - exportar_geojson()                                │
│ - exportar_json()                                   │
│ - obter_consumidores_associados()                   │
│ - listar_consumidores_bt/mt_do_transformador()      │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Usa
                   │
┌──────────────────▼──────────────────────────────────┐
│ Repository (transformador_repository.py)            │
│                                                     │
│ Métodos (13):                                       │
│ - obter_por_id()                                    │
│ - obter_area_cobertura()                            │
│ - listar_todos()                                    │
│ - listar_por_subestacao()                           │
│ - listar_por_distribuidora()                        │
│ - buscar_por_regiao()                               │
│ - buscar_por_tipo_tensao()                          │
│ - contar_total()                                    │
│ - contar_por_subestacao()                           │
│ - obter_estadisticas_gerais()                       │
│ - obter_estatisticas_areas()                        │
│ - exportar_como_dataframe()                         │
│ - contar_consumidores_por_transformador()           │
│ - obter_consumidores_bt/mt_por_transformador()      │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Acessa
                   │
┌──────────────────▼──────────────────────────────────┐
│ PostgreSQL 15 + PostGIS + TimescaleDB               │
│                                                     │
│ Tabelas:                                            │
│ - transformadores_aneel (3.548)                     │
│ - subestacoes_aneel (4)                             │
│ - consumidores_bt_aneel (0)                         │
│ - consumidores_mt_aneel (0)                         │
│ - consumidores_at_aneel (0)                         │
│ - transformador_area_cobertura (0)                  │
└─────────────────────────────────────────────────────┘
```

## Responsabilidades por Camada

| Camada | Responsabilidade | Exemplo |
|--------|------------------|---------|
| **API** | HTTP, Rotas, Validação de query params | Validar `limite: int` é válido |
| **Service** | Lógica, Validações de negócio, Formatações | Converter km para graus para bbox |
| **Repository** | SQL puro, Acesso BD, DataFrames | Construir query com LIMIT/OFFSET |
| **Database** | Persistência, Índices, Constraints | Validar potência > 0 |

## Fluxo de Erros

```
Requisição HTTP inválida
    ↓ (API valida query params)
HTTP 400 Bad Request

Transformador não existe
    ↓ (API verifica retorno do Service)
HTTP 404 Not Found

Erro interno no Service
    ↓ (Try/except)
HTTP 500 Internal Server Error

Coordenadas inválidas
    ↓ (Service valida antes de BD)
HTTP 400 Bad Request
```

---

# 📈 Casos de Uso Implementados

## 1️⃣ Análise de Transformador Individual

```
GET /api/v1/transformadores/1
GET /api/v1/transformadores/1/area?formato=geojson
GET /api/v1/transformadores/1/bbox?margem_km=5
GET /api/v1/transformadores/1/consumidores/resumo
GET /api/v1/transformadores/1/consumidores/bt?limit=100
```

## 2️⃣ Planejamento de Rede

```
GET /api/v1/transformadores/stats/geral
GET /api/v1/transformadores/distribuidora/IENERGIA?limit=1000
GET /api/v1/transformadores/tipo-tensao/MT?limit=1000
```

## 3️⃣ Busca Geográfica

```
GET /api/v1/transformadores/regiao/buscar?min_lat=-23.7&min_lon=-46.8&max_lat=-23.4&max_lon=-46.4
GET /api/v1/transformadores/{id}/bbox?margem_km=2
```

## 4️⃣ BI e Relatórios

```
GET /api/v1/transformadores/stats/areas
GET /api/v1/transformadores/export/geojson
GET /api/v1/transformadores/export/csv
```

## 5️⃣ Análise de Consumidores

```
GET /api/v1/transformadores/1/consumidores/resumo
GET /api/v1/transformadores/1/consumidores/bt?limit=50
GET /api/v1/transformadores/1/consumidores/mt?limit=50
```

---

# 🎯 Sumário Executivo

## API
- **Função:** Expõe 14 endpoints REST
- **Foco:** HTTP, validação de entrada, resposta JSON
- **Interação:** Depende de TransformadorService

## Service
- **Função:** Lógica de negócio e orquestração
- **Foco:** Validações, transformações, formatações
- **Interação:** Depende de TransformadorRepository

## Repository
- **Função:** Acesso ao banco de dados
- **Foco:** SQL seguro, filtros, agregações
- **Interação:** Acessa PostgreSQL + PostGIS

## Integração
- **Padrão:** 3 camadas bem definidas
- **Vantagem:** Fácil de testar, manter e estender
- **Escalabilidade:** Cada camada pode evoluir independente

