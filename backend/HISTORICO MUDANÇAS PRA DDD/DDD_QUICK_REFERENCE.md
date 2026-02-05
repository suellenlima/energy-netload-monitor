# DDD Quick Reference - Transformador Module

## 📂 File Structure

```
domain/
├── comum/
│   ├── errors.py           # DomainError base class
│   ├── value_objects.py    # Localizacao, Potencia, Temperatura
│   └── __init__.py
│
└── transformador/
    ├── entity.py           # Transformador aggregate root
    ├── value_objects.py    # CodigoTransformador, NomeTransformador, TensaoTipo, AreaCobertura
    ├── errors.py           # TransformadorError, TransformadorNotFoundError, InvalidTransformadorError
    ├── repository_interface.py  # ITransformadorRepository
    └── __init__.py

application/
└── transformador/
    ├── use_cases.py        # 5 use cases for transformador operations
    └── __init__.py

infrastructure/
├── persistence/
│   ├── transformador_repository.py  # SQLAlchemyTransformadorRepository
│   └── __init__.py
│
└── mappers/
    ├── transformador_mapper.py  # TransformadorMapper
    └── __init__.py

api/
└── transformadores_v2.py   # New DDD-based endpoints

schemas/
└── transformador.py        # TransformadorListResponse, TransformadorDetailResponse
```

---

## 🔨 How to Use

### Creating a Transformador Entity

```python
from domain.transformador import (
    Transformador,
    CodigoTransformador,
    NomeTransformador,
    TensaoTipo,
)
from domain.comum.value_objects import Localizacao, Potencia

# Create value objects
codigo = CodigoTransformador("TRANS001")
nome = NomeTransformador("Transformador Centro")
potencia = Potencia(kva=300.0)
localizacao = Localizacao(latitude=-23.5505, longitude=-46.6333)
tipo_tensao = TensaoTipo("Média")

# Create entity
transformador = Transformador(
    id=1,
    codigo=codigo,
    nome=nome,
    potencia=potencia,
    localizacao=localizacao,
    tipo_tensao=tipo_tensao,
    subestacao_codigo="SUB001",
    distribuidora="AES Eletropaulo"
)
```

### Using Repository (Infrastructure)

```python
from infrastructure.persistence.transformador_repository import SQLAlchemyTransformadorRepository
from sqlalchemy import create_engine

engine = create_engine("postgresql://...")
repository = SQLAlchemyTransformadorRepository(engine)

# Get transformer from database
transformador = repository.obter_por_id(1)
# Returns: Transformador | None

# List transformers
transformadores = repository.listar_todos(limite=100, pagina=0)
# Returns: List[Transformador]
```

### Using Use Cases (Application)

```python
from application.transformador import (
    ObtenerTransformadorUseCase,
    ListarTransformadoresUseCase,
)

# Initialize use case with repository
obter_use_case = ObtenerTransformadorUseCase(repository)

# Execute business logic
try:
    transformador = obter_use_case.execute(1)
    print(f"Found: {transformador.nome}")
except TransformadorNotFoundError as e:
    print(f"Error: {e.message}")
```

### Mapping Entity to DTO (for API response)

```python
from infrastructure.mappers.transformador_mapper import TransformadorMapper

# Convert domain entity to API response DTO
list_response = TransformadorMapper.to_list_response(transformador)
detail_response = TransformadorMapper.to_detail_response(transformador, area_geojson)

# Serialize to JSON (Pydantic handles this)
return detail_response
```

---

## 📌 Key Classes

### Domain Layer

| Class | Location | Purpose |
|-------|----------|---------|
| `Transformador` | `domain/transformador/entity.py` | Aggregate root - main business entity |
| `Localizacao` | `domain/comum/value_objects.py` | Geographic coordinates (immutable) |
| `Potencia` | `domain/comum/value_objects.py` | Electrical power in kVA (immutable) |
| `CodigoTransformador` | `domain/transformador/value_objects.py` | ANEEL code (immutable) |
| `TensaoTipo` | `domain/transformador/value_objects.py` | Voltage type validation |
| `ITransformadorRepository` | `domain/transformador/repository_interface.py` | Repository contract (interface) |
| `TransformadorNotFoundError` | `domain/transformador/errors.py` | Business logic error |

### Application Layer

| Class | Location | Purpose |
|-------|----------|---------|
| `ObtenerTransformadorUseCase` | `application/transformador/use_cases.py` | Get single transformer |
| `ListarTransformadoresUseCase` | `application/transformador/use_cases.py` | List all transformers |
| `ListarTransformadoresPorSubestacaoUseCase` | `application/transformador/use_cases.py` | Filter by substation |
| `ListarTransformadoresPorDistribuidoraUseCase` | `application/transformador/use_cases.py` | Filter by distributor |
| `ObtenerAreaCoberturaUseCase` | `application/transformador/use_cases.py` | Get coverage area |

### Infrastructure Layer

| Class | Location | Purpose |
|-------|----------|---------|
| `SQLAlchemyTransformadorRepository` | `infrastructure/persistence/transformador_repository.py` | Database access implementation |
| `TransformadorMapper` | `infrastructure/mappers/transformador_mapper.py` | Entity → DTO conversion |

### API Layer

| Class | Location | Purpose |
|-------|----------|---------|
| `TransformadorListResponse` | `schemas/transformador.py` | API list response DTO |
| `TransformadorDetailResponse` | `schemas/transformador.py` | API detail response DTO |

---

## 🔍 Value Object Validation

### Localizacao

```python
try:
    loc = Localizacao(latitude=91, longitude=0)  # ❌ Invalid
except ValueError as e:
    print(f"Error: {e}")  # "Invalid latitude: 91. Must be between -90 and 90."

# ✅ Valid
loc = Localizacao(latitude=-23.5505, longitude=-46.6333)
print(loc.latitude, loc.longitude)
```

### Potencia

```python
try:
    pot = Potencia(kva=-100)  # ❌ Invalid
except ValueError as e:
    print(f"Error: {e}")  # "Power must be positive. Got: -100 kVA"

# ✅ Valid
pot = Potencia(kva=300)
print(pot.kva)    # 300.0
print(pot.mva)    # 0.3 (converted)
print(pot.w)      # 300000.0 (converted)
```

### TensaoTipo

```python
try:
    tipo = TensaoTipo("Invalido")  # ❌ Invalid
except ValueError as e:
    print(f"Error: {e}")

# ✅ Valid types: "Alta", "Média", "Baixa", "Extra Alta"
tipo = TensaoTipo("Média")
```

---

## 🧪 Testing Patterns

### Test Domain Entity

```python
def test_create_transformador():
    """Test valid entity creation."""
    transformador = Transformador(
        id=1,
        codigo=CodigoTransformador("T001"),
        # ...
    )
    assert transformador.id == 1

def test_invalid_potencia():
    """Test validation in entity."""
    with pytest.raises(ValueError):
        Potencia(kva=-100)
```

### Test Use Case

```python
def test_obter_transformador_use_case(mock_repository):
    """Test use case with mocked repository."""
    mock_repository.obter_por_id.return_value = Transformador(...)
    use_case = ObtenerTransformadorUseCase(mock_repository)
    
    result = use_case.execute(1)
    
    assert result.id == 1
    mock_repository.obter_por_id.assert_called_once_with(1)
```

### Test API Endpoint

```python
def test_get_transformador_api(client):
    """Test HTTP endpoint."""
    response = client.get("/api/v1/transformadores/1")
    
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["codigo"] == "TRANS001"
```

---

## 🔗 Dependency Injection Flow

```
FastAPI Request
    ↓
get_transformador_service()
    ↓
get_repository()  → SQLAlchemyTransformadorRepository(engine)
    ↓
ObtenerTransformadorUseCase(repository)
    ↓
Use case executes: repository.obter_por_id(id)
    ↓
SQL query returns row
    ↓
_map_row_to_entity() → Transformador domain entity
    ↓
API endpoint receives Transformador
    ↓
TransformadorMapper.to_detail_response()
    ↓
Pydantic validates TransformadorDetailResponse
    ↓
JSON response
```

---

## 📊 Comparison: Old vs New API

### Old Endpoint

```python
@router.get("/{id}")
def get_transformador_detalhes(id: int, service: TransformadorService = Depends(...)):
    trans = service.obter_detalhes(id)
    if not trans:
        raise HTTPException(status_code=404, ...)
    return {"status": "success", "data": trans}
```

### New Endpoint

```python
@router.get("/{id}", response_model=TransformadorDetailResponse)
def get_transformador_detail(
    id: int,
    use_case: ObtenerTransformadorUseCase = Depends(get_obter_transformador_use_case),
    area_use_case: ObtenerAreaCoberturaUseCase = Depends(get_obter_area_cobertura_use_case)
):
    try:
        transformador = use_case.execute(id)
        area_geojson = area_use_case.execute(id)
        return TransformadorMapper.to_detail_response(transformador, area_geojson)
    except TransformadorNotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message)
```

**Benefits:**
- ✅ Explicit response model (Pydantic validation)
- ✅ Separated concerns (use case, mapper, error handling)
- ✅ Type-safe (returns specific DTO type)
- ✅ Testable (can mock use case separately)
- ✅ Domain errors converted to HTTP exceptions

---

## 🎯 Common Operations

### Get transformer by ID

```python
repository = SQLAlchemyTransformadorRepository(engine)
use_case = ObtenerTransformadorUseCase(repository)

try:
    transformador = use_case.execute(1)
    print(f"Name: {transformador.nome}")
    print(f"Power: {transformador.potencia}")
except TransformadorNotFoundError:
    print("Transformer not found")
```

### List transformers

```python
use_case = ListarTransformadoresUseCase(repository)
transformadores, total = use_case.execute(limite=100, pagina=0)
print(f"Found {total} transformers, showing {len(transformadores)}")
```

### Change transformer status

```python
# Get transformer
transformador = repository.obter_por_id(1)

# Business operation (creates new instance)
transformador_inativo = transformador.muda_status_ativacao(ativo=False)
# Note: Area coverage is automatically removed when deactivating

# To persist back to database, you would need a save method
# (not implemented yet - future enhancement)
```

### Associate coverage area

```python
area = AreaCobertura(
    geojson='{"type": "Polygon", "coordinates": [...]}',
    wkt="POLYGON(...)"
)

transformador_com_area = transformador.associa_area_cobertura(area)
```

---

## ⚠️ Important Notes

1. **Immutability**: Domain entities use dataclasses with methods that return new instances
2. **Validation**: All validation happens in Value Objects and Entity constructors
3. **No Getters**: Direct attribute access is allowed (no getters needed)
4. **Error Handling**: Domain errors should be caught by application layer and converted to HTTP exceptions
5. **Database Persistence**: Currently entities don't have built-in save methods - use repository interface

---

## 📞 Troubleshooting

### ModuleNotFoundError

```python
# ❌ Wrong
from src.domain.transformador import Transformador

# ✅ Correct (relative imports)
from domain.transformador import Transformador
```

### Import Errors in Tests

```python
# ✅ Add src to path
import sys
sys.path.insert(0, '/path/to/backend/src')

from domain.transformador import Transformador
```

### Repository Returns None

```python
transformador = repository.obter_por_id(999)  # Returns None if not found

# Always check before use
if transformador:
    print(transformador.nome)
else:
    # Handle not found
    raise TransformadorNotFoundError(999)
```
