# Domain-Driven Design (DDD) Implementation - Transformador Module

## 📚 Overview

This document describes the complete DDD implementation for the Transformador (Transformer) bounded context in the Energy Netload Monitor project. The refactoring provides a clear separation of concerns, improved testability, and better maintainability.

---

## 🏗️ Architecture Structure

### New Directory Organization

```
backend/src/
│
├── domain/                          # 🎯 CORE BUSINESS LOGIC
│   ├── comum/
│   │   ├── errors.py               # Base DomainError class
│   │   ├── value_objects.py        # Reusable VOs: Localizacao, Potencia, Temperatura
│   │   └── __init__.py
│   │
│   └── transformador/              # Transformador bounded context
│       ├── entity.py               # Transformador aggregate root
│       ├── value_objects.py        # Domain-specific VOs
│       ├── errors.py               # Domain-specific exceptions
│       ├── repository_interface.py # Repository contract (ITransformadorRepository)
│       └── __init__.py
│
├── application/                     # 🔄 USE CASES / APPLICATION SERVICES
│   └── transformador/
│       ├── use_cases.py            # Business operation workflows
│       └── __init__.py
│
├── infrastructure/                  # ⚙️ TECHNICAL IMPLEMENTATIONS
│   ├── persistence/
│   │   ├── transformador_repository.py  # SQLAlchemy implementation
│   │   └── __init__.py
│   │
│   └── mappers/
│       ├── transformador_mapper.py  # Entity ↔ DTO conversion
│       └── __init__.py
│
├── api/                            # 🌐 HTTP LAYER
│   ├── transformadores_v2.py       # DDD-based endpoints
│   └── ...
│
└── schemas/                        # 📤 DTOs / API CONTRACTS
    ├── transformador.py
    └── ...
```

---

## 🔑 Key Concepts

### 1. **Domain Layer** (`domain/`)

Contains pure business logic with NO external dependencies.

#### Value Objects (Imutable objects representing concepts)

```python
# domain/comum/value_objects.py
@dataclass(frozen=True)
class Localizacao(ValueObject):
    """Geographic location - validates coordinates."""
    latitude: float
    longitude: float

@dataclass(frozen=True)
class Potencia(ValueObject):
    """Electrical power - validates positive value."""
    kva: float

# domain/transformador/value_objects.py
@dataclass(frozen=True)
class CodigoTransformador(ValueObject):
    """ANEEL transformer code."""
    valor: str
```

**Why Value Objects?**
- Encapsulate validation logic
- Prevent invalid states at creation
- Self-documenting code
- Easy to test

#### Aggregate Root (Entity with business logic)

```python
# domain/transformador/entity.py
@dataclass
class Transformador:
    """Aggregate root for transformer bounded context."""
    id: int
    codigo: CodigoTransformador
    nome: NomeTransformador
    potencia: Potencia
    localizacao: Localizacao
    tipo_tensao: TensaoTipo
    # ... other attributes
    
    def muda_status_ativacao(self, ativo: bool) -> "Transformador":
        """Business operation: change activation status."""
        novo_transformador = Transformador(...)
        return novo_transformador
    
    def associa_area_cobertura(self, area: AreaCobertura) -> "Transformador":
        """Business operation: associate coverage area."""
        if not self.ativo:
            raise InvalidTransformadorError(...)
        novo_transformador = Transformador(...)
        return novo_transformador
```

**Why Aggregates?**
- Encapsulate all related business logic
- Ensure consistency of data
- Clear boundaries of responsibility
- Easy to reason about

#### Domain-Specific Exceptions

```python
# domain/transformador/errors.py
class TransformadorError(DomainError):
    """Base exception for all transformer errors."""

class TransformadorNotFoundError(TransformadorError):
    """Raised when transformer not found."""

class InvalidTransformadorError(TransformadorError):
    """Raised when transformer data is invalid."""
```

#### Repository Interface (High-level abstraction)

```python
# domain/transformador/repository_interface.py
class ITransformadorRepository(ABC):
    """Contract for transformer persistence."""
    
    @abstractmethod
    def obter_por_id(self, id: int) -> Optional[Transformador]:
        """Get transformer by ID."""
        
    @abstractmethod
    def listar_todos(self, limite: int, pagina: int) -> List[Transformador]:
        """List all transformers."""
```

**Why repository interfaces in domain?**
- Inversion of Control: application defines what it needs
- Infrastructure implements the contract
- Easy to swap implementations (SQL, NoSQL, APIs, etc.)

---

### 2. **Application Layer** (`application/`)

Orchestrates domain entities and implements use cases.

```python
# application/transformador/use_cases.py
class ObtenerTransformadorUseCase:
    """Use case: Get a single transformer."""
    
    def __init__(self, repository: ITransformadorRepository):
        self.repository = repository
    
    def execute(self, transformador_id: int) -> Transformador:
        """Execute the use case."""
        transformador = self.repository.obter_por_id(transformador_id)
        if not transformador:
            raise TransformadorNotFoundError(transformador_id)
        return transformador

class ListarTransformadoresUseCase:
    """Use case: List transformers with pagination."""
    
    def execute(self, limite: int, pagina: int) -> tuple:
        transformadores = self.repository.listar_todos(limite, pagina)
        total = self.repository.contar_total()
        return transformadores, total
```

**Why separate use cases?**
- One use case = one business workflow
- Easy to test in isolation
- Clear naming of business operations
- Reusable across different interfaces (REST, GraphQL, gRPC, etc.)

---

### 3. **Infrastructure Layer** (`infrastructure/`)

Technical implementations (database, external APIs, mappers).

#### Repository Implementation

```python
# infrastructure/persistence/transformador_repository.py
class SQLAlchemyTransformadorRepository(BaseRepository, ITransformadorRepository):
    """SQLAlchemy implementation of transformer persistence."""
    
    def obter_por_id(self, id: int) -> Optional[Transformador]:
        """Query database and map to domain entity."""
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT id, codigo, nome, ...
                FROM transformadores_aneel
                WHERE id = :id
            """), {"id": id})
            
            row = result.fetchone()
            if not row:
                return None
            
            # Map raw database row to domain entity
            return self._map_row_to_entity(row)
    
    def _map_row_to_entity(self, row) -> Transformador:
        """Convert database row to domain entity."""
        return Transformador(
            id=row[0],
            codigo=CodigoTransformador(row[1]),
            nome=NomeTransformador(row[2]),
            potencia=Potencia(row[5]),
            localizacao=Localizacao(row[3], row[4]),
            # ...
        )
```

#### Mapper (DTO conversion)

```python
# infrastructure/mappers/transformador_mapper.py
class TransformadorMapper:
    """Maps domain entities to API response DTOs."""
    
    @staticmethod
    def to_list_response(t: Transformador) -> TransformadorListResponse:
        """Convert to list response DTO."""
        return TransformadorListResponse(
            id=t.id,
            codigo=str(t.codigo),
            nome=str(t.nome),
            potencia_kva=t.potencia.kva,
            # ...
        )
```

---

### 4. **API Layer** (`api/`)

HTTP endpoints using dependency injection and use cases.

```python
# api/transformadores_v2.py
router = APIRouter(prefix="/api/v1/transformadores")

def get_repository() -> ITransformadorRepository:
    """DI: Provide repository."""
    return SQLAlchemyTransformadorRepository(get_engine())

@router.get("/{id}", response_model=TransformadorDetailResponse)
def get_transformador_detail(
    id: int,
    use_case: ObtenerTransformadorUseCase = Depends(get_obter_transformador_use_case)
):
    """Get transformer details."""
    try:
        transformador = use_case.execute(id)
        return TransformadorMapper.to_detail_response(transformador)
    except TransformadorNotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message)
```

---

## 🔄 Data Flow

### Request → Response Flow

```
1. HTTP Request
   ↓
2. API Endpoint (api/transformadores_v2.py)
   - Validate input
   - Inject dependencies (use case)
   ↓
3. Use Case (application/use_cases.py)
   - Orchestrate business logic
   - Validate business rules
   ↓
4. Domain Entity & Repository (domain/ + infrastructure/persistence/)
   - Apply domain logic
   - Query database
   ↓
5. Infrastructure Repository (infrastructure/persistence/)
   - Execute SQL
   - Map Row → Domain Entity
   ↓
6. Mapper (infrastructure/mappers/)
   - Convert Entity → DTO
   ↓
7. API Schema (schemas/transformador.py)
   - Validate DTO with Pydantic
   - Serialize to JSON
   ↓
8. HTTP Response
```

---

## ✨ Benefits of This Implementation

| Aspect | Before (Monolithic) | After (DDD) |
|--------|---------------------|------------|
| **Testability** | Hard to test services in isolation | Easy - mock repository interface |
| **Business Logic** | Scattered in services | Centralized in domain entities |
| **Maintainability** | Changes ripple across layers | Isolated changes per layer |
| **Reusability** | Limited to services | High - use cases work with any interface |
| **Domain Knowledge** | Hidden in code | Explicit in entities and value objects |
| **New Developers** | Hard to understand | Clear flow: domain → app → infra → api |
| **Database Changes** | Requires code refactoring | Mapper handles conversion |
| **API Changes** | Requires use case changes | Only mapper needs updating |

---

## 🧪 Testing Examples

### Testing Domain Entity

```python
# tests/domain/test_transformador_entity.py
def test_transformador_invalid_potencia():
    """Test validation in domain entity."""
    with pytest.raises(ValueError):
        Potencia(kva=-100)  # Negative power invalid

def test_transformador_muda_status():
    """Test business operation."""
    trans = Transformador(
        id=1,
        codigo=CodigoTransformador("T001"),
        potencia=Potencia(100),
        # ...
    )
    
    novo = trans.muda_status_ativacao(ativo=False)
    assert novo.ativo == False
    assert novo.area_cobertura is None  # Area removed when inactive
```

### Testing Use Case

```python
# tests/application/test_transformador_use_cases.py
def test_obter_transformador(mock_repository):
    """Test use case with mocked repository."""
    use_case = ObtenerTransformadorUseCase(mock_repository)
    
    mock_repository.obter_por_id.return_value = Transformador(...)
    
    result = use_case.execute(1)
    
    assert result.id == 1
    mock_repository.obter_por_id.assert_called_once_with(1)

def test_obter_transformador_not_found(mock_repository):
    """Test error handling."""
    use_case = ObtenerTransformadorUseCase(mock_repository)
    mock_repository.obter_por_id.return_value = None
    
    with pytest.raises(TransformadorNotFoundError):
        use_case.execute(999)
```

### Testing API Endpoint

```python
# tests/api/test_transformadores_v2.py
def test_get_transformador_endpoint(client):
    """Test HTTP endpoint."""
    response = client.get("/api/v1/transformadores/1")
    
    assert response.status_code == 200
    assert response.json()["data"]["codigo"] == "TRANS001"

def test_get_transformador_not_found(client):
    """Test 404 response."""
    response = client.get("/api/v1/transformadores/999")
    
    assert response.status_code == 404
```

---

## 📝 Migration Guide

### Old Code (Monolithic)

```python
# Before: Service with mixed responsibilities
class TransformadorService:
    def __init__(self, engine):
        self.engine = engine
    
    def obter_detalhes(self, id):
        # Query database
        # Format response
        # Handle errors
        pass

# Before: API directly using service
@app.get("/transformadores/{id}")
def get_trans(id, service=Depends(get_transformador_service)):
    result = service.obter_detalhes(id)
    return result
```

### New Code (DDD)

```python
# After: Domain entity with business logic
class Transformador:
    # Validation in __post_init__
    # Business operations as methods
    pass

# After: Use case orchestrating domain
class ObtenerTransformadorUseCase:
    def execute(self, id) -> Transformador:
        transformador = repository.obter_por_id(id)
        return transformador

# After: API clean and focused
@app.get("/{id}", response_model=TransformadorDetailResponse)
def get_trans(id, use_case: ObtenerTransformadorUseCase = Depends(...)):
    transformador = use_case.execute(id)
    return TransformadorMapper.to_detail_response(transformador)
```

---

## 🚀 Next Steps

To complete DDD for other entities:

1. **Subestacao Bounded Context**
   - [ ] Create `domain/subestacao/` with entity, value objects, errors
   - [ ] Create repository interface
   - [ ] Implement `infrastructure/persistence/subestacao_repository.py`
   - [ ] Create `application/subestacao/` use cases
   - [ ] Implement mapper
   - [ ] Create/refactor API endpoints

2. **PainelSolar Bounded Context**
   - Follow same pattern as Transformador

3. **Telhado Bounded Context**
   - Follow same pattern as Transformador

---

## 📖 References

- Domain-Driven Design (Eric Evans)
- Clean Architecture (Robert C. Martin)
- Patterns of Enterprise Application Architecture (Martin Fowler)
- CQRS Pattern (optional: for read/write separation)

---

## 📞 Questions & Support

For implementation questions:
- Refer to `domain/transformador/` as the reference implementation
- Check `tests/` directory for testing patterns
- Review `infrastructure/mappers/` for entity-DTO conversion patterns
