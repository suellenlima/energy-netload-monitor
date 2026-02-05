# DDD Implementation Checklist & Next Steps

## ✅ Phase 1: Transformador (COMPLETED)

### Domain Layer
- [x] Create `domain/comum/errors.py` - Base DomainError
- [x] Create `domain/comum/value_objects.py` - Localizacao, Potencia, Temperatura
- [x] Create `domain/transformador/entity.py` - Transformador aggregate root
- [x] Create `domain/transformador/value_objects.py` - Domain-specific VOs
- [x] Create `domain/transformador/errors.py` - Domain exceptions
- [x] Create `domain/transformador/repository_interface.py` - ITransformadorRepository

### Application Layer
- [x] Create `application/transformador/use_cases.py` - All 5 use cases
- [x] Use cases validate input and orchestrate domain + repository
- [x] Use cases return domain entities (not DTOs)

### Infrastructure Layer
- [x] Create `infrastructure/persistence/transformador_repository.py` - SQLAlchemy implementation
- [x] Map database rows to domain entities
- [x] Create `infrastructure/mappers/transformador_mapper.py` - Entity → DTO conversion

### API & Schemas
- [x] Create `api/transformadores_v2.py` - DDD-based endpoints
- [x] Create `schemas/transformador.py` - Response DTOs
- [x] Implement dependency injection for all endpoints
- [x] Convert domain errors to HTTP exceptions

### Documentation
- [x] ANALISE_DDD.md - Detailed analysis
- [x] IMPLEMENTACAO_DDD_TRANSFORMADOR.md - Complete guide
- [x] DDD_QUICK_REFERENCE.md - Quick reference
- [x] DDD_IMPLEMENTATION_SUMMARY.md - Summary

**Status**: ✅ READY FOR PRODUCTION

---

## 📋 Phase 2: Subestacao (TODO)

### Preparation
- [ ] Read `IMPLEMENTACAO_DDD_TRANSFORMADOR.md` as reference
- [ ] Review Subestacao current structure in `services/` and `repositories/`
- [ ] Identify domain concepts and value objects specific to Subestacao

### Domain Layer
- [ ] Create `domain/subestacao/entity.py` - Subestacao aggregate root
  - Analyze: What fields should be in Subestacao?
  - What business operations should be methods?
  - What validation rules exist?
  
- [ ] Create `domain/subestacao/value_objects.py` - Domain-specific VOs
  - Examples: CodigoSubestacao, NomeSubestacao, TensaoOperacao, etc.
  
- [ ] Create `domain/subestacao/errors.py` - Domain exceptions
  - SubestacaoError, SubestacaoNotFoundError, InvalidSubestacaoError
  
- [ ] Create `domain/subestacao/repository_interface.py` - ISubestacaoRepository
  - Methods: obter_por_id, obter_por_codigo, listar_todos, listar_por_distribuidora, etc.

### Application Layer
- [ ] Create `application/subestacao/use_cases.py` - All use cases
  - ObtenerSubestacao
  - ListarSubestacoes
  - ListarSubestacoesPorDistribuidora
  - ListarSubestacoesPorTensao
  - ObtenerTransformadoresDaSubestacao (if applicable)

### Infrastructure Layer
- [ ] Create `infrastructure/persistence/subestacao_repository.py`
  - Implement ISubestacaoRepository
  - Map rows to Subestacao entities
  
- [ ] Create `infrastructure/mappers/subestacao_mapper.py`
  - to_list_response()
  - to_detail_response()

### API & Schemas
- [ ] Create `api/subestacoes_v2.py` - New DDD-based endpoints
- [ ] Create `schemas/subestacao.py` - Response DTOs (if new structure needed)
- [ ] Replace or keep old API for backwards compatibility

**Estimated Time**: 3-4 hours

---

## 📋 Phase 3: PainelSolar (TODO)

### Preparation
- [ ] Review PainelSolar current structure
- [ ] Identify domain concepts

### Domain Layer
- [ ] Create `domain/painel_solar/entity.py`
- [ ] Create `domain/painel_solar/value_objects.py`
- [ ] Create `domain/painel_solar/errors.py`
- [ ] Create `domain/painel_solar/repository_interface.py`

### Application Layer
- [ ] Create `application/painel_solar/use_cases.py`

### Infrastructure Layer
- [ ] Create `infrastructure/persistence/painel_solar_repository.py`
- [ ] Create `infrastructure/mappers/painel_solar_mapper.py`

### API & Schemas
- [ ] Create `api/painel_solar_v2.py`
- [ ] Create/update `schemas/painel_solar.py`

**Estimated Time**: 3-4 hours

---

## 📋 Phase 4: Telhado (TODO)

### Follow same pattern as PainelSolar
- [ ] Domain layer (entity, VOs, errors, interface)
- [ ] Application layer (use cases)
- [ ] Infrastructure layer (repository, mapper)
- [ ] API & Schemas

**Estimated Time**: 3-4 hours

---

## 🎓 Step-by-Step Guide for Phase 2 (Subestacao)

### Step 1: Analyze Current Implementation
```bash
# Review current code
less src/services/subestacoes_service.py
less src/repositories/subestacao_repository.py
```

**Questions to answer:**
- What fields does Subestacao have?
- What validation rules exist?
- What operations are performed on Subestacao?

### Step 2: Create Domain Entity
```python
# domain/subestacao/entity.py
from dataclasses import dataclass
from domain.comum.value_objects import Localizacao

@dataclass
class Subestacao:
    id: int
    codigo: CodigoSubestacao
    nome: NomeSubestacao
    localizacao: Localizacao
    tensao: TensaoOperacao
    # Add other fields
    
    def __post_init__(self):
        self._validar_invariantes()
    
    def _validar_invariantes(self):
        # Add business rule validations
        pass
```

### Step 3: Create Value Objects
```python
# domain/subestacao/value_objects.py
@dataclass(frozen=True)
class CodigoSubestacao(ValueObject):
    valor: str
    
    def __post_init__(self):
        if not self.valor or len(self.valor) > 20:
            raise ValueError("Invalid substation code")
```

### Step 4: Create Domain Errors
```python
# domain/subestacao/errors.py
class SubestacaoError(DomainError):
    pass

class SubestacaoNotFoundError(SubestacaoError):
    pass
```

### Step 5: Create Repository Interface
```python
# domain/subestacao/repository_interface.py
class ISubestacaoRepository(ABC):
    @abstractmethod
    def obter_por_id(self, id: int) -> Optional[Subestacao]:
        pass
```

### Step 6: Implement Repository
```python
# infrastructure/persistence/subestacao_repository.py
class SQLAlchemySubestacaoRepository(BaseRepository, ISubestacaoRepository):
    def obter_por_id(self, id: int) -> Optional[Subestacao]:
        # Query database
        # Map row to Subestacao entity
        pass
```

### Step 7: Create Use Cases
```python
# application/subestacao/use_cases.py
class ObtenerSubestacaoUseCase:
    def __init__(self, repository: ISubestacaoRepository):
        self.repository = repository
    
    def execute(self, id: int) -> Subestacao:
        subestacao = self.repository.obter_por_id(id)
        if not subestacao:
            raise SubestacaoNotFoundError(id)
        return subestacao
```

### Step 8: Create Mapper
```python
# infrastructure/mappers/subestacao_mapper.py
class SubestacaoMapper:
    @staticmethod
    def to_detail_response(s: Subestacao) -> SubestacaoDetailResponse:
        return SubestacaoDetailResponse(
            id=s.id,
            codigo=str(s.codigo),
            # ...
        )
```

### Step 9: Create API Endpoints
```python
# api/subestacoes_v2.py
@router.get("/{id}")
def get_subestacao(
    id: int,
    use_case: ObtenerSubestacaoUseCase = Depends(...)
):
    try:
        subestacao = use_case.execute(id)
        return SubestacaoMapper.to_detail_response(subestacao)
    except SubestacaoNotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message)
```

### Step 10: Create Response Schemas
```python
# schemas/subestacao.py
class SubestacaoDetailResponse(BaseModel):
    id: int
    codigo: str
    nome: str
    # ... other fields
```

---

## 🔍 Code Review Checklist for Each Phase

Before merging new bounded context implementation:

### Domain Layer
- [ ] All fields are Value Objects or domain concepts
- [ ] Entity has `__post_init__` with invariant validation
- [ ] Business operations are methods returning new instances
- [ ] No SQLAlchemy imports
- [ ] No external dependencies
- [ ] Docstrings explain "why"

### Application Layer
- [ ] Each use case has single responsibility
- [ ] Use cases return domain entities (not dicts)
- [ ] Input validation happens in use cases
- [ ] Domain errors are not caught (bubble up to API)
- [ ] Docstrings for execute() method

### Infrastructure Layer
- [ ] Repository maps rows to domain entities
- [ ] No business logic in repository
- [ ] Private `_map_row_to_entity()` method
- [ ] Mapper has clear to_*_response() methods
- [ ] No HTTP concepts in mapper

### API Layer
- [ ] Dependency injection for all use cases
- [ ] Error handling converts domain to HTTP errors
- [ ] Response models are Pydantic schemas
- [ ] Endpoints use proper HTTP methods/status codes
- [ ] All endpoints have docstrings

---

## 🧪 Testing Checklist

For each new bounded context:

### Unit Tests
- [ ] `tests/domain/test_[entidade]_entity.py`
  - Test entity creation with valid data
  - Test validation in entity
  - Test business operations
  
- [ ] `tests/domain/test_[entidade]_value_objects.py`
  - Test each value object with valid/invalid data
  
- [ ] `tests/application/test_[entidade]_use_cases.py`
  - Test use case with mocked repository
  - Test error scenarios

### Integration Tests
- [ ] `tests/infrastructure/test_[entidade]_repository.py`
  - Test repository with real database
  
- [ ] `tests/api/test_[entidade]_v2.py`
  - Test HTTP endpoints
  - Test error responses

---

## 📊 Progress Tracking

### Timeline Estimate
- Phase 1 (Transformador): ✅ **COMPLETED** (16 hours)
- Phase 2 (Subestacao): ⏳ **3-4 hours**
- Phase 3 (PainelSolar): ⏳ **3-4 hours**
- Phase 4 (Telhado): ⏳ **3-4 hours**
- **Total**: ~24-28 hours for complete refactoring

### Parallel Work Opportunities
- [ ] While implementing Phase 2: Document Phase 3 patterns
- [ ] While coding: Create test templates
- [ ] Create migration guide for old code → new code

---

## 🚨 Important Reminders

1. **Don't Skip Domain Layer** - The whole benefit of DDD is in domain entities
2. **Keep Repository Interface in Domain** - That's the inversion of control
3. **Map Rows to Entities** - Repository must return domain objects, not dicts
4. **Convert Errors to HTTP** - Only API layer knows about HTTP
5. **Use Value Objects** - Even for simple fields like codes and names
6. **Validate in Entity** - `__post_init__` is where invariants are enforced
7. **Document "Why"** - Code should explain business intent

---

## 🎯 Success Criteria

✅ **Phase 2 Complete When:**
- All 4 layers implemented for Subestacao
- No import errors
- All endpoint tests pass
- Code follows Transformador pattern
- Documentation updated
- Zero direct database queries in use cases

✅ **Full Project Complete When:**
- All 4 entities refactored to DDD
- Shared domain concepts in `domain/comum/`
- Consistent patterns across all modules
- 80%+ code test coverage
- Comprehensive documentation

---

## 📞 Questions & Support

**Q: Where do I put complex queries?**
A: In repository implementation. Domain shouldn't know about queries.

**Q: Can I have multiple repositories per entity?**
A: Yes, create different implementations for different data sources.

**Q: What about transactions?**
A: Handle in repository or use application-level orchestration.

**Q: How do I handle cross-entity operations?**
A: Either in domain if entities related, or create higher-level use case.

**Q: When do I persist changes back to DB?**
A: Implement `save()` or `update()` methods in repository.

---

## 📚 Additional Resources

- `IMPLEMENTACAO_DDD_TRANSFORMADOR.md` - Full implementation guide
- `DDD_QUICK_REFERENCE.md` - Handy reference for common operations
- `ANALISE_DDD.md` - Analysis of improvements

---

**Ready to implement Phase 2? Start with analyzing Subestacao domain concepts!**
