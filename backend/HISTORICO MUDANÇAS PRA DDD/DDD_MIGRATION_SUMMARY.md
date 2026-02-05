"""
Solar Panel Service - DDD Migration Summary

This document describes the Domain-Driven Design refactoring of the solar panel detection service.

## Architecture Overview

The service has been reorganized into three main layers:

### 1. Domain Layer (backend/src/domain/painel_solar/)

**Purpose**: Contains business logic and domain entities that are independent of any framework.

**Files**:
- `entity.py`: Domain entities and value objects
  - `PropertyType`: Enum for property classification (RESIDENCIAL, COMERCIAL, INDUSTRIAL)
  - `BoundingBox`: Value object for panel location
  - `Centroide`: Value object for panel centroid
  - `PainelSolar`: Domain entity representing a detected solar panel
  - `EstimativaPotencia`: Domain entity for power estimation
  - `PropertyClassification`: Value object for classification result

- `dto.py`: Data Transfer Objects for inter-layer communication
  - `PainelSolarDTO`: Transfer panel detection data
  - `EstimativaPotenciaDTO`: Transfer power estimation
  - `PropertyClassificationDTO`: Transfer classification result
  - `DetectionResultDTO`: Complete pipeline result
  - `PowerEstimationRequestDTO`: Request for power estimation
  - `PowerEstimationResponseDTO`: Response with detailed calculations

**Benefits**:
- Pure business logic independent of infrastructure
- Immutable value objects for consistency
- Clear contracts for data transfer between layers
- Easy to test and reason about


### 2. Application Layer (backend/src/application/painel_solar/)

**Purpose**: Orchestrates domain logic, manages use cases, and coordinates between layers.

**Files**:
- `service.py`: PainelSolarApplicationService
  - `detectar_paineis_em_url()`: Download and detect panels from URL
  - `classificar_propriedade()`: Classify property type
  - `estimar_potencia()`: Estimate power capacity
  - `processar_telhado_completo()`: Complete processing pipeline
  - `classificar_e_estimar_completo()`: Full analysis workflow

- `use_cases.py`: Domain-driven use cases
  - `DetectarPainelSolarUseCase`: Panel detection logic
  - `ClassificarPropriedadeUseCase`: Property classification logic
  - `EstimarPotenciaInstalacaoUseCase`: Power estimation logic
  - `EstimarProducaoAnualUseCase`: Annual production calculation
  - `PipelineCompleteDetectionUseCase`: Orchestrated pipeline

**Benefits**:
- Clear separation of concerns
- Reusable use cases for different client needs
- Testable business logic
- Easy to extend with new workflows


### 3. Infrastructure Layer (backend/src/infrastructure/ml/)

**Purpose**: Implements technical concerns like ML detection and external service integration.

**Files**:
- `solar_panel_detection_service.py`:
  - `SolarPanelDetectionService`: YOLO model loading and inference
  - `PropertyClassifier`: Classification algorithms
  - `PowerEstimator`: Power calculation algorithms

**Benefits**:
- ML logic isolated from business logic
- Easy to swap implementations (e.g., different models)
- Technical details hidden from domain and application layers


## Key Improvements

### 1. Layering
Before: All logic mixed in `SolarPanelService` (monolithic)
After: Clear separation into domain → application → infrastructure

### 2. Testability
- Domain entities are pure, no external dependencies
- Use cases can be tested with mocks
- Infrastructure can be tested independently

### 3. Maintainability
- Easier to locate functionality
- Clear responsibility for each layer
- Reduced coupling between components

### 4. Extensibility
- Add new use cases without modifying existing code
- Support different clients (API, batch processing, etc.)
- Easy to add new classification or estimation algorithms

### 5. Reusability
- Use cases can be composed in different ways
- Domain logic is framework-agnostic
- Infrastructure implementations are interchangeable


## Migration Path

**Old Code** → **New Code**:
```python
# Old: All-in-one service
from src.services.solar_panel_service import SolarPanelService

service = SolarPanelService()
result = service.processar_telhado(url, bbox)

# New: Application service (recommended for most use cases)
from src.application.painel_solar import PainelSolarApplicationService

service = PainelSolarApplicationService()
result = service.processar_telhado_completo(url, bbox)

# Or use specific use cases (for advanced workflows)
from src.application.painel_solar.use_cases import (
    DetectarPainelSolarUseCase,
    EstimarPotenciaInstalacaoUseCase,
)
from src.infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PowerEstimator,
)

detection_service = SolarPanelDetectionService()
estimator = PowerEstimator()

detect_uc = DetectarPainelSolarUseCase(detection_service)
estimate_uc = EstimarPotenciaInstalacaoUseCase(estimator)

paineis = detect_uc.executar(url, bbox)
potencia = estimate_uc.executar(paineis)
```


## File Structure

```
backend/src/
├── domain/painel_solar/
│   ├── __init__.py           (exports)
│   ├── entity.py             (domain entities)
│   └── dto.py                (data transfer objects)
│
├── application/painel_solar/
│   ├── __init__.py           (exports)
│   ├── service.py            (application service)
│   └── use_cases.py          (use cases)
│
└── infrastructure/ml/
    ├── __init__.py
    └── solar_panel_detection_service.py  (ML infrastructure)
```


## Dependency Injection Pattern

The DDD architecture supports dependency injection:

```python
# Option 1: Direct instantiation
detection_service = SolarPanelDetectionService(modelo_yolo_path)
app_service = PainelSolarApplicationService(modelo_yolo_path)

# Option 2: Use cases with injected dependencies
from infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PowerEstimator,
)
from application.painel_solar.use_cases import (
    DetectarPainelSolarUseCase,
    EstimarPotenciaInstalacaoUseCase,
)

detection_svc = SolarPanelDetectionService()
estimator = PowerEstimator()

detect_uc = DetectarPainelSolarUseCase(detection_svc)
power_uc = EstimarPotenciaInstalacaoUseCase(estimator)

# Use injected instances
paineis = detect_uc.executar(url, bbox)
potencia = power_uc.executar(paineis)
```


## Next Steps

1. **Update API Layer**: Modify endpoints to use new `PainelSolarApplicationService`
2. **Update Dependencies**: Check `transformador_pipeline_service.py` for SolarPanelService usage
3. **Add Repository Layer** (Optional): If persistence is needed, add:
   - `backend/src/domain/painel_solar/repository.py`
   - `backend/src/infrastructure/repositories/painel_solar_repository.py`
4. **Add Event Publishing** (Optional): For cross-domain communication:
   - Domain events in domain layer
   - Event handlers in application layer


## Quality Assurance

### Testing Strategy

```python
# Unit test: Domain entity
def test_painel_solar_creation():
    painel = PainelSolar(
        id_painel="p1",
        bbox=BoundingBox(x=0, y=0, w=100, h=100),
        centroide=Centroide(x=50, y=50),
        area_pixeis=10000,
        area_m2=0.9,
        confianca=0.95,
        tipo_painel="monocristalino"
    )
    assert painel.confianca == 0.95

# Unit test: Use case with mocks
def test_detect_use_case():
    mock_service = Mock(spec=SolarPanelDetectionService)
    mock_service.detectar_paineis.return_value = [mock_painel]
    
    uc = DetectarPainelSolarUseCase(mock_service)
    result = uc.executar(url, bbox)
    
    assert len(result) == 1

# Integration test: Application service
def test_application_service():
    service = PainelSolarApplicationService()
    result = service.processar_telhado_completo(url, bbox)
    
    assert result.sucesso
    assert len(result.paineis) > 0
```


## Backward Compatibility

The old `SolarPanelService` class is maintained at `backend/src/services/solar_panel_service.py` 
for backward compatibility with existing code that depends on it.

**Migration Priority**:
1. High: Core business logic code
2. Medium: API endpoints
3. Low: Test utilities and helpers


## Performance Considerations

- Domain entities use `frozen=True` dataclasses for immutability
- Value objects prevent invalid state creation
- Validation happens at entity instantiation (fail-fast principle)
- No performance overhead compared to original implementation
- Same ML inference speed (YOLO model unchanged)


## References

- Similar DDD implementation: `backend/src/application/telhado_detection/service.py`
- Domain layer examples: `backend/src/domain/`
- Application layer examples: `backend/src/application/`
"""

# Example usage
if __name__ == "__main__":
    # Application service (recommended)
    from backend.src.application.painel_solar import PainelSolarApplicationService
    
    service = PainelSolarApplicationService()
    result = service.processar_telhado_completo(
        url_imagem="https://maps.googleapis.com/...",
        bbox={"x": 0, "y": 0, "w": 512, "h": 512}
    )
    
    print(f"Success: {result.sucesso}")
    print(f"Panels detected: {len(result.paineis)}")
    if result.estimativa_potencia:
        print(f"Power: {result.estimativa_potencia.potencia_instalada_kw} kW")
    print(f"Time: {result.tempo_processamento_s:.2f}s")
