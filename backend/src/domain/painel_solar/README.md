"""
# Solar Panel Domain Layer

This directory contains the pure domain logic for solar panel detection and analysis.
Domain entities are independent of any framework or infrastructure.

## Overview

The domain layer contains:

### Domain Entities
- **PainelSolar**: Represents a detected solar panel
- **EstimativaPotencia**: Represents power and production estimation
- **PropertyClassification**: Represents classification result

### Value Objects (Immutable)
- **PropertyType**: Enum for property classification (RESIDENCIAL, COMERCIAL, INDUSTRIAL)
- **BoundingBox**: Immutable bbox representation (x, y, w, h)
- **Centroide**: Immutable centroid representation (x, y)

### Data Transfer Objects (DTOs)
- **PainelSolarDTO**: Transfer panel data between layers
- **EstimativaPotenciaDTO**: Transfer power estimation data
- **PropertyClassificationDTO**: Transfer classification data
- **DetectionResultDTO**: Complete pipeline result

## Core Concepts

### Domain Entities

Domain entities embody business rules and constraints:

```python
from domain.painel_solar import PainelSolar, BoundingBox, Centroide

# Create a panel entity with validation
painel = PainelSolar(
    id_painel="painel_1",
    bbox=BoundingBox(x=100, y=200, w=50, h=60),
    centroide=Centroide(x=125.0, y=230.0),
    area_pixeis=3000,
    area_m2=0.27,
    confianca=0.95,  # Must be 0-1
    tipo_painel="monocristalino"
)

# Access data
print(painel.area_m2)  # 0.27 m²
print(painel.bbox.x)  # 100

# Convert to dict for serialization
data = painel.to_dict()
```

### Value Objects

Value objects are immutable and enforce constraints:

```python
from domain.painel_solar import BoundingBox, Centroide, PropertyType

# BoundingBox is frozen (immutable)
bbox = BoundingBox(x=0, y=0, w=100, h=100)
# bbox.x = 50  # TypeError: frozen dataclass

# PropertyType enum with methods
prop_type = PropertyType.COMERCIAL
print(prop_type.description)  # "Estabelecimento comercial"
print(prop_type.power_range())  # (10, 50) kW
```

### Domain Rules

The domain layer enforces business rules:

```python
from domain.painel_solar import PainelSolar

# This raises ValueError - confidence must be 0-1
try:
    invalid_painel = PainelSolar(
        id_painel="p1",
        bbox=BoundingBox(x=0, y=0, w=100, h=100),
        centroide=Centroide(x=50, y=50),
        area_pixeis=10000,
        area_m2=0.9,
        confianca=1.5,  # Invalid!
        tipo_painel="monocristalino"
    )
except ValueError as e:
    print(f"Validation error: {e}")

# This raises ValueError - area cannot be negative
try:
    invalid_potencia = EstimativaPotencia(
        total_area_m2=-5,  # Invalid!
        num_paineis=10,
        potencia_instalada_kw=2.0
    )
except ValueError as e:
    print(f"Validation error: {e}")
```

## Data Transfer Objects (DTOs)

DTOs facilitate data transfer between layers without exposing domain logic:

```python
from domain.painel_solar import (
    PainelSolarDTO,
    DetectionResultDTO,
    PropertyClassificationDTO,
)

# Create DTOs for API response
painel_dto = PainelSolarDTO(
    id_painel="painel_1",
    bbox={"x": 100, "y": 200, "w": 50, "h": 60},
    centroide={"x": 125, "y": 230},
    area_pixeis=3000,
    area_m2=0.27,
    confianca=0.95,
    tipo_painel="monocristalino"
)

# Complete result
result_dto = DetectionResultDTO(
    sucesso=True,
    paineis=[painel_dto],
    tempo_processamento_s=2.5
)

# Convert to JSON
import json
json.dumps(result_dto.to_dict())
```

## Enumerations

### PropertyType

Represents property classification:

```python
from domain.painel_solar import PropertyType

# Access enum values
residencial = PropertyType.RESIDENCIAL
comercial = PropertyType.COMERCIAL
industrial = PropertyType.INDUSTRIAL
unknown = PropertyType.UNKNOWN

# Use in string context
print(residencial.value)  # "residencial"

# Get description
print(residencial.description)  # "Resid├¬ncia unifamiliar"

# Get power range
min_kw, max_kw = comercial.power_range()  # (10, 50)
```

## Immutability Pattern

All value objects use frozen dataclasses for safety:

```python
from domain.painel_solar import BoundingBox

bbox = BoundingBox(x=10, y=20, w=100, h=200)

# Reading is allowed
print(bbox.x)  # 10

# Modification raises TypeError
try:
    bbox.x = 50
except AttributeError:
    print("Value objects are immutable")

# Create new instance instead
new_bbox = BoundingBox(x=50, y=bbox.y, w=bbox.w, h=bbox.h)
```

## Validation

Domain entities validate their state at creation:

```python
from domain.painel_solar import EstimativaPotencia

# Valid creation
valid = EstimativaPotencia(
    total_area_m2=10.5,
    num_paineis=3,
    potencia_instalada_kw=1.5
)

# Invalid: negative area
try:
    invalid = EstimativaPotencia(
        total_area_m2=-10,  # ÔÜá Invalid!
        num_paineis=3,
        potencia_instalada_kw=1.5
    )
except ValueError as e:
    print(f"Domain validation: {e}")

# Invalid: negative panels
try:
    invalid = EstimativaPotencia(
        total_area_m2=10,
        num_paineis=-5,  # ÔÜá Invalid!
        potencia_instalada_kw=1.5
    )
except ValueError as e:
    print(f"Domain validation: {e}")
```

## Business Logic

Domain entities can contain business logic:

```python
from domain.painel_solar import EstimativaPotencia

estimativa = EstimativaPotencia(
    total_area_m2=20.0,
    num_paineis=5,
    potencia_instalada_kw=3.0,
    potencia_por_m2=150.0,
    insolacao_media_kwh_m2_dia=4.5
)

# Calculate annual production
estimativa.calcular()

print(f"Daily production: {estimativa.producao_diaria_kwh:.2f} kWh")
print(f"Annual production: {estimativa.producao_anual_kwh:.2f} kWh")
print(f"Annual savings: R$ {estimativa.economia_anual_brl:.2f}")
```

## Serialization

All domain objects can be converted to dictionaries:

```python
from domain.painel_solar import PainelSolar, BoundingBox, Centroide
import json

painel = PainelSolar(
    id_painel="painel_1",
    bbox=BoundingBox(x=100, y=200, w=50, h=60),
    centroide=Centroide(x=125.0, y=230.0),
    area_pixeis=3000,
    area_m2=0.27,
    confianca=0.95,
    tipo_painel="monocristalino"
)

# Convert to dict
data = painel.to_dict()
print(json.dumps(data, indent=2, default=str))

# Output:
# {
#   "id_painel": "painel_1",
#   "bbox": {"x": 100, "y": 200, "w": 50, "h": 60},
#   "centroide": {"x": 125.0, "y": 230.0},
#   "area_pixeis": 3000,
#   "area_m2": 0.27,
#   "confianca": 0.95,
#   "tipo_painel": "monocristalino",
#   "timestamp_deteccao": "2026-02-15T10:30:45.123456"
# }
```

## Integration

Use domain entities with application and infrastructure layers:

```python
# Domain layer (pure business logic)
from domain.painel_solar import PainelSolar, BoundingBox, Centroide

# Application layer (coordinates layers)
from application.painel_solar import PainelSolarApplicationService

# Infrastructure layer (technical implementation)
from infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PowerEstimator,
)

# Infrastructure returns domain entities
detection_service = SolarPanelDetectionService()
paineis = detection_service.detectar_paineis(roi)  # Returns List[PainelSolar]

# Application service processes domain entities
service = PainelSolarApplicationService()
potencia = service.estimar_potencia(paineis)  # Takes List[PainelSolar]

# Results are DTOs for external use
result = service.processar_telhado_completo(url, bbox)
api_response = result.to_dict()  # Converts to JSON-serializable dict
```

## Testing

Domain layer is easy to test - no external dependencies:

```python
import pytest
from domain.painel_solar import (
    PainelSolar,
    BoundingBox,
    Centroide,
    EstimativaPotencia,
    PropertyType,
)

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
    assert painel.area_m2 == 0.9
    assert painel.confianca == 0.95

def test_painel_confidence_validation():
    with pytest.raises(ValueError):
        PainelSolar(
            id_painel="p1",
            bbox=BoundingBox(x=0, y=0, w=100, h=100),
            centroide=Centroide(x=50, y=50),
            area_pixeis=10000,
            area_m2=0.9,
            confianca=1.5,  # Invalid
            tipo_painel="monocristalino"
        )

def test_estimativa_potencia_calculation():
    estimativa = EstimativaPotencia(
        total_area_m2=10.0,
        num_paineis=3,
        potencia_instalada_kw=1.5,
        insolacao_media_kwh_m2_dia=4.5
    )
    estimativa.calcular()
    
    assert estimativa.producao_diaria_kwh > 0
    assert estimativa.producao_anual_kwh > 0
    assert estimativa.economia_anual_brl > 0

def test_property_type_enum():
    assert PropertyType.RESIDENCIAL.description == "Resid├¬ncia unifamiliar"
    assert PropertyType.COMERCIAL.power_range() == (10, 50)
    assert PropertyType.INDUSTRIAL.power_range() == (50, 500)
```

## Files

- **entity.py**: Domain entities and value objects
  - PropertyType enum
  - BoundingBox value object
  - Centroide value object
  - PainelSolar entity
  - EstimativaPotencia entity
  - PropertyClassification entity

- **dto.py**: Data transfer objects
  - PainelSolarDTO
  - EstimativaPotenciaDTO
  - PropertyClassificationDTO
  - DetectionResultDTO
  - PowerEstimationRequestDTO
  - PowerEstimationResponseDTO

## Design Principles

1. **Immutability**: Value objects are frozen
2. **Validation**: Constraints enforced at entity creation
3. **No Framework Coupling**: Pure Python, no external dependencies
4. **Testability**: No side effects, easy to test
5. **Clear Responsibility**: Domain logic only, no infrastructure concerns
6. **Aggregates**: Entity acts as aggregate root for related objects
"""
