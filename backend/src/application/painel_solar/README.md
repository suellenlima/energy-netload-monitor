"""
# Solar Panel Detection Service - DDD Architecture

## Quick Start

### Option 1: Application Service (Recommended)

For most use cases, use the application service:

```python
from src.application.painel_solar import PainelSolarApplicationService

# Initialize
service = PainelSolarApplicationService()

# Detect panels from Google Maps image
result = service.processar_telhado_completo(
    url_imagem="https://maps.googleapis.com/maps/api/staticmap?...",
    bbox={"x": 0, "y": 0, "w": 512, "h": 512},
    confianca_minima=0.5,
    potencia_por_m2=150.0
)

# Access results
if result.sucesso:
    for painel in result.paineis:
        print(f"Panel: {painel.id_painel}, Area: {painel.area_m2} m²")
    
    if result.estimativa_potencia:
        print(f"Power: {result.estimativa_potencia.potencia_instalada_kw} kW")
    
    if result.classificacao:
        print(f"Type: {result.classificacao.tipo}")
```


### Option 2: Individual Use Cases

For advanced workflows, compose use cases:

```python
from src.application.painel_solar.use_cases import (
    DetectarPainelSolarUseCase,
    EstimarPotenciaInstalacaoUseCase,
    ClassificarPropriedadeUseCase,
)
from src.infrastructure.ml.solar_panel_detection_service import (
    SolarPanelDetectionService,
    PowerEstimator,
    PropertyClassifier,
)

# Initialize dependencies
detection_svc = SolarPanelDetectionService()
estimator = PowerEstimator()
classifier = PropertyClassifier()

# Create use cases
detect_uc = DetectarPainelSolarUseCase(detection_svc)
power_uc = EstimarPotenciaInstalacaoUseCase(estimator)
classify_uc = ClassificarPropriedadeUseCase(classifier)

# Execute workflow
paineis = detect_uc.executar(url, bbox)
potencia = power_uc.executar(paineis)
detections = [{"area_pixels": p.area_pixeis, "confidence": p.confianca} for p in paineis]
classificacao = classify_uc.executar(detections, potencia.potencia_instalada_kw)

print(f"Panels: {len(paineis)}, Power: {potencia.potencia_instalada_kw} kW")
print(f"Classification: {classificacao.property_type.description}")
```


### Option 3: Domain Objects Directly

For custom workflows:

```python
from src.domain.painel_solar import PainelSolar, BoundingBox, Centroide

# Create domain entity
painel = PainelSolar(
    id_painel="panel_1",
    bbox=BoundingBox(x=100, y=200, w=50, h=60),
    centroide=Centroide(x=125.0, y=230.0),
    area_pixeis=3000,
    area_m2=0.27,
    confianca=0.95,
    tipo_painel="monocristalino"
)

# Validation happens automatically
print(painel.area_m2)  # 0.27 m²
```


## API Endpoints Integration

Update your FastAPI endpoints to use the new architecture:

```python
from fastapi import APIRouter, HTTPException
from src.application.painel_solar import PainelSolarApplicationService
from src.domain.painel_solar import DetectionResultDTO

router = APIRouter(prefix="/api/solar-panels", tags=["Solar Panels"])
service = PainelSolarApplicationService()

@router.post("/detect")
async def detect_panels(
    url_imagem: str,
    bbox: Dict[str, float],
    confianca_minima: float = 0.5
):
    """Detect solar panels in a roof image"""
    result = service.processar_telhado_completo(
        url_imagem=url_imagem,
        bbox=bbox,
        confianca_minima=confianca_minima
    )
    
    if not result.sucesso:
        raise HTTPException(status_code=400, detail=result.erros[0] if result.erros else "Detection failed")
    
    return result.to_dict()

@router.post("/classify")
async def classify_property(detections: List[Dict]):
    """Classify property based on detections"""
    result = service.classificar_e_estimar_completo(detections)
    return result
```


## Domain Entities

### PainelSolar (Domain Entity)
Represents a detected solar panel:
- `id_painel`: Unique identifier
- `bbox`: Bounding box (x, y, w, h)
- `centroide`: Panel centroid (x, y)
- `area_pixeis`: Area in pixels
- `area_m2`: Area in square meters
- `confianca`: Detection confidence (0-1)
- `tipo_painel`: Panel type (monocristalino, policristalino, filme fino, desconhecido)
- `timestamp_deteccao`: Detection timestamp

### EstimativaPotencia (Domain Entity)
Represents power estimation:
- `total_area_m2`: Total area in m²
- `num_paineis`: Number of detected panels
- `potencia_instalada_kw`: Installed power in kW
- `producao_anual_kwh`: Annual production in kWh
- `economia_anual_brl`: Annual savings in BRL

### PropertyType (Enum)
Property classification:
- `RESIDENCIAL`: 1-5 panels, 3-10 kW
- `COMERCIAL`: 5-20 panels, 10-50 kW
- `INDUSTRIAL`: 20+ panels, 50+ kW
- `UNKNOWN`: Classification uncertain


## Data Transfer Objects (DTOs)

DTOs facilitate data transfer between layers:

### PainelSolarDTO
```python
{
    "id_painel": "painel_1",
    "bbox": {"x": 100, "y": 200, "w": 50, "h": 60},
    "centroide": {"x": 125, "y": 230},
    "area_pixeis": 3000,
    "area_m2": 0.27,
    "confianca": 0.95,
    "tipo_painel": "monocristalino",
    "timestamp_deteccao": "2026-02-15T10:30:45.123456"
}
```

### DetectionResultDTO
```python
{
    "sucesso": true,
    "paineis": [...],
    "estimativa_potencia": {...},
    "classificacao": {...},
    "erros": [],
    "tempo_processamento_s": 2.345
}
```


## Error Handling

```python
try:
    result = service.processar_telhado_completo(url, bbox)
    if result.sucesso:
        # Process results
        pass
    else:
        # Handle errors
        for error in result.erros:
            logger.error(f"Detection error: {error}")
except ValueError as e:
    # Validation errors in domain entities
    logger.error(f"Validation error: {e}")
except Exception as e:
    # Unexpected errors
    logger.error(f"Unexpected error: {e}")
```


## Testing

### Unit Test: Domain Entity
```python
def test_painel_solar_validation():
    with pytest.raises(ValueError):
        PainelSolar(
            id_painel="p1",
            bbox=BoundingBox(x=0, y=0, w=100, h=100),
            centroide=Centroide(x=50, y=50),
            area_pixeis=10000,
            area_m2=0.9,
            confianca=1.5,  # Invalid: > 1.0
            tipo_painel="monocristalino"
        )
```

### Unit Test: Use Case
```python
def test_detect_use_case(mocker):
    mock_service = mocker.Mock(spec=SolarPanelDetectionService)
    mock_service.baixar_roi_do_telhado.return_value = np.zeros((512, 512, 3))
    mock_service.detectar_paineis.return_value = [
        PainelSolar(id_painel="p1", ...)
    ]
    
    uc = DetectarPainelSolarUseCase(mock_service)
    result = uc.executar("http://example.com/image.jpg", {"x": 0, "y": 0, "w": 512, "h": 512})
    
    assert len(result) == 1
```

### Integration Test: Application Service
```python
def test_full_pipeline(mocker):
    # Mock external dependencies
    mocker.patch("requests.get")
    
    service = PainelSolarApplicationService()
    result = service.processar_telhado_completo(
        url_imagem="http://example.com/image.jpg",
        bbox={"x": 0, "y": 0, "w": 512, "h": 512}
    )
    
    assert result.sucesso or len(result.erros) > 0
```


## Migration Checklist

If migrating from the old `SolarPanelService`:

- [ ] Update imports to use `PainelSolarApplicationService`
- [ ] Replace `service.processar_telhado()` with `service.processar_telhado_completo()`
- [ ] Update result access (use DTOs instead of dicts)
- [ ] Add error handling with `result.sucesso` check
- [ ] Update API endpoints to return DTOs as JSON
- [ ] Add tests for the new service
- [ ] Document any custom workflows with use cases
- [ ] Remove old `SolarPanelService` imports from new code


## Configuration

### Model Path
By default, the service looks for the YOLOv8 model at:
```
notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt
```

To use a different model:
```python
service = PainelSolarApplicationService(
    modelo_yolo_path="/path/to/custom/model.pt"
)
```

### Power Estimation Parameters
```python
# Default values
potencia_por_m2 = 150.0  # W/m²
resolution_m_per_pixel = 0.3  # for Google Maps zoom 20
power_density = 200  # W/m²
efficiency = 0.20  # 20%
capacity_factor = 0.18  # for Brazil
```

Customize for your region:
```python
estimator = PowerEstimator(resolution_m_per_pixel=0.25)
# or in application service use cases
```


## Performance Tips

1. **Batch Processing**: Process multiple images in parallel
2. **Model Loading**: Load model once and reuse service instance
3. **Image Quality**: Provide high-resolution images for better detection
4. **Confidence Threshold**: Adjust `confianca_minima` for speed vs accuracy tradeoff
5. **Caching**: Cache results for identical inputs


## Architecture Layers

```
API Layer (endpoints)
    ↓
Application Layer (PainelSolarApplicationService)
    ├── Use Cases
    ├── Service Orchestration
    └── DTO Conversion
    ↓
Domain Layer (Business Logic)
    ├── Domain Entities (PainelSolar, EstimativaPotencia)
    ├── Value Objects (BoundingBox, Centroide, PropertyType)
    └── Domain Services (PropertyClassifier, PowerEstimator)
    ↓
Infrastructure Layer (Technical Implementation)
    ├── ML Detection (YOLO)
    ├── Image Download (requests, PIL)
    └── External Services
```


## Further Reading

- See `DDD_MIGRATION_SUMMARY.md` for detailed migration guide
- See `backend/src/application/telhado_detection/` for similar DDD implementation
- Domain-Driven Design principles: https://martinfowler.com/tags/domain%20driven%20design.html
"""
