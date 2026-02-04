# 🚀 Quick Reference: Solar Panel Service

## Uso Básico

```python
from solar_panel_service import SolarPanelService

# Inicializar
service = SolarPanelService()

# Pipeline completo (recomendado)
resultado = service.processar_telhado(
    url_imagem="https://maps.googleapis.com/maps/api/...",
    bbox={"x": 10, "y": 20, "w": 300, "h": 400},
    confianca_minima=0.5,
    potencia_por_m2=150
)

print(f"✅ Sucesso: {resultado['sucesso']}")
print(f"📊 Painéis detectados: {len(resultado['paineis'])}")
print(f"⚡ Potência: {resultado['potencia']['potencia_instalada_kw']:.2f} kW")
```

## Métodos Principais

### 1. Processamento Completo (Pipeline)

```python
resultado = service.processar_telhado(
    url_imagem: str,              # URL da imagem Google Maps
    bbox: Dict[str, float],       # {"x": int, "y": int, "w": int, "h": int}
    confianca_minima: float = 0.5,
    potencia_por_m2: float = 150.0
) -> Dict
```

**Retorna:**
```python
{
    'sucesso': bool,
    'paineis': List[Dict],        # Lista de painéis detectados
    'potencia': Dict,             # Estimativa de potência
    'erros': List[str],
    'tempo_processamento_s': float
}
```

### 2. Detecção de Painéis

```python
# Passo 1: Baixar ROI
roi = service.baixar_roi_do_telhado(
    url_imagem="https://...",
    bbox={"x": 10, "y": 20, "w": 300, "h": 400}
) -> np.ndarray

# Passo 2: Detectar painéis
paineis = service.detectar_paineis(
    imagem_roi=roi,
    confianca_minima=0.5
) -> List[PainelSolarDetectado]

# Passo 3: Estimar potência
estimativa = service.estimar_potencia(
    paineis=paineis,
    potencia_por_m2=150.0
) -> EstimativaPotencia
```

### 3. Classificação e Estimativa

```python
resultado = service.classificar_e_estimar(
    detections=[
        {'area_pixels': 1500, 'confidence': 0.85},
        {'area_pixels': 1600, 'confidence': 0.82},
        # ...
    ],
    resolution_m_per_pixel=0.3
) -> Dict
```

**Retorna:**
```python
{
    'classificacao': {
        'tipo': 'residencial|comercial|industrial',
        'confianca': float,       # 0.0-1.0
        'descricao': str,
        'faixa_potencia_kw': Tuple[float, float]
    },
    'potencia': {
        'total_power_kw': float,
        'power_from_area_kw': float,
        'power_from_count_kw': float,
        'total_area_m2': float,
        'num_panels_detected': int,
        # ...
    },
    'producao_anual': {
        'annual_production_kwh': float,
        'annual_savings_brl': float,
        'estimated_payback_years': float,
        # ...
    },
    'deteccoes': {
        'num_paineis': int,
        'confianca_media': float,
        'area_total_m2': float
    }
}
```

## Dataclasses

### PainelSolarDetectado

```python
from solar_panel_service import PainelSolarDetectado

painel = PainelSolarDetectado(
    id_painel="painel_1",
    bbox={"x": 100, "y": 200, "w": 50, "h": 60},
    centroide={"x": 125, "y": 230},
    area_pixeis=3000,
    area_m2=0.27,
    confianca=0.92,
    tipo_painel="monocristalino"
)

# Converter para dict
painel_dict = painel.to_dict()
```

### EstimativaPotencia

```python
from solar_panel_service import EstimativaPotencia

estimativa = EstimativaPotencia(
    total_area_m2=5.0,
    num_paineis=10,
    potencia_instalada_kw=0.75,
    potencia_por_m2=150.0
)

# Calcular produção anual
estimativa.calcular()

print(f"Produção anual: {estimativa.producao_anual_kwh:.0f} kWh")
print(f"Economia anual: R$ {estimativa.economia_anual_brl:.2f}")

# Converter para dict
estimativa_dict = estimativa.to_dict()
```

## Classes Utilitárias

### PropertyClassifier

```python
from solar_panel_service import PropertyClassifier

classifier = PropertyClassifier()

# Classificar propriedade
tipo, confianca, features = classifier.classify(
    detections=[
        {'area_pixels': 1500, 'confidence': 0.85},
        {'area_pixels': 1600, 'confidence': 0.82},
    ],
    estimated_power=3.5  # opcional
)

print(f"Tipo: {tipo}")  # 'residencial', 'comercial' ou 'industrial'
print(f"Confiança: {confianca:.0%}")

# Obter descrição
descricao = classifier.get_description(tipo)
print(f"Descrição: {descricao}")

# Obter faixa de potência típica
min_kw, max_kw = classifier.get_power_range(tipo)
print(f"Faixa típica: {min_kw}-{max_kw} kW")
```

### PowerEstimator

```python
from solar_panel_service import PowerEstimator

estimator = PowerEstimator(resolution_m_per_pixel=0.3)

# Estimar potência
power_estimate = estimator.estimate_power(
    detections=[
        {'area_pixels': 1500},
        {'area_pixels': 1600},
    ],
    power_density=200,      # W/m²
    efficiency=0.20
)

print(f"Potência total: {power_estimate['total_power_kw']:.2f} kW")
print(f"Área total: {power_estimate['total_area_m2']:.2f} m²")

# Estimar produção anual
production = estimator.estimate_annual_production(
    power_kw=3.5,
    location='Brazil',
    capacity_factor=0.18
)

print(f"Produção anual: {production['annual_production_kwh']:.0f} kWh")
print(f"Economia anual: R$ {production['annual_savings_brl']:.2f}")
print(f"Payback: {production['estimated_payback_years']:.1f} anos")
```

## Backward Compatibility (Aliases)

```python
# ✅ Novo (recomendado)
from solar_panel_service import SolarPanelService
service = SolarPanelService()

# ✅ Antigo (ainda funciona)
from solar_panel_service import PainelSolarDetectionService
service = PainelSolarDetectionService()

# ✅ Antigo (ainda funciona)
from solar_panel_service import SolarPanelClassifier
classifier = SolarPanelClassifier()
```

## Exemplos Práticos

### Exemplo 1: Detecção Simples

```python
from solar_panel_service import SolarPanelService

service = SolarPanelService()

# URL e bbox de exemplo
url = "https://maps.googleapis.com/maps/api/staticmap?..."
bbox = {"x": 50, "y": 75, "w": 400, "h": 350}

# Executar pipeline
resultado = service.processar_telhado(
    url_imagem=url,
    bbox=bbox
)

if resultado['sucesso']:
    print(f"✅ {len(resultado['paineis'])} painéis detectados")
    print(f"⚡ Potência: {resultado['potencia']['potencia_instalada_kw']:.2f} kW")
    print(f"💰 Economia anual: R$ {resultado['potencia'].get('economia_anual_brl', 0):.2f}")
else:
    print(f"❌ Erro: {resultado['erros']}")
```

### Exemplo 2: Classificação Customizada

```python
from solar_panel_service import SolarPanelService

service = SolarPanelService()

# Simular detecções
deteccoes = [
    {'area_pixels': 2500, 'confidence': 0.92} for _ in range(8)
]

# Classificar e estimar
resultado = service.classificar_e_estimar(deteccoes)

print(f"Tipo: {resultado['classificacao']['tipo'].upper()}")
print(f"Faixa de potência: {resultado['classificacao']['faixa_potencia_kw']}")
print(f"Potência estimada: {resultado['potencia']['total_power_kw']:.2f} kW")
print(f"Produção anual: {resultado['producao_anual']['annual_production_kwh']:,.0f} kWh")
```

### Exemplo 3: Processamento Etapa por Etapa

```python
from solar_panel_service import SolarPanelService, PowerEstimator, PropertyClassifier

service = SolarPanelService()
classifier = PropertyClassifier()
estimator = PowerEstimator()

# Etapa 1: Baixar e cortar ROI
roi = service.baixar_roi_do_telhado(
    url_imagem="https://...",
    bbox={"x": 10, "y": 20, "w": 300, "h": 300}
)

if roi is None:
    print("❌ Falha ao baixar imagem")
    exit()

# Etapa 2: Detectar painéis
paineis = service.detectar_paineis(roi, confianca_minima=0.5)
print(f"✅ {len(paineis)} painéis detectados")

# Etapa 3: Estimar potência
estimativa = service.estimar_potencia(paineis)
print(f"⚡ Potência: {estimativa.potencia_instalada_kw:.2f} kW")
print(f"📊 Área: {estimativa.total_area_m2:.2f} m²")

# Etapa 4: Classificar propriedade
tipo, confianca, features = classifier.classify(
    detections=[{'area_pixels': p.area_pixeis} for p in paineis],
    estimated_power=estimativa.potencia_instalada_kw
)
print(f"🏠 Tipo: {tipo} ({confianca:.0%})")

# Etapa 5: Produção anual
producao = estimator.estimate_annual_production(estimativa.potencia_instalada_kw)
print(f"📈 Produção anual: {producao['annual_production_kwh']:,.0f} kWh")
print(f"💰 Economia anual: R$ {producao['annual_savings_brl']:,.2f}")
```

## Integração com FastAPI

```python
from fastapi import APIRouter
from solar_panel_service import SolarPanelService

router = APIRouter(prefix="/solar", tags=["solar"])
service = SolarPanelService()

@router.post("/processar-telhado")
async def processar_telhado(url: str, bbox: dict):
    """Processa telhado e retorna detecções de painéis"""
    resultado = service.processar_telhado(url_imagem=url, bbox=bbox)
    return resultado

@router.post("/classificar")
async def classificar(deteccoes: list):
    """Classifica propriedade baseado em detecções"""
    resultado = service.classificar_e_estimar(deteccoes)
    return resultado
```

## Configurações Recomendadas

| Parâmetro | Padrão | Recomendação |
|---|---|---|
| `confianca_minima` | 0.5 | 0.5-0.7 |
| `potencia_por_m2` | 150 W | 150-200 W/m² |
| `capacity_factor` | 0.18 | 0.15-0.20 (Brasil: 0.15-0.18) |
| `resolution_m_per_pixel` | 0.3 m | 0.3 m (Google Maps zoom 20) |

## Troubleshooting

### ❌ Erro: "Modelo YOLO não foi carregado"

```python
# Verificar se ultralytics está instalado
pip install ultralytics

# Verificar se o modelo existe
import os
modelo_path = "notebooks/runs/detect/solar_panel_detection/yolov8_solar3/weights/best.pt"
print(f"Modelo existe: {os.path.exists(modelo_path)}")
```

### ❌ Erro: "Falha ao baixar ROI"

```python
# Verificar se URL é válida
# Verificar se bbox está dentro da imagem
# Adicionar timeout maior
roi = service.baixar_roi_do_telhado(
    url_imagem="https://...",
    bbox={"x": 0, "y": 0, "w": 640, "h": 640},
    timeout=60  # aumentar timeout
)
```

### ❌ Nenhum painel detectado

```python
# Reduzir confiança mínima
paineis = service.detectar_paineis(
    imagem_roi=roi,
    confianca_minima=0.3  # Reduzir de 0.5
)

# Ou usar imagem com melhor resolução/zoom
```

## Documentação Relacionada

- [UNIFICACAO_SOLAR_PANEL_SERVICE.md](UNIFICACAO_SOLAR_PANEL_SERVICE.md) - Detalhes da consolidação
- [SINTESE_UNIFICACOES.md](SINTESE_UNIFICACOES.md) - Série completa de unificações
