from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str
    nullable: bool = True
    description: Optional[str] = None


@dataclass(frozen=True)
class DatasetSchema:
    name: str
    table: str
    columns: Sequence[ColumnSpec]
    unique: Optional[Sequence[str]] = None
    notes: Optional[str] = None


@dataclass(frozen=True)
class ExtractorContract:
    name: str
    module: str
    source: str
    inputs: Sequence[str]
    output: DatasetSchema
    notes: Optional[str] = None


ANEEL_SIGA_SCHEMA = DatasetSchema(
    name="aneel_siga",
    table="usinas_siga",
    columns=[
        ColumnSpec("ceg", "text"),
        ColumnSpec("nome", "text"),
        ColumnSpec("fonte", "text"),
        ColumnSpec("combustivel", "text"),
        ColumnSpec("potencia_kw", "double precision", nullable=False),
        ColumnSpec("latitude", "double precision", nullable=False),
        ColumnSpec("longitude", "double precision", nullable=False),
        ColumnSpec(
            "geometry",
            "geometry(Point,4326)",
            nullable=False,
            description="Derived from latitude/longitude in EPSG:4326.",
        ),
    ],
    notes="Rows without latitude/longitude/potencia_kw are dropped.",
)

GD_SCHEMA = DatasetSchema(
    name="gd_detalhada",
    table="gd_detalhada",
    columns=[
        ColumnSpec("distribuidora", "text", nullable=False),
        ColumnSpec("classe", "text", nullable=False),
        ColumnSpec("sigla_uf", "text", nullable=False),
        ColumnSpec("fonte", "text", nullable=False),
        ColumnSpec("potencia_mw", "double precision", nullable=False),
    ],
    notes="Aggregated by distribuidora/classe/uf with solar filter only.",
)

CLIMA_REAL_SCHEMA = DatasetSchema(
    name="clima_real",
    table="clima_real",
    columns=[
        ColumnSpec("time", "timestamptz", nullable=False),
        ColumnSpec("subsistema", "varchar(20)", nullable=False),
        ColumnSpec("irradiancia_wm2", "double precision"),
        ColumnSpec("temperatura_c", "double precision"),
    ],
    unique=("time", "subsistema"),
    notes="Time shifted by OFFSET_ANOS to align with simulation window.",
)

ONS_CARGA_SCHEMA = DatasetSchema(
    name="carga_ons",
    table="carga_ons",
    columns=[
        ColumnSpec("time", "timestamp", nullable=False),
        ColumnSpec("subsistema", "text", nullable=False),
        ColumnSpec("carga_mw", "double precision", nullable=False),
    ],
)

EXTRACTOR_CONTRACTS = [
    ExtractorContract(
        name="aneel_siga",
        module="etl_pipeline/src/extractors/aneel_client.py",
        source="https://dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao-da-aneel",
        inputs=[
            "CSV at SIGA_URL; sep=';'; encoding='ISO-8859-1'; decimal=','.",
        ],
        output=ANEEL_SIGA_SCHEMA,
        notes="Downloaded via HTTP and loaded as GeoDataFrame to PostGIS.",
    ),
    ExtractorContract(
        name="gd_aneel",
        module="etl_pipeline/src/extractors/gd_client.py",
        source="https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida",
        inputs=[
            "CSV at GD_URL; sep=';'; encoding='latin-1'.",
            "Cached at /app/data/raw/gd_temp.csv.",
        ],
        output=GD_SCHEMA,
        notes="Filters solar rows and aggregates by distribuidora/classe/uf.",
    ),
    ExtractorContract(
        name="clima_open_meteo",
        module="etl_pipeline/src/extractors/inpe_weather_client.py",
        source="https://archive-api.open-meteo.com/v1/archive",
        inputs=[
            "Hourly shortwave_radiation and temperature_2m for regional centroids.",
            "Timezone America/Sao_Paulo; date window with OFFSET_ANOS and DIAS_ATRAS.",
        ],
        output=CLIMA_REAL_SCHEMA,
    ),
    ExtractorContract(
        name="carga_ons",
        module="etl_pipeline/src/extractors/ons_client.py",
        source="https://dados.ons.org.br/api/3/action/package_show?id=carga-energia",
        inputs=[
            "CKAN API to locate CSV (current or previous year).",
            "CSV sep=';'; decimal=','; carga column resolved dynamically.",
        ],
        output=ONS_CARGA_SCHEMA,
    ),
]

EXTRACTOR_CONTRACTS_BY_NAME = {c.name: c for c in EXTRACTOR_CONTRACTS}