import logging
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, create_session, delete_all_rows, load_settings, request

GD_URL = (
    "https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida/"
    "resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"
)


def download_gd_csv(session, settings, path: Path, logger: logging.Logger) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        logger.info("Usando cache: %s", path)
        return path

    tmp_path = path.with_suffix(path.suffix + ".partial")
    logger.info("Baixando CSV de GD.")
    response = request(session, "GET", GD_URL, settings=settings.http, logger=logger, stream=True)
    response.raise_for_status()
    with open(tmp_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    tmp_path.replace(path)
    logger.info("Download concluido: %s", path)
    return path


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _normalize_kw(value: pd.Series) -> pd.Series:
    return (
        value.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )


def classify_establishment_type(row: pd.Series) -> str:
    """
    Classifica tipo de estabelecimento baseado em:
    - Classe de consumo (RE/CO/IN)
    - Tipo consumidor (PF/PJ)
    - Subgrupo tarifário (B1/B3/A1/A2/A3)
    - Quantidade de UCs

    Returns:
        str: Tipo do estabelecimento (residencia, predio_residencial, comercio, predio_comercial, industria, outro)
    """
    classe = str(row.get("DscClasseConsumo", "")).upper()
    tipo = str(row.get("SigTipoConsumidor", "")).upper()
    subgrupo = str(row.get("DscSubGrupoTarifario", "")).upper()
    qtd_uc = int(row.get("QtdUCRecebeCredito", 1))

    # Residências individuais vs Prédios Residenciais
    if "RESIDENCIAL" in classe or classe == "RE":
        if tipo == "PF" and "B1" in subgrupo and qtd_uc == 1:
            return "residencia"
        else:  # PJ ou múltiplas UCs
            return "predio_residencial"

    # Comércios individuais vs Prédios Comerciais
    elif "COMERCIAL" in classe or classe == "CO":
        if tipo == "PF" and "B3" in subgrupo and qtd_uc == 1:
            return "comercio"
        else:
            return "predio_comercial"

    # Indústrias (grupos A1-A3)
    elif "INDUSTRIAL" in classe or classe == "IN":
        return "industria"

    # Rural, Poder Público, etc
    else:
        return "outro"


def iter_gd_chunks(path: Path, chunk_size: int = 50000) -> Iterable[pd.DataFrame]:
    return pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        chunksize=chunk_size,
        on_bad_lines="skip",
        dtype=str,
    )


def transform_gd_chunks(chunks: Iterable[pd.DataFrame], logger: logging.Logger) -> Dict[Tuple[str, str, str], float]:
    aggregated: Dict[Tuple[str, str, str], float] = {}
    required_cols = [
        "NomAgente",
        "DscClasseConsumo",
        "SigUF",
        "DscFonteGeracao",
        "MdaPotenciaInstaladaKW",
    ]

    for index, chunk in enumerate(chunks):
        if index % 20 == 0:
            logger.info("Processando lote %s", index)
        chunk = _clean_columns(chunk)
        if not all(col in chunk.columns for col in required_cols):
            continue

        chunk = chunk[
            chunk["DscFonteGeracao"].str.contains("Solar", case=False, na=False)
        ].copy()
        if chunk.empty:
            continue

        potencia = _normalize_kw(chunk["MdaPotenciaInstaladaKW"])
        chunk["MdaPotenciaInstaladaKW"] = pd.to_numeric(potencia, errors="coerce").fillna(0)

        grouped = (
            chunk.groupby(["NomAgente", "DscClasseConsumo", "SigUF"])[
                "MdaPotenciaInstaladaKW"
            ]
            .sum()
        )
        for (distribuidora, classe, uf), potencia_total in grouped.items():
            key = (str(distribuidora).upper(), str(classe).upper(), str(uf).upper())
            aggregated[key] = aggregated.get(key, 0.0) + float(potencia_total)

    return aggregated


def build_gd_dataframe(aggregated: Dict[Tuple[str, str, str], float]) -> pd.DataFrame:
    rows = [
        {
            "distribuidora": key[0],
            "classe": key[1],
            "sigla_uf": key[2],
            "fonte": "Radiacao Solar",
            "potencia_mw": value / 1000,
        }
        for key, value in aggregated.items()
    ]
    return pd.DataFrame(rows)


def transform_gd_granular(chunks: Iterable[pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    """
    Processa chunks SEM agregação, mantendo granularidade.
    Aplica classificação de tipo de estabelecimento.
    """
    all_rows = []
    required_cols = [
        "NomAgente",
        "DscClasseConsumo",
        "SigUF",
        "DscFonteGeracao",
        "MdaPotenciaInstaladaKW",
        "QtdUCRecebeCredito",
        "SigTipoConsumidor",
        "DscSubGrupoTarifario",
    ]

    for index, chunk in enumerate(chunks):
        if index % 20 == 0:
            logger.info("Processando lote granular %s", index)

        chunk = _clean_columns(chunk)
        if not all(col in chunk.columns for col in required_cols):
            continue

        # Filtrar solar
        chunk = chunk[
            chunk["DscFonteGeracao"].str.contains("Solar", case=False, na=False)
        ].copy()
        if chunk.empty:
            continue

        # Normalizar potência
        potencia = _normalize_kw(chunk["MdaPotenciaInstaladaKW"])
        chunk["MdaPotenciaInstaladaKW"] = pd.to_numeric(potencia, errors="coerce").fillna(0)

        # Normalizar quantidade de UCs
        chunk["QtdUCRecebeCredito"] = pd.to_numeric(
            chunk["QtdUCRecebeCredito"], errors="coerce"
        ).fillna(1).astype(int)

        # Classificar tipo
        chunk["tipo_estabelecimento"] = chunk.apply(classify_establishment_type, axis=1)

        # Selecionar e renomear colunas
        chunk_processed = chunk[[
            "NomAgente",
            "DscClasseConsumo",
            "SigTipoConsumidor",
            "DscSubGrupoTarifario",
            "QtdUCRecebeCredito",
            "SigUF",
            "DscFonteGeracao",
            "MdaPotenciaInstaladaKW",
            "tipo_estabelecimento"
        ]].copy()

        chunk_processed.columns = [
            "distribuidora",
            "classe_consumo",
            "tipo_consumidor",
            "subgrupo_tarifario",
            "qtd_unidades",
            "sigla_uf",
            "fonte_geracao",
            "potencia_kw",
            "tipo_estabelecimento"
        ]

        all_rows.append(chunk_processed)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def load_gd_data(df: pd.DataFrame, engine, logger: logging.Logger) -> int:
    if df.empty:
        logger.info("Sem linhas para carregar.")
        return 0
    delete_all_rows(engine, "gd_detalhada")
    df.to_sql("gd_detalhada", engine, if_exists="append", index=False)
    logger.info("Carregadas %s linhas em gd_detalhada.", len(df))
    return int(len(df))


def load_gd_granular(df: pd.DataFrame, engine, logger: logging.Logger) -> int:
    """Carrega dados granulares em lotes para performance."""
    if df.empty:
        logger.info("Sem linhas granulares para carregar.")
        return 0

    delete_all_rows(engine, "gd_granular")

    batch_size = 10000
    total_loaded = 0

    for start in range(0, len(df), batch_size):
        end = min(start + batch_size, len(df))
        batch = df.iloc[start:end]
        batch.to_sql("gd_granular", engine, if_exists="append", index=False)
        total_loaded += len(batch)
        if start % 50000 == 0:
            logger.info("Carregadas %s/%s linhas granulares", total_loaded, len(df))

    logger.info("Total carregado em gd_granular: %s linhas", total_loaded)
    return total_loaded


def run_extraction(session=None, engine=None, settings=None, logger=None) -> dict:
    """Executa extração completa (agregada + granular)."""
    logger = logger or logging.getLogger("etl.gd")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL is not configured.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(settings.http, logger=logger)

    raw_path = settings.paths.raw_dir / "gd_temp.csv"
    path = download_gd_csv(session, settings, raw_path, logger)

    # Processar agregado (existente)
    logger.info("Processando dados agregados...")
    aggregated = transform_gd_chunks(iter_gd_chunks(path), logger)
    df_aggregated = build_gd_dataframe(aggregated)
    rows_aggregated = load_gd_data(df_aggregated, engine, logger)

    # Processar granular (novo)
    logger.info("Processando dados granulares...")
    df_granular = transform_gd_granular(iter_gd_chunks(path), logger)
    rows_granular = load_gd_granular(df_granular, engine, logger)

    return {
        "agregado": rows_aggregated,
        "granular": rows_granular
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.gd")
    try:
        run_extraction(logger=logger)
    except Exception:
        logger.exception("Falha na extracao GD.")
        raise


if __name__ == "__main__":
    main()