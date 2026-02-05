import io
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, create_session, delete_time_window, load_settings, request

CKAN_API_URL = "https://dados.ons.org.br/api/3/action/package_show?id=carga-energia"

# Mapeamento de subsistemas com informações detalhadas
SUBSISTEMAS_MAPPING = {
    "norte": {
        "codigo": "NO",
        "nome_completo": "Subsistema Norte",
        "regiao": "Norte",
        "descricao": "Abrange principalmente os estados do Amazonas, Amapá, Pará e Roraima"
    },
    "nordeste": {
        "codigo": "NE",
        "nome_completo": "Subsistema Nordeste",
        "regiao": "Nordeste",
        "descricao": "Abrange os estados de Alagoas, Bahia, Ceará, Maranhão, Paraíba, Pernambuco, Piauí, Rio Grande do Norte e Sergipe"
    },
    "sudeste/centro-oeste": {
        "codigo": "SE/CO",
        "nome_completo": "Subsistema Sudeste/Centro-Oeste",
        "regiao": "Sudeste/Centro-Oeste",
        "descricao": "Abrange São Paulo, Minas Gerais, Rio de Janeiro, Espírito Santo, Mato Grosso, Mato Grosso do Sul, Goiás e Distrito Federal"
    },
    "sul": {
        "codigo": "S",
        "nome_completo": "Subsistema Sul",
        "regiao": "Sul",
        "descricao": "Abrange Rio Grande do Sul, Santa Catarina e Paraná"
    }
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def normalize_subsistema(nome: str) -> Optional[Dict]:
    """
    Normaliza o nome do subsistema e retorna informações enriquecidas.
    
    Handles variações como:
    - "NORDESTE" -> "nordeste"
    - "Sul" -> "sul"
    - "SUDESTE/CENTRO-OESTE" -> "sudeste/centro-oeste"
    - "Subsistema Norte" -> "norte"
    """
    if not nome or pd.isna(nome):
        return None
    
    # Limpar e normalizar
    nome_limpo = str(nome).strip().lower()
    
    # Remover prefixos comuns
    for prefix in ["subsistema ", "sub-sistema ", "ss "]:
        if nome_limpo.startswith(prefix):
            nome_limpo = nome_limpo[len(prefix):].strip()
    
    # Buscar no mapeamento
    if nome_limpo in SUBSISTEMAS_MAPPING:
        return {
            "subsistema": nome_limpo,
            **SUBSISTEMAS_MAPPING[nome_limpo]
        }
    
    # Tentar fuzzy matching para variações
    for chave, info in SUBSISTEMAS_MAPPING.items():
        if chave in nome_limpo or nome_limpo in chave:
            return {
                "subsistema": chave,
                **info
            }
    
    logger_module = logging.getLogger("etl.ons")
    logger_module.warning(f"Subsistema desconhecido: {nome}")
    return None


def find_carga_column(columns: Iterable[str]) -> Optional[str]:
    candidates = [
        "val_cargaenergiamw",
        "val_cargaenergiamediamw",
        "val_cargaeneergiamwmed",
        "val_cargaenergiammwmed",
        "val_cargaenergiamwmed",
    ]
    for name in candidates:
        if name in columns:
            return name

    for col in columns:
        if col.startswith("val_carga"):
            return col

    return None


def get_dynamic_url(session, settings, logger: logging.Logger) -> Optional[str]:
    logger.info("Consultando API CKAN do ONS.")
    try:
        response = request(session, "GET", CKAN_API_URL, settings=settings.http, logger=logger)
        response.raise_for_status()
        data = response.json()
        resources = data["result"]["resources"]
        current_year = str(datetime.now().year)
        previous_year = str(datetime.now().year - 1)

        for res in resources:
            name = res.get("name", "")
            if current_year in name and res.get("format", "").upper() == "CSV":
                return res.get("url")
        for res in resources:
            name = res.get("name", "")
            if previous_year in name and res.get("format", "").upper() == "CSV":
                return res.get("url")
        return None
    except Exception as exc:
        logger.warning("Erro na API do ONS: %s", exc)
        return None


def transform_carga_ons_csv(content: bytes, logger: logging.Logger) -> pd.DataFrame:
    """
    Transforma dados CSV do ONS com modelo normalizado.
    
    Retorna DataFrame com colunas MÍNIMAS (sem duplicação):
    - time: timestamp da medição
    - subsistema: nome normalizado do subsistema (FK para subsistema_ons_regiao)
    - carga_mw: carga em MW
    
    Informações de regiao, codigo, descricao ficam em subsistema_ons_regiao
    """
    df = pd.read_csv(io.BytesIO(content), sep=";", decimal=",")
    df = _normalize_columns(df)
    df = df.rename(
        columns={
            "din_instante": "time",
            "nom_subsistema": "subsistema_original",
            "subsistema": "subsistema_original",
            "time": "time",
        }
    )

    carga_col = find_carga_column(df.columns)
    if not carga_col:
        logger.warning("Coluna de carga nao encontrada. Colunas: %s", list(df.columns))
        return pd.DataFrame(columns=["time", "subsistema", "carga_mw"])

    df = df.rename(columns={carga_col: "carga_mw"})
    df["time"] = pd.to_datetime(df["time"])
    df["carga_mw"] = pd.to_numeric(df["carga_mw"], errors="coerce")

    # Normalizar nome do subsistema
    def normalizar_subsistema(nome_original):
        info = normalize_subsistema(nome_original)
        if info:
            return info["subsistema"]
        return None

    df["subsistema"] = df["subsistema_original"].apply(normalizar_subsistema)

    # Selecionar apenas colunas necessárias (sem duplicação)
    df_final = df[["time", "subsistema", "carga_mw"]].dropna(subset=["time", "subsistema", "carga_mw"])
    
    # Remover duplicatas (mantém primeira ocorrência)
    df_final = df_final.drop_duplicates(subset=["time", "subsistema"])
    
    logger.info(f"Transformadas {len(df_final)} linhas. Subsistemas: {df_final['subsistema'].unique().tolist()}")
    return df_final


def load_subsistema_ons_regiao(engine, logger: logging.Logger) -> None:
    """
    Cria/atualiza tabela de dimensão subsistema_ons_regiao.
    
    Esta tabela é a referência central de subsistemas e deve ser carregada
    ANTES dos dados de carga (fatos).
    
    Schema:
    - subsistema (PK): nome normalizado
    - subsistema_codigo: identificador único (NO, NE, SE/CO, S)
    - regiao: região geográfica
    - nome_completo: descrição completa
    - descricao: informações dos estados
    """
    df_subsistemas = pd.DataFrame([
        {
            "subsistema": chave,
            "subsistema_codigo": info["codigo"],
            "nome_completo": info["nome_completo"],
            "regiao": info["regiao"],
            "descricao": info["descricao"],
            "ativo": True,
            "data_criacao": datetime.now()
        }
        for chave, info in SUBSISTEMAS_MAPPING.items()
    ])
    
    with engine.begin() as conn:
        # Criar tabela se não existir
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS subsistema_ons_regiao (
                subsistema TEXT PRIMARY KEY,
                subsistema_codigo VARCHAR(10) UNIQUE NOT NULL,
                regiao TEXT NOT NULL,
                nome_completo TEXT NOT NULL,
                descricao TEXT,
                ativo BOOLEAN DEFAULT TRUE,
                data_criacao TIMESTAMP DEFAULT NOW(),
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """))
    
    # Usar SQL para fazer upsert (insert or update)
    for _, row in df_subsistemas.iterrows():
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO subsistema_ons_regiao 
                (subsistema, subsistema_codigo, nome_completo, regiao, descricao, ativo, data_criacao)
                VALUES (:subsistema, :codigo, :nome_completo, :regiao, :descricao, TRUE, NOW())
                ON CONFLICT (subsistema) DO UPDATE SET
                    nome_completo = EXCLUDED.nome_completo,
                    regiao = EXCLUDED.regiao,
                    descricao = EXCLUDED.descricao,
                    data_atualizacao = NOW()
            """), {
                "subsistema": row["subsistema"],
                "codigo": row["subsistema_codigo"],
                "nome_completo": row["nome_completo"],
                "regiao": row["regiao"],
                "descricao": row["descricao"]
            })
    
    logger.info("Tabela subsistema_ons_regiao atualizada com sucesso.")


def load_carga_ons(df: pd.DataFrame, engine, logger: logging.Logger) -> int:
    """
    Carrega dados de carga na tabela normalizada subsistema_ons.
    
    Padrão normalizado (2NF/3NF):
    - subsistema_ons: time, subsistema, carga_mw (fatos)
    - subsistema_ons_regiao: dimensão (carregada separadamente)
    
    Mantém histórico completo com timezone aware timestamps.
    """
    if df.empty:
        logger.info("Sem linhas para carregar.")
        return 0

    # Manter apenas as colunas necessárias (sem duplicação de dados)
    df_load = df[["time", "subsistema", "carga_mw"]].copy()

    min_time = df_load["time"].min()
    max_time = df_load["time"].max()
    subsistemas = df_load["subsistema"].dropna().unique().tolist()

    if subsistemas:
        delete_time_window(
            engine,
            "subsistema_ons",
            "time",
            min_time,
            max_time,
            filters={"subsistema": subsistemas},
        )

    df_load.to_sql(
        "subsistema_ons",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )
    
    logger.info(
        f"Carregadas {len(df_load)} linhas em subsistema_ons. "
        f"Subsistemas: {subsistemas}. "
        f"Período: {min_time} a {max_time}"
    )
    return int(len(df_load))


def run_extraction(session=None, engine=None, settings=None, logger=None) -> int:
    """
    Orquestra a extração completa com modelo normalizado:
    
    1. Atualiza dimensão: subsistema_ons_regiao (deve ser feito PRIMEIRO)
    2. Baixa CSV do ONS
    3. Transforma dados (sem duplicação)
    4. Carrega em subsistema_ons (fatos)
    
    Padrão 2NF/3NF - evita redundância de dados.
    """
    logger = logger or logging.getLogger("etl.ons")
    if settings is None:
        settings = load_settings()

    if not settings.database.url:
        raise ValueError("DATABASE_URL is not configured.")

    engine = engine or create_db_engine(settings.database.url)
    session = session or create_session(settings.http, logger=logger)

    logger.info("Iniciando extracao ONS (modelo normalizado).")
    
    # 1. Atualizar dimensão PRIMEIRO (carrega referências)
    load_subsistema_ons_regiao(engine, logger)
    
    # 2. Extrair dados
    target_url = get_dynamic_url(session, settings, logger)
    if not target_url:
        logger.warning("Nenhum link CSV encontrado.")
        return 0

    response = request(session, "GET", target_url, settings=settings.http, logger=logger)
    response.raise_for_status()

    # 3. Transformar
    df_final = transform_carga_ons_csv(response.content, logger)
    
    # 4. Carregar
    return load_carga_ons(df_final, engine, logger)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("etl.ons")
    try:
        run_extraction(logger=logger)
    except Exception:
        logger.exception("Falha na extracao ONS.")
        raise


if __name__ == "__main__":
    main()