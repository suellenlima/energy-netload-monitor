"""ETL para capturar a carga horária do ONS e persistir por subsistema/distribuidora."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, time, timezone
from ftplib import FTP, all_errors as FTP_Errors
from html import unescape
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

import requests
from sqlalchemy import text

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import create_db_engine, load_settings  # noqa: E402

logger = logging.getLogger(__name__)

ONS_REFERENCIA_URL = "https://apicarga.ons.org.br/referencia"
ONS_DSVAZAO_URL = "https://apicarga.ons.org.br/desviodesvazoes"
ONS_PREVISAO_URL = "https://apicarga.ons.org.br/previsaocargahoraria"
ONS_FTP_HOST = "ftp.ons.org.br"
ONS_FTP_USER = "anonymous"
ONS_FTP_PASS = "anonymous@example.com"
ONS_FTP_PATH = "/operacao/dadosabertos/carga"
ONS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.ons.org.br/paginas/energia-agora/",
    "Origin": "https://www.ons.org.br",
}
ENERGIA_AGORA_URL = "https://www.ons.org.br/paginas/energia-agora"
ENERGIA_AGORA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
    "Referer": "https://www.ons.org.br/",
}
NEXT_DATA_RE = re.compile(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
NUXT_DATA_RE = re.compile(r"window\.__NUXT__\s*=\s*(\{.*?\})\s*;", re.DOTALL)
SUBSISTEMA_ALIAS = {
    "SE/CO": "SUDESTE",
    "SECO": "SUDESTE",
    "SUDESTE": "SUDESTE",
    "S": "SUL",
    "SUL": "SUL",
    "NE": "NORDESTE",
    "NORDESTE": "NORDESTE",
    "N": "NORTE",
    "NORTE": "NORTE",
}
DEFAULT_DISTRIBUIDORAS = {
    "SUDESTE": ["LIGHT", "ENEL", "CPFL", "ELEKTRO", "CEMIG"],
    "SUL": ["AES", "COPEL", "RGE"],
    "NORDESTE": ["NEOENERGIA", "EQUATORIAL", "COSERN"],
    "NORTE": ["AMPERE", "EQUATORIAL"],
}


def _parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.utcnow()
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        logger.warning("Timestamp ONS inválido: %s", value)
        return datetime.utcnow()


def _extract_carga(payload: dict) -> List[dict]:
    for key in ("carga", "cargaSubsistemas", "subsistemas", "dados"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            return value
    return []


def buscar_carga_ons_referencia(target_timestamp: datetime | None = None) -> List[dict]:
    if target_timestamp:
        url = f"{ONS_REFERENCIA_URL}/{target_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}"
    else:
        url = ONS_REFERENCIA_URL
    response = requests.get(url, headers=ONS_HEADERS, timeout=15)
    response.raise_for_status()
    data = response.json()
    timestamp = _parse_timestamp(
        data.get("atualizacao")
        or data.get("dataAtualizacao")
        or data.get("referencia")
    )
    if target_timestamp:
        timestamp = target_timestamp.replace(minute=0, second=0, microsecond=0)
    carga_entries = _extract_carga(data)
    if not carga_entries:
        raise ValueError("Resposta do ONS sem vetor de carga")
    pontos: List[dict] = []
    for item in carga_entries:
        nome_raw = str(item.get("nome") or item.get("sigla") or item.get("subsistema") or "").strip()
        valor = item.get("valor") or item.get("mw") or item.get("carga")
        if not nome_raw or valor is None:
            continue
        nome = nome_raw.upper()
        subsistema = SUBSISTEMA_ALIAS.get(nome)
        if not subsistema:
            continue
        try:
            carga_mw = float(valor)
        except (TypeError, ValueError):
            continue
        pontos.append({
            "timestamp": timestamp,
            "subsistema": subsistema,
            "carga_mw": carga_mw,
        })
    if not pontos:
        raise ValueError("Nenhum subsistema reconhecido no payload do ONS")
    logger.info("Carga real do ONS obtida para %d subsistemas", len(pontos))
    return pontos


def gerar_carga_sintetica(timestamp_override: datetime | None = None) -> List[dict]:
    timestamp = (timestamp_override or datetime.utcnow()).replace(minute=0, second=0, microsecond=0)
    hora = timestamp.hour
    perfil = [
        0.82, 0.78, 0.74, 0.70, 0.68, 0.72, 0.80, 0.90,
        0.98, 1.02, 1.05, 1.04, 0.98, 0.94, 0.90, 0.92,
        0.97, 1.03, 1.08, 1.10, 1.05, 0.96, 0.90, 0.86,
    ]
    base = 170000
    carga_total = base * perfil[hora]
    pontos = []
    for subsistema, peso in ("SUDESTE", 0.52), ("SUL", 0.18), ("NORDESTE", 0.18), ("NORTE", 0.12):
        pontos.append({
            "timestamp": timestamp,
            "subsistema": subsistema,
            "carga_mw": carga_total * peso,
        })
    logger.info("Usando carga sintética para %s", timestamp.isoformat())
    return pontos


def _normalize_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = (
            value.replace("MW", "")
            .replace("mw", "")
            .replace(".", "")
            .replace(" ", "")
            .replace("\u00a0", "")
        )
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _find_timestamp_in_payload(payload: Any) -> datetime | None:
    stack = [payload]
    candidate_keys = (
        "atualizacao",
        "dataAtualizacao",
        "ultimaAtualizacao",
        "dataHora",
        "datahora",
        "data",
        "referencia",
        "timestamp",
    )
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key in candidate_keys:
                value = node.get(key)
                if isinstance(value, str):
                    try:
                        return _parse_timestamp(value)
                    except ValueError:
                        continue
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return None


def _extract_json_payload_from_html(html_text: str) -> Any:
    for pattern in (NEXT_DATA_RE, NUXT_DATA_RE):
        match = pattern.search(html_text)
        if not match:
            continue
        raw = unescape(match.group(1))
        for candidate in (raw, raw.replace("undefined", "null")):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    raise ValueError("Nao foi possivel localizar JSON no HTML do portal Energia Agora")


def _coletar_cargas_em_payload(payload: Any) -> Dict[str, float]:
    resultados: Dict[str, float] = {}
    stack = [payload]
    valor_keys = ("valor", "mw", "carga", "demanda", "geracao")
    nome_keys = ("subsistema", "sigla", "nome")
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            nome_raw = None
            for key in nome_keys:
                if key in node and node[key]:
                    nome_raw = str(node[key]).strip()
                    break
            valor_raw = None
            for key in valor_keys:
                if key in node and node[key] not in (None, ""):
                    valor_raw = node[key]
                    break
            if nome_raw and valor_raw is not None:
                subsistema = SUBSISTEMA_ALIAS.get(nome_raw.upper())
                carga = _normalize_float(valor_raw)
                if subsistema and carga is not None:
                    resultados[subsistema] = carga
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return resultados


def buscar_carga_ftp_ons() -> List[dict]:
    """Tenta baixar dados de carga do FTP publico da ONS (ultima opcao antes do sintetico)."""
    try:
        ftp = FTP(ONS_FTP_HOST, timeout=15)
        ftp.login(ONS_FTP_USER, ONS_FTP_PASS)
        ftp.cwd(ONS_FTP_PATH)
        
        arquivos = ftp.nlst()
        arquivos = [f for f in arquivos if f.lower().endswith(('.csv', '.txt', '.xls', '.xlsx'))]
        
        if not arquivos:
            raise ValueError("Nenhum arquivo de carga encontrado no FTP ONS")
        
        logger.info("Arquivos FTP disponiveis: %s", arquivos[:3])
        
        ftp.quit()
        raise NotImplementedError("Parsing de arquivos FTP nao implementado; use API ou sintetico")
    
    except FTP_Errors as e:
        logger.debug("Erro FTP ONS: %s", e)
        raise ValueError(f"FTP indisponivel: {e}")


def buscar_carga_energia_agora_html() -> List[dict]:
    """Tenta extrair dados de endpoints alternativos ONS quando API referencia falha."""
    session = requests.Session()
    session.headers.update(ONS_HEADERS)
    
    endpoints = [
        (ONS_DSVAZAO_URL, "desviodesvazoes"),
        (ONS_PREVISAO_URL, "previsaocargahoraria"),
    ]
    
    for url, nome in endpoints:
        try:
            response = session.get(url, timeout=20)
            if response.status_code == 200:
                data = response.json()
                timestamp = _find_timestamp_in_payload(data) or datetime.utcnow()
                timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
                cargas = _coletar_cargas_em_payload(data)
                if cargas:
                    pontos = [
                        {
                            "timestamp": timestamp,
                            "subsistema": subsistema,
                            "carga_mw": carga,
                        }
                        for subsistema, carga in cargas.items()
                    ]
                    logger.info("Carga extraida de %s para %d subsistemas", nome, len(pontos))
                    return pontos
        except Exception as e:
            logger.debug("Endpoint %s indisponivel: %s", nome, e)
    
    raise ValueError("Nenhum endpoint alternativo retornou dados de carga")


def obter_carga_ons(timestamp_override: datetime | None = None, source: str = "auto") -> List[dict]:
    fontes: List[Tuple[str, Callable[[], List[dict]]]] = []
    if source == "referencia":
        fontes.append(("API referência ONS", lambda: buscar_carga_ons_referencia(timestamp_override)))
    elif source == "energia-agora":
        if timestamp_override is not None:
            raise ValueError("Energia Agora nao suporta preenchimento historico horario")
        fontes.append(("Portal Energia Agora", buscar_carga_energia_agora_html))
    elif source == "ftp":
        if timestamp_override is not None:
            raise ValueError("FTP nao suporta preenchimento historico horario")
        fontes.append(("FTP ONS", buscar_carga_ftp_ons))
    else:  # auto
        if timestamp_override is not None:
            fontes.append(("API referência ONS", lambda: buscar_carga_ons_referencia(timestamp_override)))
        else:
            fontes.append(("API referência ONS", lambda: buscar_carga_ons_referencia(None)))
            fontes.append(("Portal Energia Agora", buscar_carga_energia_agora_html))
            fontes.append(("FTP ONS", buscar_carga_ftp_ons))

    for nome, func in fontes:
        try:
            pontos = func()
            if pontos:
                logger.info("Fonte %s utilizada com sucesso", nome)
                return pontos
        except Exception as exc:  # pragma: no cover - comunicação externa
            logger.warning("Fonte %s indisponível: %s", nome, exc)

    logger.warning("Nenhuma fonte ONS disponível, retornando perfil sintético")
    return gerar_carga_sintetica(timestamp_override)


def carregar_distribuidoras(conn) -> Dict[str, List[Tuple[str, float]]]:
    query = text(
        """
        SELECT nome, UPPER(subsistema) AS subsistema, COALESCE(potencia_total_kva, 1) AS peso
        FROM distribuidoras_aneel
        WHERE ativo = TRUE
        """
    )
    rows = conn.execute(query).fetchall()
    if not rows:
        return {k: [(nome, 1.0) for nome in nomes] for k, nomes in DEFAULT_DISTRIBUIDORAS.items()}
    mapping: Dict[str, List[Tuple[str, float]]] = {}
    for nome, subsistema, peso in rows:
        mapping.setdefault(subsistema, []).append((nome, float(peso)))
    return mapping


def distribuir_por_distribuidora(
    pontos_subsistemas: Iterable[dict],
    mapping: Dict[str, List[Tuple[str, float]]],
) -> List[dict]:
    registros: List[dict] = []
    total = 0.0
    for ponto in pontos_subsistemas:
        subsistema = ponto["subsistema"].upper()
        carga = float(ponto["carga_mw"])
        timestamp = ponto["timestamp"]
        distros = mapping.get(subsistema)
        if not distros:
            logger.debug("Sem distribuidoras configuradas para %s, ignorando", subsistema)
            continue
        peso_total = sum(peso for _, peso in distros) or len(distros)
        for nome, peso in distros:
            carga_dist = carga * (peso / peso_total)
            registros.append({
                "timestamp": timestamp,
                "subsistema": subsistema,
                "distribuidora": nome,
                "carga_mw": carga_dist,
            })
            total += carga_dist
    if total <= 0:
        return []
    for item in registros:
        item["percentual"] = (item["carga_mw"] / total) * 100.0
    return registros


def garantir_tabela(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS carga_ons_realtime (
                id SERIAL PRIMARY KEY,
                data_medicao TIMESTAMP NOT NULL,
                subsistema VARCHAR(50) NOT NULL,
                distribuidora VARCHAR(255),
                carga_mw FLOAT NOT NULL,
                percentual FLOAT,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_medicao, subsistema, distribuidora)
            )
            """
        )
    )
    conn.execute(
        text("CREATE INDEX IF NOT EXISTS idx_carga_ons_data ON carga_ons_realtime(data_medicao DESC)")
    )
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_carga_ons_subsistema ON carga_ons_realtime(subsistema, data_medicao DESC)"
        )
    )
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_carga_ons_distribuidora ON carga_ons_realtime(distribuidora, data_medicao DESC)"
        )
    )


def salvar_carga_ons(engine, *, timestamp_override: datetime | None = None, source: str = "auto") -> None:
    pontos = obter_carga_ons(timestamp_override, source=source)
    if not pontos:
        logger.warning("Nenhuma carga retornada pelo ONS")
        return
    with engine.begin() as conn:
        garantir_tabela(conn)
        mapping = carregar_distribuidoras(conn)
        registros = distribuir_por_distribuidora(pontos, mapping)
        if not registros:
            logger.warning("Sem registros calculados para inserir")
            return
        insert_stmt = text(
            """
            INSERT INTO carga_ons_realtime (data_medicao, subsistema, distribuidora, carga_mw, percentual)
            VALUES (:data_medicao, :subsistema, :distribuidora, :carga_mw, :percentual)
            ON CONFLICT (data_medicao, subsistema, distribuidora)
            DO UPDATE SET carga_mw = EXCLUDED.carga_mw,
                          percentual = EXCLUDED.percentual,
                          criado_em = CURRENT_TIMESTAMP
            """
        )
        for item in registros:
            conn.execute(
                insert_stmt,
                {
                    "data_medicao": item["timestamp"],
                    "subsistema": item["subsistema"],
                    "distribuidora": item["distribuidora"],
                    "carga_mw": item["carga_mw"],
                    "percentual": item["percentual"],
                },
            )
    logger.info("%d registros de carga ONS salvos", len(registros))


def preencher_dia(engine, dia: str, source: str = "auto") -> None:
    alvo = datetime.strptime(dia, "%Y-%m-%d").date()
    if source == "energia-agora":
        logger.warning("Fonte 'energia-agora' nao suporta preenchimento historico; caindo para 'referencia'")
        source = "referencia"
    logger.info("Populando histórico completo de %s", alvo.isoformat())
    for hora in range(24):
        timestamp = datetime.combine(alvo, time(hour=hora))
        salvar_carga_ons(engine, timestamp_override=timestamp, source=source)
    logger.info("Dia %s preenchido", alvo.isoformat())

def sincronizar_para_carga_distribuidoras(engine) -> None:
    """
    Copia dados de carga_ons_realtime para carga_distribuidoras e calcula carga_estimada_total_mw.
    
    Formula: carga_estimada_total_mw = carga_liquida_mw + consumo_granular_mw
    Onde: consumo_granular_mw = SUM(consumo_kwh) / 24000 da tabela consumo_granular_classe
    """
    with engine.begin() as conn:
        sync_query = text("""
            INSERT INTO carga_distribuidoras (
                data_medicao, distribuidora, subsistema, carga_liquida_mw, carga_estimada_total_mw
            )
            SELECT DISTINCT ON (ons.data_medicao, ons.distribuidora)
                ons.data_medicao,
                ons.distribuidora,
                ons.subsistema,
                ons.carga_mw as carga_liquida_mw,
                -- Calcular carga total = liquida + granular
                ons.carga_mw + COALESCE(
                    (
                        SELECT SUM(cgc.consumo_kwh) / 24000.0
                        FROM consumo_granular_classe cgc
                        WHERE UPPER(cgc.distribuidora) ILIKE UPPER(ons.distribuidora || '%')
                    ), 
                    0
                ) as carga_estimada_total_mw
            FROM carga_ons_realtime ons
            WHERE ons.distribuidora IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM carga_distribuidoras cd
                WHERE cd.data_medicao = ons.data_medicao
                AND cd.distribuidora = ons.distribuidora
            )
            ORDER BY ons.data_medicao, ons.distribuidora, ons.carga_mw DESC
            ON CONFLICT (distribuidora, data_medicao) 
            DO UPDATE SET 
                carga_estimada_total_mw = EXCLUDED.carga_estimada_total_mw,
                carga_liquida_mw = EXCLUDED.carga_liquida_mw
        """)
        result = conn.execute(sync_query)
        logger.info("Sincronizados %d registros para carga_distribuidoras (com carga granular)", result.rowcount or 0)

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    settings = load_settings()
    if not settings.database.url:
        raise RuntimeError("DATABASE_URL nao definido para o ETL do ONS")
    engine = create_db_engine(settings.database.url)
    parser = argparse.ArgumentParser(description="ETL carga ONS")
    parser.add_argument("--fill-day", dest="fill_day", help="YYYY-MM-DD para preencher as 24h")
    parser.add_argument(
        "--source",
        choices=["auto", "referencia", "energia-agora", "ftp"],
        default="auto",
        help="Fonte primaria: auto (padrao), API referencia, endpoints alternativos ou FTP ONS",
    )
    args = parser.parse_args()
    if args.fill_day:
        preencher_dia(engine, args.fill_day, source=args.source)
        sincronizar_para_carga_distribuidoras(engine)
    else:
        salvar_carga_ons(engine, source=args.source)
        sincronizar_para_carga_distribuidoras(engine)


if __name__ == "__main__":
    main()
