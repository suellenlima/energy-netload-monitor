from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def calculate_hidden_load(
	engine: Engine,
	subsistema: str = "SUDESTE",
	distribuidora: str | None = None,
) -> list[dict]:
	sub_upper = subsistema.upper()
	sub_simple = "SUDESTE" if "SUDESTE" in sub_upper else sub_upper
	sub_like = f"%{sub_simple}%"
	filter_clause, params_cap = _build_distrib_filter(distribuidora)

	try:
		with engine.connect() as conn:
			cap_solar_mw = _fetch_capacity(conn, filter_clause, params_cap)
			if not cap_solar_mw or cap_solar_mw < 10:
				cap_solar_mw = 3000.0 if distribuidora else 15000.0

			query = text("""
				SELECT 
					ons.time as hora,
					ons.carga_mw as carga_ons,
					COALESCE(clima.irradiancia_wm2, 0) as sol_wm2
				FROM carga_ons ons
				LEFT JOIN clima_real clima 
					ON date_trunc('hour', ons.time) = date_trunc('hour', clima.time)
					AND clima.subsistema = :sub_simple
				WHERE UPPER(ons.subsistema) LIKE :sub_like
				ORDER BY ons.time DESC
				LIMIT 24
			""")

			result = conn.execute(
				query, {"sub_simple": sub_simple, "sub_like": sub_like}
			).fetchall()
	except Exception as exc:
		logger.error(f"Erro ao calcular carga oculta: {exc}", exc_info=True)
		return []

	if not result:
		return []

	df = _build_hidden_load_dataframe(result)
	df["sol_wm2_final"] = df.apply(_corrigir_sol, axis=1)
	df["estimativa_solar_mw"] = (cap_solar_mw * (df["sol_wm2_final"] / 1000) * 0.85).clip(lower=0)
	df["carga_real_estimada"] = df["carga_ons"] + df["estimativa_solar_mw"]
	return df.to_dict(orient="records")


def fetch_classes_consumption(engine: Engine, distribuidora: str | None = None) -> list[dict]:
	filter_clause, params = _build_distrib_filter(distribuidora)
	query = text(f"""
		SELECT classe, SUM(potencia_mw) as total_mw
		FROM gd_detalhada
		{filter_clause}
		GROUP BY classe
		ORDER BY total_mw DESC
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, params).fetchall()
	except Exception as exc:
		logger.error(f"Erro ao buscar classes de consumo: {exc}", exc_info=True)
		return []

	return [
		{"classe": row.classe, "mw": round(row.total_mw or 0, 2)} for row in result
	]


def fetch_fraud_alert(engine: Engine, distribuidora: str | None = None) -> dict:
	filter_clause, params = _build_distrib_filter(distribuidora)
	query = text(f"""
		SELECT * FROM auditoria_visual 
		{filter_clause}
		ORDER BY data_inspecao DESC 
		LIMIT 1
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, params).fetchone()
	except Exception as exc:
		logger.error(f"Erro ao buscar alertas de fraude: {exc}", exc_info=True)
		return {}

	if not result:
		return {}

	return {
		"data": result.data_inspecao,
		"local": f"{result.latitude}, {result.longitude}",
		"distribuidora": result.distribuidora,
		"classe_ia": getattr(result, "classe_estimada_ia", "Não Classificado"),
		"fraude_kw": result.diferenca_fraude_kw,
		"oficial_kw": result.potencia_oficial_kw,
		"status": result.status,
	}


def list_distribuidoras(engine: Engine, subsistema: str | None = None, limit: int = 50) -> list[str]:
	where_clause = ""
	params = {"limit": limit}
	
	if subsistema:
		where_clause = "WHERE subsistema ILIKE :subsistema"
		params["subsistema"] = f"%{subsistema}%"
	
	query = text(f"""
		SELECT DISTINCT distribuidora 
		FROM subestacoes_ons
		{where_clause}
		ORDER BY distribuidora
		LIMIT :limit
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, params).fetchall()
			names = [row.distribuidora for row in result]
			return [""] + names
	except Exception as exc:
		logger.error(f"Erro ao listar distribuidoras: {exc}", exc_info=True)
		return ["", "COPEL-GT", "CEMIG GT", "CPFL PAULISTA"]


def _build_distrib_filter(distribuidora: str | None) -> tuple[str, dict]:
	if distribuidora and distribuidora.strip():
		clean = distribuidora.strip()
		return "WHERE distribuidora ILIKE :dist", {"dist": f"%{clean}%"}
	return "", {}


def _fetch_capacity(conn, filter_clause: str, params: dict) -> float:
	query = text(f"SELECT SUM(potencia_mw) FROM gd_detalhada {filter_clause}")
	return conn.execute(query, params).scalar() or 0.0


def _build_hidden_load_dataframe(result) -> pd.DataFrame:
	df = pd.DataFrame(result, columns=["hora", "carga_ons", "sol_wm2"])
	df["hora"] = pd.to_datetime(df["hora"])
	return df.sort_values("hora")


def _corrigir_sol(row: pd.Series) -> float:
	hora = row["hora"].hour
	if 6 <= hora <= 18 and row["sol_wm2"] < 10:
		return np.sin(np.pi * (hora - 6) / 12) * 800
	return row["sol_wm2"]


def fetch_establishment_counts(engine: Engine, distribuidora: str | None = None) -> list[dict]:
	"""Retorna contagem de estabelecimentos por tipo."""
	filter_clause, params = _build_distrib_filter(distribuidora)

	query = text(f"""
		SELECT
			tipo_estabelecimento as tipo,
			COUNT(*) as quantidade,
			SUM(qtd_unidades) as total_unidades,
			SUM(potencia_kw) / 1000 as total_mw
		FROM gd_granular
		{filter_clause}
		GROUP BY tipo_estabelecimento
		ORDER BY quantidade DESC
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, params).fetchall()
	except Exception as exc:
		logger.error(f"Erro ao buscar contagens: {exc}", exc_info=True)
		return []

	return [
		{
			"tipo": row.tipo,
			"quantidade": int(row.quantidade or 0),
			"total_unidades": int(row.total_unidades or 0),
			"total_mw": round(row.total_mw or 0, 2)
		}
		for row in result
	]


def fetch_granular_summary(engine: Engine, distribuidora: str | None = None) -> dict:
	"""Retorna resumo geral dos dados granulares."""
	filter_clause, params = _build_distrib_filter(distribuidora)

	query = text(f"""
		SELECT
			COUNT(*) as total_instalacoes,
			SUM(qtd_unidades) as total_unidades,
			SUM(potencia_kw) / 1000 as total_mw
		FROM gd_granular
		{filter_clause}
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, params).fetchone()
			counts = fetch_establishment_counts(engine, distribuidora)
	except Exception as exc:
		logger.error(f"Erro ao buscar resumo: {exc}", exc_info=True)
		return {}

	if not result:
		return {}

	return {
		"total_instalacoes": int(result.total_instalacoes or 0),
		"total_unidades_consumidoras": int(result.total_unidades or 0),
		"total_mw": round(result.total_mw or 0, 2),
		"por_tipo": {item["tipo"]: item for item in counts}
	}
