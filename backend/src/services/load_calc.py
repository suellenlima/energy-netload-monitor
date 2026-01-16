from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


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
		print(f"Erro ao calcular carga oculta: {exc}")
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
		print(f"Erro ao buscar classes de consumo: {exc}")
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
		print(f"Erro ao buscar alertas de fraude: {exc}")
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


def list_distribuidoras(engine: Engine, limit: int = 50) -> list[str]:
	query = text("""
		SELECT distribuidora 
		FROM gd_detalhada 
		GROUP BY distribuidora 
		ORDER BY SUM(potencia_mw) DESC
		LIMIT :limit
	""")

	try:
		with engine.connect() as conn:
			result = conn.execute(query, {"limit": limit}).fetchall()
			names = [row.distribuidora for row in result]
			return [""] + names
	except Exception as exc:
		print(f"Erro ao listar distribuidoras: {exc}")
		return ["", "CEMIG DISTRIBUICAO S.A", "ENEL DISTRIBUICAO SAO PAULO"]


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
