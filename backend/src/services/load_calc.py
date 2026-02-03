from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.cache import cached
from ..core.database import get_engine

logger = logging.getLogger(__name__)


def calculate_hidden_load(
	engine: Engine,
	subsistema: str = "SUDESTE",
	distribuidora: str | None = None,
) -> list[dict]:
	"""
	Calcula a separação entre carga líquida e consumo real.

	CONCEITOS FUNDAMENTAIS:
	=======================

	1. CARGA LÍQUIDA (ONS):
	   - Medida nos pontos de entrega do sistema de transmissão
	   - NÃO inclui a geração distribuída (MMGD) consumida localmente
	   - É o que o ONS "enxerga" e registra oficialmente
	   - Fórmula: Carga_Líquida = Consumo_Real - Geração_MMGD_Injetada

	2. GERAÇÃO MMGD (Micro e Minigeração Distribuída):
	   - Painéis solares e pequenas usinas no ponto de consumo
	   - Geram energia que é consumida localmente (não passa pela transmissão)
	   - Durante o dia, reduzem a carga vista pelo ONS
	   - Fórmula: Geração(h) = Potência_Instalada × Fator_Capacidade(h) × Eficiência

	3. CONSUMO REAL (Estimado):
	   - Demanda TOTAL de energia pelos consumidores
	   - Inclui o que vem da rede + o que vem da MMGD local
	   - Fórmula: Consumo_Real = Carga_Líquida_ONS + Geração_MMGD

	4. "CARGA OCULTA":
	   - Diferença entre consumo real e carga líquida
	   - É a energia da MMGD que está "escondida" do ONS
	   - Carga_Oculta = Geração_MMGD

	EXEMPLO NUMÉRICO:
	=================
	Hora: 12h (pico solar)
	- Consumo Real: 100 MW (o que os consumidores realmente usam)
	- Geração MMGD: 30 MW (painéis solares gerando)
	- Carga Líquida ONS: 70 MW (100 - 30 = o que vem da rede)

	Args:
		engine: Conexão com banco de dados
		subsistema: Subsistema elétrico (SUDESTE, SUL, etc)
		distribuidora: Filtro opcional por distribuidora

	Returns:
		Lista de dicts com campos:
		- hora: timestamp
		- carga_ons: Carga Líquida em MW (medida pelo ONS)
		- sol_wm2: Irradiância solar em W/m²
		- sol_wm2_final: Irradiância corrigida (usa perfil se faltante)
		- estimativa_solar_mw: Geração MMGD estimada em MW
		- carga_real_estimada: Consumo Real em MW (carga_ons + solar)
	"""
	sub_upper = subsistema.upper()
	sub_simple = "SUDESTE" if "SUDESTE" in sub_upper else sub_upper
	sub_like = f"%{sub_simple}%"
	filter_clause, params_cap = _build_distrib_filter(distribuidora)

	try:
		with engine.connect() as conn:
			# Buscar capacidade instalada de MMGD
			cap_solar_mw = _fetch_capacity(conn, filter_clause, params_cap)
			if not cap_solar_mw or cap_solar_mw < 10:
				# Valores padrão se não houver dados
				cap_solar_mw = 3000.0 if distribuidora else 15000.0

			# Buscar histórico de carga ONS (líquida) e irradiância solar
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

	# Corrigir irradiância faltante usando perfil solar típico
	df["sol_wm2_final"] = df.apply(_corrigir_sol, axis=1)

	# Calcular geração MMGD
	# Fórmula: Geração = Pot_Instalada × (Irradiância/1000) × Eficiência
	# Onde:
	#   - Pot_Instalada: capacidade em MW (DC)
	#   - Irradiância/1000: fator de capacidade (normalizado por 1000 W/m² = STC)
	#   - 0.85: eficiência do sistema (perdas inversor, cabos, temperatura)
	df["estimativa_solar_mw"] = (
		cap_solar_mw * (df["sol_wm2_final"] / 1000.0) * 0.85
	).clip(lower=0)

	# Calcular consumo real estimado
	# Fórmula: Consumo_Real = Carga_Líquida_ONS + Geração_MMGD
	df["carga_real_estimada"] = df["carga_ons"] + df["estimativa_solar_mw"]

	return df.to_dict(orient="records")


def fetch_classes_consumption(engine: Engine | None = None, distribuidora: str | None = None) -> list[dict]:
	"""
	Busca consumo por classe.
	Usa cache de 10 minutos para evitar queries repetidas.
	"""
	return _fetch_classes_consumption_cached(distribuidora)


@cached(ttl_seconds=600)  # Cache de 10 minutos
def _fetch_classes_consumption_cached(distribuidora: str | None = None) -> list[dict]:
	"""Versão cacheada da busca de classes de consumo."""
	engine = get_engine()
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
	"""
	Busca alertas de fraude/anomalias.

	Prioridade:
	1. Auditoria manual (tabela auditoria_visual)
	2. Detecção automática de anomalias

	Returns:
		Dict com dados do alerta ou dict vazio se nenhum alerta
	"""
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
		logger.warning(f"Tabela auditoria_visual não disponível: {exc}")
		result = None

	# Se houver alerta manual, retornar
	if result:
		return {
			"data": result.data_inspecao,
			"local": f"{result.latitude}, {result.longitude}",
			"distribuidora": result.distribuidora,
			"classe_ia": getattr(result, "classe_estimada_ia", "Não Classificado"),
			"fraude_kw": result.diferenca_fraude_kw,
			"oficial_kw": result.potencia_oficial_kw,
			"status": result.status,
			"fonte": "auditoria_manual"
		}

	# Se não houver alerta manual, usar detecção automática
	try:
		from .anomaly_detection import get_latest_alert
		alerta_auto = get_latest_alert(engine, distribuidora)
		if alerta_auto:
			return alerta_auto
	except Exception as exc:
		logger.error(f"Erro na detecção automática: {exc}", exc_info=True)

	return {}


def list_distribuidoras(engine: Engine | None = None, subsistema: str | None = None, limit: int = 50) -> list[str]:
	"""
	Lista distribuidoras disponíveis.
	Usa cache de 10 minutos para evitar queries repetidas.
	"""
	return _list_distribuidoras_cached(subsistema, limit)


@cached(ttl_seconds=600)  # Cache de 10 minutos
def _list_distribuidoras_cached(subsistema: str | None = None, limit: int = 50) -> list[str]:
	"""Versão cacheada da busca de distribuidoras."""
	engine = get_engine()
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
	"""
	Corrige irradiância faltante usando perfil solar típico.

	Quando não há medição de irradiância disponível (sensor offline, falha de rede, etc),
	esta função estima um valor baseado em perfis típicos de irradiância solar.

	Args:
		row: Linha do DataFrame com campos 'hora' e 'sol_wm2'

	Returns:
		Irradiância em W/m² (medida ou estimada)

	Lógica:
		- Se há medição válida (>10 W/m²): usa valor medido
		- Se falta medição durante o dia (6h-18h): estima usando perfil típico
		- Durante a noite: mantém zero
	"""
	hora = row["hora"].hour
	irradiancia_medida = row["sol_wm2"]

	# Durante o dia (6h-18h), se não há medição válida (< 10 W/m²)
	if 6 <= hora <= 18 and irradiancia_medida < 10:
		# Importar perfil solar
		try:
			from ..data.solar_profile import get_solar_profile
			perfil = get_solar_profile("tipico")
			# Converter fator de capacidade para irradiância (0-1 → 0-1000 W/m²)
			return perfil[hora] * 1000.0
		except ImportError:
			# Fallback: usar função senoidal simples
			# Pico ao meio-dia (12h), zero ao amanhecer (6h) e pôr do sol (18h)
			return np.sin(np.pi * (hora - 6) / 12) * 800

	# Usar medição real se disponível
	return irradiancia_medida


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


def get_load_profiles(classes: list[str] | None = None) -> dict:
	"""
	Retorna os perfis de carga típicos para classes especificadas.

	Args:
		classes: Lista de classes desejadas. Se None, retorna todas.

	Returns:
		Dict com perfis e metadados de cada classe.
	"""
	try:
		# Importar módulo de perfis
		from ..data import load_profiles
	except ImportError:
		logger.error("Módulo load_profiles não encontrado")
		return {"perfis": [], "classes_disponiveis": []}

	# Classes disponíveis no sistema
	todas_classes = list(load_profiles.PERFIS_TIPICOS.keys())

	# Se nenhuma classe especificada, retornar todas
	if not classes:
		classes = todas_classes

	# Validar classes solicitadas
	classes_validas = [c for c in classes if c.lower() in todas_classes]

	# Buscar metadados
	metadados = load_profiles.get_profile_metadata()

	# Construir resposta
	perfis = []
	for classe in classes_validas:
		classe_lower = classe.lower()
		curva = load_profiles.get_profile(classe_lower)
		meta = metadados[classe_lower]

		perfis.append({
			"classe": classe_lower,
			"curva": curva,
			"hora_pico": meta["hora_pico"],
			"fator_pico": round(meta["fator_pico"], 2),
			"hora_vale": meta["hora_vale"],
			"fator_vale": round(meta["fator_vale"], 2),
			"amplitude": round(meta["amplitude"], 2)
		})

	return {
		"perfis": perfis,
		"classes_disponiveis": todas_classes
	}
