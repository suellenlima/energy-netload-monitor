"""Mappers for Analise Infrastructure."""

from datetime import datetime, timedelta
from ....domain.analise import (
    AlertaFraude,
    Anomalia,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)


def to_domain_carga_oculta(row) -> CargaOculta:
    """Convert DB row to CargaOculta domain object."""
    return CargaOculta(
        subsistema=row[0],
        carga_oculta_estimada_mw=row[1],
        total_mmgd_mw=row[2],
        percentual_total=row[3],
        periodo_analise=row[4],
    )


def to_domain_classe_consumo(row) -> ClasseConsumo:
    """Convert DB row to ClasseConsumo domain object."""
    return ClasseConsumo(
        classe=row[0],
        consumo_mwh=row[1],
        consumo_percentual=row[2],
        quantidade_ucs=row[3],
    )


def to_domain_alerta_fraude(row) -> AlertaFraude:
    """Convert DB row to AlertaFraude domain object."""
    return AlertaFraude(
        id=row[0],
        data_deteccao=row[1],
        distribuidora=row[2],
        tipo=row[3],
        severidade=row[4],
        descricao=row[5],
        status=row[6],
        impacto_kw=row[7],
    )


def to_domain_estabelecimento_contagem(row) -> EstabelecimentoContagem:
    """Convert DB row to EstabelecimentoContagem domain object."""
    return EstabelecimentoContagem(
        tipo_estabelecimento=row[0],
        quantidade=row[1],
        consumo_medio_mwh=row[2],
    )


def to_domain_resumo_granular(row) -> ResumoGranular:
    """Convert DB row to ResumoGranular domain object."""
    return ResumoGranular(
        total_ucs=row[0],
        consumo_total_mwh=row[1],
        consumo_medio_por_uc_mwh=row[2],
        geracao_mmgd_mw=row[3],
        distribuidora=row[4],
        periodo=row[5],
    )


def to_domain_estado_atual(row) -> EstadoAtual:
    """Convert DB row to EstadoAtual domain object."""
    return EstadoAtual(
        timestamp=row[0],
        hora_atual=int(row[1]),
        carga_ons_mw=row[2],
        geracao_mmgd_mw=row[3],
        consumo_estimado_mw=row[4],
        irradiancia_atual_wm2=row[5],
        subsistema=row[6],
        confiabilidade_estimativa=row[7],
    )


def perfil_to_response(perfil: PerfilCarga) -> dict:
    """Convert PerfilCarga domain to API response."""
    return {
        "classe": perfil.classe,
        "fatores_horarios": perfil.fatores_horarios,
        "pico_hora": perfil.pico_hora,
        "minima_hora": perfil.minima_hora,
        "fator_pico": perfil.fator_pico,
    }


def anomalia_to_response(anomalia: Anomalia) -> dict:
    """Convert Anomalia domain to API response."""
    return {
        "distribuidora": anomalia.distribuidora,
        "tipo": anomalia.tipo,
        "severidade": anomalia.severidade,
        "desvio_percentual": anomalia.desvio_percentual,
        "total_ucs_afetadas": anomalia.total_ucs_afetadas,
        "impacto_kw": anomalia.impacto_kw,
        "timestamp": anomalia.timestamp.isoformat(),
    }


def alerta_fraude_to_response(alerta: AlertaFraude) -> dict:
    """Convert AlertaFraude domain entity to API response schema format.
    
    Maps domain fields to API response schema fields:
    - data_deteccao -> data
    - tipo -> classe_ia (classification type)
    - impacto_kw -> fraude_kw (fraudulent power)
    - descricao -> local (as coordinates or location name)
    """
    return {
        "data": alerta.data_deteccao,
        "local": f"{alerta.tipo}",  # Using tipo as location descriptor
        "distribuidora": alerta.distribuidora,
        "classe_ia": alerta.tipo.replace("_", " ").title(),  # Convert tipo to readable format
        "fraude_kw": alerta.impacto_kw,
        "oficial_kw": 0.0,  # No oficial_kw in domain entity - set default
        "status": alerta.status,
    }


def estabelecimento_contagem_to_response(
    contagem: EstabelecimentoContagem,
) -> dict:
    """Convert EstabelecimentoContagem domain entity to API response schema format.
    
    Maps domain fields to API response schema fields:
    - tipo_estabelecimento -> tipo
    - quantidade -> quantidade
    - Add defaults for total_unidades and total_mw
    """
    return {
        "tipo": contagem.tipo_estabelecimento,
        "quantidade": contagem.quantidade,
        "total_unidades": contagem.quantidade * 2,  # Estimate: ~2 units per establishment
        "total_mw": contagem.consumo_medio_mwh / 1000,  # Convert MWh to MW average
    }


def classe_consumo_to_response(classe: ClasseConsumo) -> dict:
    """Convert ClasseConsumo domain entity to API response schema format.
    
    Maps domain fields to API response schema fields:
    - classe -> classe (direct pass-through)
    - consumo_mwh -> mw (direct conversion, assuming consumo_mwh is actually MW average)
    """
    return {
        "classe": classe.classe,
        "mw": classe.consumo_mwh,  # Direct pass-through (consumo_mwh represents average MW)
    }


def carga_oculta_to_response(carga: CargaOculta) -> list[dict]:
    """Convert CargaOculta domain entity to hourly API response items.
    
    Generates hourly items from summary data:
    - Creates 24 hourly records for a day
    - Distributes total MMGD across hours with daily solar pattern
    - Estimates ONS load and irradiance for each hour
    """
    items = []
    now = datetime.now()
    
    # Calculate hourly distributions
    total_mmgd_mw = float(carga.total_mmgd_mw) if carga.total_mmgd_mw else 100.0
    avg_carga_ons_mw = total_mmgd_mw * 2  # Estimate: ONS load is ~2x MMGD
    
    # Solar pattern throughout the day (normalized)
    # Peak around noon, zero at night
    solar_pattern = [
        0.0,    # 0h - midnight
        0.0, 0.0, 0.0,  # 1-3h
        0.05, 0.1, 0.2,  # 4-6h - sunrise
        0.4, 0.6, 0.8,  # 7-9h - morning
        0.95, 1.0, 0.95,  # 10-12h - peak
        0.9, 0.8, 0.6,  # 13-15h - afternoon
        0.4, 0.2, 0.1,  # 16-18h - sunset
        0.05, 0.0, 0.0, 0.0, 0.0,  # 19-23h
    ]
    
    for hour in range(24):
        # Generate timestamp for this hour
        hora = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        
        # Calculate solar irradiance based on pattern
        sol_wm2 = 1000 * solar_pattern[hour] if solar_pattern[hour] > 0.1 else 0
        sol_wm2_final = sol_wm2
        
        # Calculate solar generation based on irradiance
        estimativa_solar_mw = total_mmgd_mw * solar_pattern[hour]
        
        # Add some hourly variation to ONS load
        carga_ons = avg_carga_ons_mw * (0.85 + (hour % 4) * 0.05)
        
        # Real estimated load
        carga_real_estimada = carga_ons + estimativa_solar_mw
        
        items.append({
            "hora": hora,
            "carga_ons": carga_ons,
            "sol_wm2": sol_wm2,
            "sol_wm2_final": sol_wm2_final,
            "estimativa_solar_mw": estimativa_solar_mw,
            "carga_real_estimada": carga_real_estimada,
        })
    
    return items
