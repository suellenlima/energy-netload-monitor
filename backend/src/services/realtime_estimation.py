"""
Serviço de Estimativas em Tempo Real.

Combina:
- Hora atual do sistema
- Perfis de carga típicos
- Irradiância solar atual (API meteorológica)
- Carga ONS (última leitura disponível)

Para criar uma "visão em tempo real" mesmo sem smart meters.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.cache import cached
from ..core.database import get_engine
from ..data import load_profiles

logger = logging.getLogger(__name__)

# Timezone de Brasília (com fallback para Python < 3.9)
try:
    from zoneinfo import ZoneInfo
    TIMEZONE_BRASILIA = ZoneInfo("America/Sao_Paulo")
except ImportError:
    # Fallback para pytz se zoneinfo não disponível (Python < 3.9)
    try:
        import pytz
        TIMEZONE_BRASILIA = pytz.timezone("America/Sao_Paulo")
    except ImportError:
        # Último fallback: usar UTC-3 (fixo, não considera horário de verão)
        from datetime import timezone
        TIMEZONE_BRASILIA = timezone(timedelta(hours=-3))


@cached(ttl_seconds=300)  # Cache de 5 minutos
def get_current_irradiance(
    lat: float = -23.55,  # São Paulo padrão
    lon: float = -46.63,
    subsistema: str = "SUDESTE"
) -> Dict[str, any]:
    """
    Busca irradiância solar atual da API Open-Meteo.

    Args:
        lat: Latitude
        lon: Longitude
        subsistema: Subsistema elétrico

    Returns:
        Dict com irradiância, temperatura e timestamp
    """
    logger.info(f"Buscando irradiância atual para ({lat:.2f}, {lon:.2f})")

    # API Open-Meteo (gratuita, sem necessidade de chave)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": True,
        "hourly": "shortwave_radiation",
        "timezone": "America/Sao_Paulo",
        "forecast_days": 1
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Irradiância atual (última hora)
        current_weather = data.get("current_weather", {})
        hourly = data.get("hourly", {})

        # Timestamp atual
        current_time = datetime.fromisoformat(current_weather.get("time", ""))
        hora_atual = current_time.hour

        # Irradiância em W/m²
        irradiance_list = hourly.get("shortwave_radiation", [0] * 24)
        irradiance_current = irradiance_list[hora_atual] if len(irradiance_list) > hora_atual else 0

        result = {
            "timestamp": current_time.isoformat(),
            "hora": hora_atual,
            "irradiancia_wm2": float(irradiance_current),
            "temperatura_c": float(current_weather.get("temperature", 0)),
            "subsistema": subsistema,
            "fonte": "Open-Meteo API"
        }

        logger.info(
            f"Irradiância atual: {result['irradiancia_wm2']:.0f} W/m² "
            f"às {hora_atual}h"
        )
        return result

    except Exception as exc:
        logger.error(f"Erro ao buscar irradiância: {exc}", exc_info=True)

        # Fallback: estimar pela hora do dia (horário de Brasília)
        agora_brasilia = datetime.now(TIMEZONE_BRASILIA)
        hora_atual = agora_brasilia.hour
        irradiance_estimada = _estimate_irradiance_by_hour(hora_atual)

        return {
            "timestamp": agora_brasilia.isoformat(),
            "hora": hora_atual,
            "irradiancia_wm2": irradiance_estimada,
            "temperatura_c": 25.0,
            "subsistema": subsistema,
            "fonte": "Estimativa (API indisponível)"
        }


def _estimate_irradiance_by_hour(hora: int) -> float:
    """
    Estima irradiância solar baseada apenas na hora do dia.

    Curva simplificada para dia claro (1000 W/m² ao meio-dia).
    """
    if hora < 6 or hora > 18:
        return 0.0  # Noite

    # Curva senoidal simplificada
    # Pico ao meio-dia (12h), zero ao amanhecer/anoitecer
    import math
    angulo = (hora - 6) * (math.pi / 12)  # 6h=0°, 12h=90°, 18h=180°
    return 1000 * math.sin(angulo)


def get_latest_ons_load(
    engine: Engine,
    subsistema: str = "SUDESTE"
) -> Optional[Dict[str, any]]:
    """
    Busca a última leitura de carga do ONS no banco.

    Args:
        engine: Engine do SQLAlchemy
        subsistema: Subsistema elétrico

    Returns:
        Dict com carga, timestamp, ou None se não houver dados
    """
    logger.info(f"Buscando última carga ONS para {subsistema}")

    query = text("""
        SELECT
            time,
            subsistema,
            carga_mw
        FROM carga_ons
        WHERE UPPER(subsistema) LIKE :subsistema
        ORDER BY time DESC
        LIMIT 1
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {
                "subsistema": f"%{subsistema.upper()}%"
            }).fetchone()

            if result:
                # Calcular idade do dado em relação ao horário de Brasília
                agora_brasilia = datetime.now(TIMEZONE_BRASILIA)
                timestamp_result = result.time

                # Se timestamp do banco não tem timezone, assumir UTC e converter para Brasília
                if timestamp_result.tzinfo is None:
                    try:
                        from zoneinfo import ZoneInfo
                        timestamp_result = timestamp_result.replace(tzinfo=ZoneInfo("UTC"))
                    except ImportError:
                        try:
                            import pytz
                            timestamp_result = pytz.UTC.localize(timestamp_result)
                        except ImportError:
                            from datetime import timezone
                            timestamp_result = timestamp_result.replace(tzinfo=timezone.utc)

                # Converter para horário de Brasília para exibição
                timestamp_brasilia = timestamp_result.astimezone(TIMEZONE_BRASILIA)

                return {
                    "timestamp": timestamp_brasilia.isoformat(),
                    "subsistema": result.subsistema,
                    "carga_mw": float(result.carga_mw),
                    "idade_minutos": int(
                        (agora_brasilia - timestamp_brasilia).total_seconds() / 60
                    )
                }
            else:
                logger.warning(f"Sem dados ONS para {subsistema}")
                return None

    except Exception as exc:
        logger.error(f"Erro ao buscar carga ONS: {exc}", exc_info=True)
        return None


def estimate_mmgd_generation(
    potencia_instalada_mw: float,
    irradiancia_wm2: float,
    hora: int
) -> float:
    """
    Estima geração MMGD (solar) atual.

    Args:
        potencia_instalada_mw: Potência instalada de MMGD na região (MW)
        irradiancia_wm2: Irradiância atual (W/m²)
        hora: Hora do dia (0-23)

    Returns:
        Geração estimada em MW
    """
    # Fator de capacidade = irradiância / 1000 W/m² (referência)
    fator_capacidade = min(irradiancia_wm2 / 1000.0, 1.0)

    # Eficiência típica de painéis solares
    eficiencia = 0.75  # 75% (perdas por temperatura, inversor, etc.)

    # Geração = Potência × Fator de Capacidade × Eficiência
    geracao_mw = potencia_instalada_mw * fator_capacidade * eficiencia

    # Zero à noite
    if hora < 6 or hora > 18:
        geracao_mw = 0.0

    return geracao_mw


def get_realtime_state(
    subsistema: str = "SUDESTE",
    distribuidora: Optional[str] = None,
    subestacao_id: Optional[int] = None
) -> Dict[str, any]:
    """
    Retorna o estado atual do sistema (estimado).

    Args:
        subsistema: Subsistema elétrico
        distribuidora: Distribuidora (opcional)
        subestacao_id: ID da subestação (opcional)

    Returns:
        Dict com todas as estimativas de tempo real
    """
    logger.info(f"Calculando estado em tempo real para {subsistema}")

    engine = get_engine()
    agora = datetime.now(TIMEZONE_BRASILIA)  # Horário de Brasília
    hora_atual = agora.hour

    # 1. Irradiância solar atual
    irradiance_data = get_current_irradiance(subsistema=subsistema)

    # 2. Última carga ONS
    ons_data = get_latest_ons_load(engine, subsistema)

    # 3. Potência MMGD instalada (da GD)
    query_mmgd = text("""
        SELECT
            SUM(potencia_kw) / 1000.0 as potencia_mw
        FROM gd_granular
        WHERE fonte_geracao IN ('Solar', 'UFV')
    """)

    with engine.connect() as conn:
        mmgd_result = conn.execute(query_mmgd).fetchone()
        potencia_mmgd_mw = float(mmgd_result.potencia_mw or 0) if mmgd_result else 0

    # 4. Estimar geração MMGD atual
    geracao_mmgd_mw = estimate_mmgd_generation(
        potencia_instalada_mw=potencia_mmgd_mw,
        irradiancia_wm2=irradiance_data["irradiancia_wm2"],
        hora=hora_atual
    )

    # 5. Estimar consumo real (Carga ONS + Geração MMGD)
    carga_ons_mw = ons_data["carga_mw"] if ons_data else 0
    consumo_estimado_mw = carga_ons_mw + geracao_mmgd_mw

    # 6. Fator de capacidade solar
    fator_capacidade_solar = irradiance_data["irradiancia_wm2"] / 1000.0

    # 7. Próxima atualização (1 hora)
    proxima_atualizacao = agora + timedelta(hours=1)

    result = {
        "timestamp": agora.isoformat(),
        "hora_atual": hora_atual,
        "subsistema": subsistema,
        "distribuidora": distribuidora,
        "subestacao_id": subestacao_id,

        # Estimativas
        "estimativas": {
            "carga_ons_mw": round(carga_ons_mw, 2),
            "geracao_mmgd_mw": round(geracao_mmgd_mw, 2),
            "consumo_estimado_mw": round(consumo_estimado_mw, 2),
            "irradiancia_atual_wm2": round(irradiance_data["irradiancia_wm2"], 1),
            "fator_capacidade_solar": round(fator_capacidade_solar, 3),
            "temperatura_c": round(irradiance_data["temperatura_c"], 1)
        },

        # Metadados
        "metadados": {
            "fonte_irradiancia": irradiance_data["fonte"],
            "idade_carga_ons_min": ons_data["idade_minutos"] if ons_data else None,
            "potencia_mmgd_instalada_mw": round(potencia_mmgd_mw, 2),
            "proxima_atualizacao": proxima_atualizacao.isoformat()
        }
    }

    logger.info(
        f"Estado atual: Carga ONS {carga_ons_mw:.1f} MW + "
        f"MMGD {geracao_mmgd_mw:.1f} MW = "
        f"Consumo {consumo_estimado_mw:.1f} MW"
    )

    return result


def get_daily_forecast(
    subsistema: str = "SUDESTE",
    use_profiles: bool = True
) -> Dict[str, any]:
    """
    Retorna previsão para o restante do dia.

    Args:
        subsistema: Subsistema elétrico
        use_profiles: Se True, usa perfis de carga para projetar

    Returns:
        Dict com curvas horárias previstas
    """
    logger.info("Gerando previsão para o restante do dia")

    agora = datetime.now(TIMEZONE_BRASILIA)  # Horário de Brasília
    hora_atual = agora.hour

    # Buscar previsão de irradiância
    # (simplificado - usar última leitura ou estimativa)
    irradiance_forecast = []
    for h in range(24):
        irradiance_forecast.append(_estimate_irradiance_by_hour(h))

    # Se usar perfis, combinar com perfil residencial médio
    if use_profiles:
        perfil_res = load_profiles.get_profile("residencial")
        # Escalar para MW (assumindo base de consumo)
        base_consumo_mw = 10000  # Placeholder
        consumo_forecast = [base_consumo_mw * fator for fator in perfil_res]
    else:
        consumo_forecast = [0] * 24

    return {
        "data": agora.date().isoformat(),
        "hora_atual": hora_atual,
        "previsao_irradiancia": irradiance_forecast,
        "previsao_consumo": consumo_forecast,
        "tipo": "estimado" if use_profiles else "historico"
    }


if __name__ == "__main__":
    # Teste
    logging.basicConfig(level=logging.INFO)

    print("\n=== Estado Atual do Sistema ===")
    estado = get_realtime_state("SUDESTE")

    print(f"\nHora: {estado['hora_atual']}h")
    print(f"Irradiância: {estado['estimativas']['irradiancia_atual_wm2']:.0f} W/m²")
    print(f"Carga ONS: {estado['estimativas']['carga_ons_mw']:.1f} MW")
    print(f"Geração MMGD: {estado['estimativas']['geracao_mmgd_mw']:.1f} MW")
    print(f"Consumo Total: {estado['estimativas']['consumo_estimado_mw']:.1f} MW")
