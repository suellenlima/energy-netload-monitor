"""
Scheduler para atualizações horárias de dados.

Este script deve ser executado via cron/task scheduler a cada hora para:
1. Atualizar dados do ONS (carga mais recente)
2. Atualizar clima/irradiância
3. Limpar cache de estimativas antigas

Uso:
    # Linux/Mac
    crontab -e
    0 * * * * cd /path/to/project && python etl_pipeline/src/schedulers/hourly_update.py

    # Windows Task Scheduler
    Criar tarefa agendada executando este script a cada hora
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Adicionar path do projeto
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root / "backend"))
sys.path.insert(0, str(project_root / "etl_pipeline"))

from etl_pipeline.src.extractors.ons_subsistema_client import load_carga_ons
from src.extractors.inpe_weather_client import load_weather_data

logger = logging.getLogger(__name__)


def run_hourly_update():
    """
    Executa atualização horária de dados.
    """
    logger.info("=" * 60)
    logger.info(f"Iniciando atualização horária - {datetime.now()}")
    logger.info("=" * 60)

    success_count = 0
    error_count = 0

    # 1. Atualizar carga ONS
    try:
        logger.info("\n[1/2] Atualizando carga ONS...")
        load_carga_ons()
        success_count += 1
        logger.info("[OK] Carga ONS atualizada")
    except Exception as exc:
        logger.error(f"[ERRO] Falha ao atualizar carga ONS: {exc}", exc_info=True)
        error_count += 1

    # 2. Atualizar clima/irradiância
    try:
        logger.info("\n[2/2] Atualizando dados de clima...")
        load_weather_data()
        success_count += 1
        logger.info("[OK] Clima atualizado")
    except Exception as exc:
        logger.error(f"[ERRO] Falha ao atualizar clima: {exc}", exc_info=True)
        error_count += 1

    # Sumário
    logger.info("\n" + "=" * 60)
    logger.info(f"Atualização concluída - {datetime.now()}")
    logger.info(f"Sucesso: {success_count}/2 | Erros: {error_count}/2")
    logger.info("=" * 60)

    return error_count == 0


def setup_logging():
    """Configura logging para arquivo e console."""
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"hourly_update_{datetime.now():%Y%m%d}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )


if __name__ == "__main__":
    setup_logging()

    try:
        success = run_hourly_update()
        sys.exit(0 if success else 1)
    except Exception as exc:
        logger.error(f"Erro fatal na atualização: {exc}", exc_info=True)
        sys.exit(1)
