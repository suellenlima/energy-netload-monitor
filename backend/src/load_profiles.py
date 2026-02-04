"""
Módulo de acesso aos perfis de carga.
Este é um proxy que mantém compatibilidade com importações antigas.
"""

from .data.data_loader import (
    get_profile,
    apply_profile_to_load,
    get_profile_metadata,
    PERFIS_TIPICOS,
)

__all__ = [
    "get_profile",
    "apply_profile_to_load",
    "get_profile_metadata",
    "PERFIS_TIPICOS",
]
