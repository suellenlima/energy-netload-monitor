"""Classe base para repositórios."""

from __future__ import annotations

import logging
from abc import ABC

from sqlalchemy.engine import Engine


class BaseRepository(ABC):
    """
    Classe base abstrata para repositórios.

    Fornece acesso ao engine do banco de dados e logger.
    """

    def __init__(self, engine: Engine):
        """
        Inicializa o repositório.

        Args:
            engine: Engine SQLAlchemy para acesso ao banco
        """
        self.engine = engine
        self.logger = logging.getLogger(self.__class__.__name__)

    def _build_filter_clause(
        self,
        column: str,
        value: str | None,
        param_name: str = "filter_value"
    ) -> tuple[str, dict]:
        """
        Constrói cláusula WHERE para filtro ILIKE.

        Args:
            column: Nome da coluna
            value: Valor para filtrar (None = sem filtro)
            param_name: Nome do parâmetro SQL

        Returns:
            Tupla (clause_sql, params_dict)
        """
        if value and value.strip():
            clean = value.strip()
            return f"WHERE {column} ILIKE :{param_name}", {param_name: f"%{clean}%"}
        return "", {}
