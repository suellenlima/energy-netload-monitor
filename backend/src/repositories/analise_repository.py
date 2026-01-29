"""Repositório para acesso a dados de análise."""

from __future__ import annotations

from ..services.load_calc import (
    calculate_hidden_load,
    fetch_classes_consumption,
    fetch_establishment_counts,
    fetch_fraud_alert,
    fetch_granular_summary,
    list_distribuidoras,
)
from .base import BaseRepository


class AnaliseRepository(BaseRepository):
    """
    Repositório para operações de análise.

    Encapsula chamadas aos serviços de cálculo para facilitar
    injeção de dependência e testabilidade.
    """

    def get_carga_oculta(
        self,
        subsistema: str = "SUDESTE",
        distribuidora: str | None = None,
    ) -> list[dict]:
        """
        Calcula carga oculta estimada.

        Args:
            subsistema: Subsistema elétrico (SUDESTE, NORTE, etc)
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            Lista com dados de carga oculta por hora
        """
        return calculate_hidden_load(self.engine, subsistema, distribuidora)

    def get_classes_consumo(
        self,
        distribuidora: str | None = None,
    ) -> list[dict]:
        """
        Busca consumo por classe.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            Lista com consumo por classe
        """
        return fetch_classes_consumption(self.engine, distribuidora)

    def get_alerta_fraude(
        self,
        distribuidora: str | None = None,
    ) -> dict:
        """
        Busca último alerta de fraude.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            Dados do alerta de fraude
        """
        return fetch_fraud_alert(self.engine, distribuidora)

    def get_contagem_estabelecimentos(
        self,
        distribuidora: str | None = None,
    ) -> list[dict]:
        """
        Busca contagem de estabelecimentos por tipo.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            Lista com contagem por tipo
        """
        return fetch_establishment_counts(self.engine, distribuidora)

    def get_resumo_granular(
        self,
        distribuidora: str | None = None,
    ) -> dict:
        """
        Busca resumo geral dos dados granulares.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            Resumo com totais e detalhamento por tipo
        """
        return fetch_granular_summary(self.engine, distribuidora)

    def get_distribuidoras(
        self,
        subsistema: str | None = None,
        limit: int = 50,
    ) -> list[str]:
        """
        Lista distribuidoras disponíveis.

        Args:
            subsistema: Filtrar por subsistema (opcional)
            limit: Máximo de registros

        Returns:
            Lista de nomes de distribuidoras
        """
        return list_distribuidoras(self.engine, subsistema, limit)
