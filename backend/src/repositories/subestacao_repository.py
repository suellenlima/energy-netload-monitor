"""Repositório para acesso a dados de subestações."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from ..core.database import table_exists
from .base import BaseRepository


class SubestacaoRepository(BaseRepository):
    """Repositório para operações de subestações."""

    def get_ons(
        self,
        distribuidora: str | None = None,
        limite: int = 100,
    ) -> list[dict]:
        """
        Busca subestações do ONS.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)
            limite: Máximo de registros

        Returns:
            Lista de subestações ONS
        """
        where_clause, params = self._build_filter_clause(
            "distribuidora", distribuidora, "dist"
        )
        params["limite"] = limite

        query = text(f"""
            SELECT id_estacao as id, nome, sigla_se, tensao_kv, subsistema,
                   distribuidora, latitude, longitude, fonte_dados
            FROM subestacoes_ons
            {where_clause}
            ORDER BY tensao_kv DESC, nome
            LIMIT :limite
        """)

        df = pd.read_sql(query, self.engine, params=params)
        self.logger.debug(f"Retornadas {len(df)} subestações ONS")
        return df.to_dict(orient="records")

    def get_detectadas(
        self,
        distribuidora: str | None = None,
        limite: int = 100,
    ) -> list[dict]:
        """
        Busca subestações detectadas via clustering.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)
            limite: Máximo de registros

        Returns:
            Lista de subestações detectadas
        """
        if not table_exists("subestacoes_detectadas", self.engine):
            self.logger.info("Tabela subestacoes_detectadas não existe ainda")
            return []

        where_clause, params = self._build_filter_clause(
            "distribuidora", distribuidora, "dist"
        )
        params["limite"] = limite

        query = text(f"""
            SELECT id, cluster_id, nome, latitude, longitude, distribuidora,
                   subsistema, quantidade_gd, potencia_total_mw, raio_deteccao_km,
                   data_deteccao
            FROM subestacoes_detectadas
            {where_clause}
            ORDER BY potencia_total_mw DESC
            LIMIT :limite
        """)

        df = pd.read_sql(query, self.engine, params=params)
        self.logger.debug(f"Retornadas {len(df)} subestações detectadas")
        return df.to_dict(orient="records") if not df.empty else []

    def get_ons_for_geojson(self, limite: int = 100) -> pd.DataFrame:
        """
        Busca subestações ONS para conversão GeoJSON.

        Args:
            limite: Máximo de registros

        Returns:
            DataFrame com dados das subestações
        """
        query = text("""
            SELECT id_estacao as id, nome, sigla_se, tensao_kv, subsistema,
                   distribuidora, latitude, longitude, 'ONS' as origem
            FROM subestacoes_ons
            LIMIT :limite
        """)
        return pd.read_sql(query, self.engine, params={"limite": limite})

    def get_detectadas_for_geojson(self, limite: int = 100) -> pd.DataFrame:
        """
        Busca subestações detectadas para conversão GeoJSON.

        Args:
            limite: Máximo de registros

        Returns:
            DataFrame com dados das subestações
        """
        if not table_exists("subestacoes_detectadas", self.engine):
            return pd.DataFrame()

        query = text("""
            SELECT id, cluster_id, nome, latitude, longitude, distribuidora,
                   subsistema, quantidade_gd, potencia_total_mw,
                   'DETECTADA' as origem
            FROM subestacoes_detectadas
            LIMIT :limite
        """)
        return pd.read_sql(query, self.engine, params={"limite": limite})

    def get_resumo(self) -> list[dict]:
        """
        Retorna resumo de subestações por distribuidora.

        Returns:
            Lista com resumo por distribuidora
        """
        has_detectadas = table_exists("subestacoes_detectadas", self.engine)

        if not has_detectadas:
            query = text("""
                SELECT
                    distribuidora,
                    COUNT(*) as total_ons,
                    0 as total_detectadas,
                    COUNT(*) as total
                FROM subestacoes_ons
                GROUP BY distribuidora
                ORDER BY total DESC
            """)
        else:
            query = text("""
                SELECT
                    distribuidora,
                    COUNT(CASE WHEN origem = 'ons' THEN 1 END) as total_ons,
                    COUNT(CASE WHEN origem = 'detectada' THEN 1 END) as total_detectadas,
                    COUNT(*) as total
                FROM (
                    SELECT distribuidora, 'ons' as origem FROM subestacoes_ons
                    UNION ALL
                    SELECT distribuidora, 'detectada' as origem FROM subestacoes_detectadas
                ) t
                GROUP BY distribuidora
                ORDER BY total DESC
            """)

        df = pd.read_sql(query, self.engine)
        return df.to_dict(orient="records") if not df.empty else []
