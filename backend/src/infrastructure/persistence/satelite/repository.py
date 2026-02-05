"""Infrastructure SQLAlchemy repository for Satelite Module."""

from typing import Dict, Optional, Tuple

from sqlalchemy import text

from ....domain.satelite import (
    AreaCobertura,
    Coordenadas,
    QuotaMensal,
    RequisicaoHistorico,
    RequisicaoSatelite,
    SateliteRepository,
    TransformadorSatelite,
)


class SateliteRepositorySQLAlchemy(SateliteRepository):
    """SQLAlchemy implementation of satellite repository."""

    def __init__(self, engine):
        """Initialize with SQLAlchemy engine."""
        self.engine = engine

    def obter_transformador(self, transformador_id: int) -> Optional[TransformadorSatelite]:
        """Get transformador with all satellite data."""
        with self.engine.connect() as conn:
            sql_trafo = text("""
                SELECT
                    id,
                    codigo,
                    nome,
                    distribuidora,
                    latitude,
                    longitude,
                    tipo_tensao
                FROM transformadores_aneel
                WHERE id = :id
            """)

            result = conn.execute(sql_trafo, {"id": transformador_id}).fetchone()

            if not result:
                return None

            area = self.obter_area_cobertura(transformador_id)

            coordenadas = None
            if result.latitude and result.longitude:
                try:
                    coordenadas = Coordenadas(
                        latitude=result.latitude,
                        longitude=result.longitude
                    )
                    coordenadas.validar()
                except ValueError:
                    pass

            return TransformadorSatelite(
                transformador_id=result.id,
                transformador_codigo=result.codigo,
                transformador_nome=result.nome,
                distribuidora=result.distribuidora,
                coordenadas=coordenadas,
                tipo_tensao=result.tipo_tensao,
                area_cobertura=area
            )

    def obter_coordenadas_transformador(
        self, transformador_id: int
    ) -> Optional[Coordenadas]:
        """Get coordinates for transformador."""
        with self.engine.connect() as conn:
            sql = text("""
                SELECT latitude, longitude
                FROM transformadores_aneel
                WHERE id = :id
            """)

            result = conn.execute(sql, {"id": transformador_id}).fetchone()

            if not result or not result.latitude or not result.longitude:
                return None

            try:
                coords = Coordenadas(
                    latitude=result.latitude,
                    longitude=result.longitude
                )
                coords.validar()
                return coords
            except ValueError:
                return None

    def obter_area_cobertura(
        self, transformador_id: int
    ) -> Optional[AreaCobertura]:
        """Get coverage area for transformador."""
        with self.engine.connect() as conn:
            sql = text("""
                SELECT
                    area_m2,
                    area_km2,
                    metodo_calculo,
                    num_consumidores,
                    num_vertices
                FROM transformador_area_cobertura
                WHERE transformador_id = :id
            """)

            result = conn.execute(sql, {"id": transformador_id}).fetchone()

            if not result:
                return None

            return AreaCobertura(
                area_m2=result.area_m2,
                area_km2=result.area_km2,
                metodo_calculo=result.metodo_calculo,
                num_consumidores=result.num_consumidores,
                num_vertices=result.num_vertices
            )

    def listar_historico_requisicoes(
        self,
        transformador_id: int,
        limite: int = 50,
        offset: int = 0,
        apenas_sucesso: bool = True
    ) -> Tuple[list[RequisicaoSatelite], int]:
        """List satellite request history."""
        with self.engine.connect() as conn:
            count_sql = text("""
                SELECT COUNT(*) as total
                FROM requisicoes_satelite_cbers4a
                WHERE transformador_id = :id
                AND (:apenas_sucesso = FALSE OR status = 'sucesso')
            """)

            count_result = conn.execute(
                count_sql,
                {"id": transformador_id, "apenas_sucesso": apenas_sucesso}
            ).fetchone()
            total = count_result.total if count_result else 0

            sql = text("""
                SELECT
                    id,
                    transformador_id,
                    subestacao_id,
                    fonte_satelite,
                    status,
                    imagem_id,
                    url_download,
                    data_imagem,
                    cobertura_nuvem_percentual,
                    resolucao_metros,
                    tempo_requisicao_ms,
                    custo_usd_estimado,
                    data_requisicao
                FROM requisicoes_satelite_cbers4a
                WHERE transformador_id = :id
                AND (:apenas_sucesso = FALSE OR status = 'sucesso')
                ORDER BY data_requisicao DESC
                LIMIT :limit OFFSET :offset
            """)

            results = conn.execute(
                sql,
                {
                    "id": transformador_id,
                    "apenas_sucesso": apenas_sucesso,
                    "limit": limite,
                    "offset": offset
                }
            ).fetchall()

            requisicoes = [
                RequisicaoSatelite(
                    transformador_id=r.transformador_id,
                    subestacao_id=r.subestacao_id,
                    fonte_satelite=r.fonte_satelite,
                    status=r.status,
                    imagem_id=r.imagem_id,
                    url_download=r.url_download,
                    cobertura_nuvem_percentual=r.cobertura_nuvem_percentual,
                    resolucao_metros=r.resolucao_metros,
                    tempo_requisicao_ms=r.tempo_requisicao_ms,
                    custo_usd_estimado=r.custo_usd_estimado,
                    data_requisicao=str(r.data_requisicao) if r.data_requisicao else None,
                    data_imagem=str(r.data_imagem) if r.data_imagem else None,
                )
                for r in results
            ]

            return requisicoes, total

    def obter_quota_mensal_atual(self) -> QuotaMensal:
        """Get current month Google Maps quota."""
        with self.engine.connect() as conn:
            from datetime import datetime
            ano_mes = datetime.now().strftime("%Y-%m")

            sql = text("""
                SELECT COUNT(*) as total
                FROM requisicoes_satelite_cbers4a
                WHERE fonte_satelite = 'google_maps'
                AND TO_CHAR(data_requisicao, 'YYYY-MM') = :mes
            """)

            result = conn.execute(sql, {"mes": ano_mes}).fetchone()
            requisicoes_mes = result.total if result else 0

            return QuotaMensal(
                requisicoes_mes=requisicoes_mes,
                limite_mensal=25000
            )

    def obter_estatisticas_google_maps(self) -> Dict:
        """Get statistics for Google Maps usage."""
        with self.engine.connect() as conn:
            sql_total = text("""
                SELECT COUNT(*) as total
                FROM requisicoes_satelite_cbers4a
                WHERE fonte_satelite = 'google_maps'
            """)
            result_total = conn.execute(sql_total).fetchone()
            total = result_total.total if result_total else 0

            sql_uniq = text("""
                SELECT COUNT(DISTINCT transformador_id) as uniq
                FROM requisicoes_satelite_cbers4a
                WHERE fonte_satelite = 'google_maps'
            """)
            result_uniq = conn.execute(sql_uniq).fetchone()
            uniq = result_uniq.uniq if result_uniq else 0

            sql_stats = text("""
                SELECT
                    SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucesso,
                    SUM(CASE WHEN status != 'sucesso' THEN 1 ELSE 0 END) as erro
                FROM requisicoes_satelite_cbers4a
                WHERE fonte_satelite = 'google_maps'
            """)
            result_stats = conn.execute(sql_stats).fetchone()
            sucesso = result_stats.sucesso if result_stats and result_stats.sucesso else 0
            erro = result_stats.erro if result_stats and result_stats.erro else 0

            custo_total = total * 0.007
            taxa_sucesso = (sucesso / total * 100) if total > 0 else 0

            return {
                "total_requisicoes": total,
                "transformadores_unicos": uniq,
                "custo_total_usd": round(custo_total, 2),
                "sucesso": sucesso,
                "erro": erro,
                "taxa_sucesso": round(taxa_sucesso, 2),
            }

    def registrar_requisicao(self, requisicao: RequisicaoSatelite) -> int:
        """Register satellite request in database."""
        with self.engine.connect() as conn:
            sql = text("""
                INSERT INTO requisicoes_satelite_cbers4a
                (transformador_id, subestacao_id, fonte_satelite, status,
                 imagem_id, url_download, cobertura_nuvem_percentual,
                 resolucao_metros, tempo_requisicao_ms, custo_usd_estimado,
                 data_requisicao, data_imagem)
                VALUES
                (:transformador_id, :subestacao_id, :fonte_satelite, :status,
                 :imagem_id, :url_download, :cobertura_nuvem_percentual,
                 :resolucao_metros, :tempo_requisicao_ms, :custo_usd_estimado,
                 NOW(), :data_imagem)
            """)

            conn.execute(sql, {
                "transformador_id": requisicao.transformador_id,
                "subestacao_id": requisicao.subestacao_id,
                "fonte_satelite": requisicao.fonte_satelite,
                "status": requisicao.status,
                "imagem_id": requisicao.imagem_id,
                "url_download": requisicao.url_download,
                "cobertura_nuvem_percentual": requisicao.cobertura_nuvem_percentual,
                "resolucao_metros": requisicao.resolucao_metros,
                "tempo_requisicao_ms": requisicao.tempo_requisicao_ms,
                "custo_usd_estimado": requisicao.custo_usd_estimado,
                "data_imagem": requisicao.data_imagem,
            })
            conn.commit()

            last_id_result = conn.execute(text("SELECT LAST_INSERT_ID() as id")).fetchone()
            return last_id_result.id if last_id_result else 0

    def decidir_fonte_melhor(
        self,
        transformador_id: int,
        tentar_google: bool = True,
        tentar_cbers: bool = True,
        forcar_cbers: bool = False
    ) -> Dict:
        """Decide best satellite source."""
        if forcar_cbers:
            return {
                "fonte_recomendada": "cbers4a",
                "razao": "CBERS-4A forçado pelo usuário",
                "pode_usar": True,
                "resolucao_m": 2.0,
                "cobertura": "Brasil inteiro",
                "custo_estimado": 0.0,
            }

        if not tentar_google and not tentar_cbers:
            return {
                "fonte_recomendada": None,
                "razao": "Nenhuma fonte habilitada",
                "pode_usar": False,
            }

        quota = self.obter_quota_mensal_atual()

        if tentar_google and quota.tem_quota_disponivel():
            return {
                "fonte_recomendada": "google_maps",
                "razao": f"Quota disponível ({quota.disponivel} requisições)",
                "pode_usar": True,
                "resolucao_m": 1.0,
                "cobertura": "Mundo inteiro",
                "quota_disponivel": quota.disponivel,
                "custo_estimado": 0.007,
            }

        if tentar_cbers:
            return {
                "fonte_recomendada": "cbers4a",
                "razao": "Fallback CBERS-4A (Google Maps sem quota)",
                "pode_usar": True,
                "resolucao_m": 2.0,
                "cobertura": "Brasil inteiro",
                "custo_estimado": 0.0,
            }

        return {
            "fonte_recomendada": None,
            "razao": "Nenhuma fonte disponível",
            "pode_usar": False,
        }
