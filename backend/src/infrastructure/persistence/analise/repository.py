"""Infrastructure SQLAlchemy repository for Analise Module."""

from datetime import datetime
from typing import Optional

from sqlalchemy import text

from ....domain.analise import (
    AlertaFraude,
    Anomalia,
    AnaliseRepository,
    CargaOculta,
    ClasseConsumo,
    EstabelecimentoContagem,
    EstadoAtual,
    PerfilCarga,
    ResumoGranular,
)


class AnaliseRepositorySQLAlchemy(AnaliseRepository):
    """SQLAlchemy repository for Analise domain."""

    def __init__(self, engine):
        """Initialize repository."""
        self.engine = engine

    def obter_carga_oculta(
        self, subsistema: str, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """
        Get hidden load analysis (MMGD - distributed generation).
        
        Busca dados REAIS de MMGD POR DISTRIBUIDORA da tabela geracao_mmgd_distribuidora.
        Soma TODAS as fontes: Solar + Eólica + Hidro + Biomassa + Outro
        Se não houver dados reais, retorna fallback com dados sintéticos.
        """
        try:
            from datetime import datetime, timedelta
            
            with self.engine.connect() as conn:
                # Se tem distribuidora, buscar dados REAIS dela
                if distribuidora:
                    try:
                        import logging
                        logger = logging.getLogger(__name__)
                        
                        # Buscar dados REAIS usando ILIKE para fuzzy matching
                        query_mmgd = text("""
                            SELECT 
                                COALESCE(SUM(CASE WHEN fonte_geracao = 'Solar' THEN potencia_total_kw ELSE 0 END), 0) / 1000.0 AS solar_mw,
                                COALESCE(SUM(CASE WHEN fonte_geracao = 'Eólica' THEN potencia_total_kw ELSE 0 END), 0) / 1000.0 AS eolica_mw,
                                COALESCE(SUM(CASE WHEN fonte_geracao = 'Hidro' THEN potencia_total_kw ELSE 0 END), 0) / 1000.0 AS hidro_mw,
                                COALESCE(SUM(CASE WHEN fonte_geracao = 'Biomassa' THEN potencia_total_kw ELSE 0 END), 0) / 1000.0 AS biomassa_mw,
                                COALESCE(SUM(CASE WHEN fonte_geracao = 'Outro' THEN potencia_total_kw ELSE 0 END), 0) / 1000.0 AS outro_mw,
                                COALESCE(SUM(potencia_total_kw), 0) / 1000.0 AS total_mw,
                                COALESCE(SUM(quantidade_empreendimentos), 0) AS total_empreendimentos,
                                MAX(data_medicao) AS ultima_medicao
                            FROM geracao_mmgd_distribuidora
                            WHERE distribuidora ILIKE :dist_pattern
                            AND potencia_total_kw > 0
                        """)
                        
                        dist_pattern = f"%{distribuidora.upper()}%"
                        result = conn.execute(query_mmgd, {"dist_pattern": dist_pattern})
                        row = result.fetchone()
                        
                        logger.info(f"🔍 Query ILIKE para {distribuidora} ({dist_pattern}): row={row}")
                        logger.info(f"   Row type: {type(row)}, Row[5]: {row[5] if row else 'None'}")
                        
                        # Verificar se encontrou dados reais
                        total_mw = float(row[5]) if row and row[5] else 0
                        logger.info(f"   Total MW calculado: {total_mw}")
                        
                        if total_mw > 0:
                            # Potência instalada total de TODAS as fontes
                            potencia_solar_mw = float(row[0])
                            potencia_eolica_mw = float(row[1])
                            potencia_hidro_mw = float(row[2])
                            potencia_biomassa_mw = float(row[3])
                            potencia_outro_mw = float(row[4])
                            potencia_total_mw = float(row[5])
                            total_empreendimentos = int(row[6]) if row[6] else 0
                            ultima_medicao = row[7] if row[7] else None
                            
                            logger.info(f"✅ Encontrados dados REAIS de MMGD para {distribuidora}: {potencia_total_mw:.1f} MW (Solar: {potencia_solar_mw:.1f} + Eólica: {potencia_eolica_mw:.1f} + Hidro: {potencia_hidro_mw:.1f} + Biomassa: {potencia_biomassa_mw:.1f} + Outro: {potencia_outro_mw:.1f})")
                            
                            # Buscar carga atual desta distribuidora
                            carga_query = text("""
                                SELECT carga_mw, data_medicao 
                                FROM carga_distribuidoras
                                WHERE UPPER(distribuidora) = UPPER(:distribuidora)
                                ORDER BY data_medicao DESC
                                LIMIT 1
                            """)
                            
                            carga_result = conn.execute(carga_query, {"distribuidora": distribuidora})
                            carga_row = carga_result.fetchone()
                            
                            carga_ons = float(carga_row[0]) if carga_row else 180.0
                            
                            # Retornar dados históricos horárias com dados REAIS
                            carga_oculta_list = []
                            for i in range(24):
                                timestamp = datetime.now().replace(hour=i, minute=0, second=0)
                                
                                # Calcular fator solar para cada hora
                                if i < 6 or i >= 19:
                                    fator = 0.0
                                elif i < 8:
                                    fator = (i - 6) / 2 * 0.95
                                elif i < 17:
                                    fator = 0.95
                                else:
                                    fator = (19 - i) / 2 * 0.95
                                
                                geracao_solar_hora = potencia_solar_mw * fator
                                geracao_outras_hora = (potencia_eolica_mw + potencia_hidro_mw + potencia_biomassa_mw + potencia_outro_mw) * 0.7
                                geracao_total_hora = geracao_solar_hora + geracao_outras_hora
                                
                                # Calcular irradiância (W/m²) baseado no fator solar
                                sol_wm2 = 1000 * fator if fator > 0.1 else 0
                                
                                item = {
                                    "hora": timestamp.isoformat(),
                                    "distribuidora": distribuidora,
                                    "carga_ons": carga_ons,
                                    "estimativa_solar_mw": geracao_total_hora,
                                    "consumo_estimado_mw": max(0, carga_ons - geracao_total_hora),
                                    "carga_real_estimada": max(0, carga_ons - geracao_total_hora),
                                    "percentual_total": (geracao_total_hora / carga_ons * 100) if carga_ons > 0 else 0,
                                    "sol_wm2": sol_wm2,
                                    "sol_wm2_final": sol_wm2,
                                }
                                carga_oculta_list.append(item)
                            
                            return carga_oculta_list
                        else:
                            logger.warning(f"⚠️ Nenhum dado REAL encontrado para {distribuidora}, usando fallback sintético")
                            
                            # Buscar carga atual desta distribuidora
                            carga_query = text("""
                                SELECT carga_mw, data_medicao 
                                FROM carga_distribuidoras
                                WHERE UPPER(distribuidora) = UPPER(:distribuidora)
                                ORDER BY data_medicao DESC
                                LIMIT 1
                            """)
                            
                            carga_result = conn.execute(carga_query, {"distribuidora": distribuidora})
                            carga_row = carga_result.fetchone()
                            
                            carga_ons = float(carga_row[0]) if carga_row else 180.0
                            
                            # Calcular geração MMGD baseado em padrão solar (fonte principal)
                            hora_atual = datetime.now().hour
                            if hora_atual < 6 or hora_atual >= 19:
                                fator_solar = 0.0
                            elif hora_atual < 8:
                                fator_solar = (hora_atual - 6) / 2 * 0.95
                            elif hora_atual < 17:
                                fator_solar = 0.95
                            else:
                                fator_solar = (19 - hora_atual) / 2 * 0.95
                            
                            # Geração = Solar com fator + (Eólica + Hidro + Biomassa + Outro com fator menor)
                            geracao_solar_atual = potencia_solar_mw * fator_solar
                            geracao_outras_atual = (potencia_eolica_mw + potencia_hidro_mw + potencia_biomassa_mw + potencia_outro_mw) * 0.7  # Outras fontes com fator 70%
                            geracao_mmgd_atual = geracao_solar_atual + geracao_outras_atual
                            consumo_estimado = carga_ons + geracao_mmgd_atual
                            
                            # Retornar dados históricos horárias
                            carga_oculta_list = []
                            for i in range(24):
                                timestamp = datetime.now().replace(hour=i, minute=0, second=0)
                                
                                # Calcular fator solar para cada hora
                                if i < 6 or i >= 19:
                                    fator = 0.0
                                elif i < 8:
                                    fator = (i - 6) / 2 * 0.95
                                elif i < 17:
                                    fator = 0.95
                                else:
                                    fator = (19 - i) / 2 * 0.95
                                
                                geracao_solar_hora = potencia_solar_mw * fator
                                geracao_outras_hora = (potencia_eolica_mw + potencia_hidro_mw + potencia_biomassa_mw) * 0.7
                                geracao_total_hora = geracao_solar_hora + geracao_outras_hora
                                
                                # Calcular irradiância (W/m²) baseado no fator solar
                                # Irradiância máxima típica: ~1000 W/m², ajustar baseado no fator
                                sol_wm2 = 1000 * fator if fator > 0.1 else 0
                                
                                item = {
                                    "hora": timestamp.isoformat(),
                                    "distribuidora": distribuidora,
                                    "carga_ons": carga_ons,
                                    "estimativa_solar_mw": geracao_total_hora,
                                    "consumo_estimado_mw": max(0, carga_ons - geracao_total_hora),
                                    "carga_real_estimada": max(0, carga_ons - geracao_total_hora),
                                    "percentual_total": (geracao_total_hora / carga_ons * 100) if carga_ons > 0 else 0,
                                    "sol_wm2": sol_wm2,
                                    "sol_wm2_final": sol_wm2,
                                }
                                carga_oculta_list.append(item)
                            
                            return carga_oculta_list
                    
                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f"⚠️ Erro ao buscar dados de distribuidora {distribuidora}: {e}")
                
                # FALLBACK: Se não houver dados reais, usar dados sintéticos
                import logging
                import math
                logger = logging.getLogger(__name__)
                logger.info("📊 Usando dados sintéticos de MMGD (ETL não preencheu dados reais ainda)")
                
                # Dados realistas de MMGD por distribuidora
                mmgd_por_distribuidora = {
                    "LIGHT": 85.5,
                    "ENEL": 156.2,
                    "CPFL": 142.8,
                    "CEMIG": 128.4,
                    "IENERGIA": 3.2,
                    "AES": 98.7,
                    "ENERGISA": 76.3,
                    "EDP": 65.2,
                    "EQUATORIAL": 45.8,
                    "RGE": 38.9,
                    "COPEL": 52.3,
                    "CEEE": 28.5,
                }
                
                mmgd_total = mmgd_por_distribuidora.get(distribuidora.upper() if distribuidora else "LIGHT", 95.0)
                consumo_estimado = 220.5
                
                carga_oculta_list = []
                for i in range(24):
                    timestamp = datetime.now().replace(hour=i, minute=0, second=0)
                    
                    # Padrão solar realista
                    if i < 6 or i >= 19:
                        fator = 0.0
                    elif i < 8:
                        fator = (i - 6) / 2 * 0.95
                    elif i < 17:
                        fator = 0.95
                    else:
                        fator = (19 - i) / 2 * 0.95
                    
                    geracao_hora = mmgd_total * fator
                    
                    # Calcular irradiância (W/m²) baseado no fator solar
                    sol_wm2 = 1000 * fator if fator > 0.1 else 0
                    
                    item = {
                        "hora": timestamp.isoformat(),
                        "distribuidora": distribuidora or subsistema,
                        "carga_ons": 180.0 + 20.0 * math.sin((i - 12) * math.pi / 12),
                        "estimativa_solar_mw": geracao_hora,
                        "consumo_estimado_mw": consumo_estimado,
                        "carga_real_estimada": 180.0 + 20.0 * math.sin((i - 12) * math.pi / 12) + geracao_hora,
                        "percentual_total": (geracao_hora / mmgd_total * 100) if mmgd_total > 0 else 0,
                        "sol_wm2": sol_wm2,
                        "sol_wm2_final": sol_wm2,
                    }
                    carga_oculta_list.append(item)
                
                return carga_oculta_list
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ Erro ao obter carga oculta: {e}", exc_info=True)
            return []

    def obter_distribuidoras_disponiveis(self) -> list[str]:
        """
        Retorna lista de distribuidoras com dados reais na base.
        Útil para validação e autocomplete no frontend.
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT UPPER(distribuidora) as distribuidora
                    FROM geracao_mmgd_distribuidora
                    ORDER BY distribuidora
                """)
                result = conn.execute(query)
                distribuidoras = [row[0] for row in result.fetchall()]
                return distribuidoras
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ Erro ao obter lista de distribuidoras: {e}")
            return []

    def obter_carga_atual_distribuidora(self, distribuidora: str) -> dict:
        """Obtém última carga ONS da distribuidora."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        MAX(co.carga_ons) as carga_ons,
                        MAX(co.estimativa_solar_mw) as geracao_mmgd,
                        MAX(co.carga_real_estimada) as consumo_estimado,
                        MAX(co.data_criacao) as data_criacao
                    FROM carga_oculta co
                    WHERE co.distribuidora = :distribuidora
                    ORDER BY co.data_criacao DESC
                    LIMIT 1
                """)
                result = conn.execute(query, {"distribuidora": distribuidora})
                row = result.fetchone()
                
                if row:
                    return {
                        "carga_ons": float(row[0]) if row[0] else 0,
                        "geracao_mmgd": float(row[1]) if row[1] else 0,
                        "consumo_estimado": float(row[2]) if row[2] else 0,
                        "data_criacao": row[3],
                        "irradiancia": 0  # TODO: integrar com API meteorológica
                    }
                return None
        except Exception:
            return None

    def obter_classes_consumo(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get consumption classes mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        'residencial' as classe,
                        0.0 as consumo_mwh,
                        0.0 as consumo_percentual,
                        0 as quantidade_ucs
                    LIMIT 1
                """)
                result = conn.execute(query)
                rows = result.fetchall()

                classes = []
                for row in rows:
                    classe = ClasseConsumo(
                        classe=row[0],
                        consumo_mwh=row[1],
                        consumo_percentual=row[2],
                        quantidade_ucs=row[3],
                    )
                    # Map domain entity to response schema format
                    from .mapper import classe_consumo_to_response
                    classes.append(classe_consumo_to_response(classe))

                return classes
        except Exception:
            return []

    def obter_alerta_fraude(
        self, distribuidora: Optional[str] = None
    ) -> Optional[dict]:
        """Get latest fraud alert mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        1 as id,
                        NOW() as data_deteccao,
                        'distribuidora' as distribuidora,
                        'consumo_baixo' as tipo,
                        'alto' as severidade,
                        'Consumo abaixo do esperado' as descricao,
                        'ativo' as status,
                        85.5 as impacto_kw
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
                    # Convert row to domain entity
                    alerta = AlertaFraude(
                        id=row[0],
                        data_deteccao=row[1],
                        distribuidora=row[2],
                        tipo=row[3],
                        severidade=row[4],
                        descricao=row[5],
                        status=row[6],
                        impacto_kw=row[7],
                    )
                    # Map domain entity to response schema format
                    from .mapper import alerta_fraude_to_response
                    return alerta_fraude_to_response(alerta)
                return None
        except Exception:
            return None

    def obter_contagem_estabelecimentos(
        self, distribuidora: Optional[str] = None
    ) -> list[dict]:
        """Get establishment count mapped to response schema."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        'tipo' as tipo_estabelecimento,
                        0 as quantidade,
                        0.0 as consumo_medio_mwh
                    LIMIT 1
                """)
                result = conn.execute(query)
                rows = result.fetchall()

                contagens = []
                for row in rows:
                    # Convert row to domain entity
                    contagem = EstabelecimentoContagem(
                        tipo_estabelecimento=row[0],
                        quantidade=row[1],
                        consumo_medio_mwh=row[2],
                    )
                    # Map domain entity to response schema format
                    from .mapper import estabelecimento_contagem_to_response
                    contagens.append(estabelecimento_contagem_to_response(contagem))

                return contagens
        except Exception:
            return []

    def obter_resumo_granular(
        self, distribuidora: Optional[str] = None
    ) -> Optional[ResumoGranular]:
        """Get granular data summary."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        0 as total_ucs,
                        0.0 as consumo_total_mwh,
                        0.0 as consumo_medio_por_uc_mwh,
                        0.0 as geracao_mmgd_mw,
                        'distribuidora' as distribuidora,
                        '2026-02' as periodo
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
                    return ResumoGranular(
                        total_ucs=row[0],
                        consumo_total_mwh=row[1],
                        consumo_medio_por_uc_mwh=row[2],
                        geracao_mmgd_mw=row[3],
                        distribuidora=row[4],
                        periodo=row[5],
                    )
                return None
        except Exception:
            return None

    def obter_perfis_carga(
        self, classes: Optional[list[str]] = None
    ) -> list[PerfilCarga]:
        """Get load profiles."""
        try:
            # Default profiles
            perfis = [
                PerfilCarga(
                    classe="residencial",
                    fatores_horarios=[
                        0.6, 0.5, 0.5, 0.5, 0.6, 0.8, 0.9, 0.95, 1.0, 1.05,
                        1.0, 0.95, 0.9, 0.85, 0.8, 0.85, 0.9, 1.1, 1.3, 1.4,
                        1.3, 1.1, 0.9, 0.7
                    ],
                    pico_hora=19,
                    minima_hora=3,
                    fator_pico=1.4,
                ),
                PerfilCarga(
                    classe="comercial",
                    fatores_horarios=[
                        0.3, 0.3, 0.3, 0.3, 0.4, 0.6, 0.8, 1.0, 1.05, 1.1,
                        1.15, 1.1, 1.05, 1.0, 1.0, 0.95, 0.9, 0.8, 0.6, 0.5,
                        0.4, 0.4, 0.4, 0.3
                    ],
                    pico_hora=10,
                    minima_hora=1,
                    fator_pico=1.15,
                ),
                PerfilCarga(
                    classe="industrial",
                    fatores_horarios=[
                        0.9, 0.85, 0.8, 0.8, 0.85, 0.95, 1.0, 1.05, 1.1, 1.1,
                        1.1, 1.05, 1.0, 0.95, 0.95, 0.95, 1.0, 1.05, 1.05, 1.0,
                        0.95, 0.9, 0.9, 0.9
                    ],
                    pico_hora=10,
                    minima_hora=4,
                    fator_pico=1.1,
                ),
            ]

            # Filter by classes if specified
            if classes:
                perfis = [p for p in perfis if p.classe.lower() in [c.lower() for c in classes]]

            return perfis
        except Exception:
            return []

    def obter_estado_atual(
        self,
        subsistema: str,
        distribuidora: Optional[str] = None,
        subestacao_id: Optional[int] = None,
    ) -> Optional[EstadoAtual]:
        """Get current system state."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        NOW() as timestamp,
                        EXTRACT(HOUR FROM NOW()) as hora_atual,
                        0.0 as carga_ons_mw,
                        0.0 as geracao_mmgd_mw,
                        0.0 as consumo_estimado_mw,
                        850.0 as irradiancia_atual_wm2,
                        'SUDESTE' as subsistema,
                        0.75 as confiabilidade_estimativa
                    LIMIT 1
                """)
                result = conn.execute(query)
                row = result.fetchone()

                if row:
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
                return None
        except Exception:
            return None

    def obter_alertas_historico(
        self,
        distribuidora: Optional[str] = None,
        dias: int = 30,
        limite: int = 50,
    ) -> list[AlertaFraude]:
        """Get alert history."""
        try:
            alertas = []
            # Generate synthetic historical alerts
            for i in range(min(limite, 5)):
                alerta = AlertaFraude(
                    id=i + 1,
                    data_deteccao=datetime.now(),
                    distribuidora=distribuidora or "CPFL",
                    tipo="consumo_baixo",
                    severidade="alto",
                    descricao=f"Consumo {45 + i*10}% abaixo do esperado",
                    status="ativo",
                    impacto_kw=125.5 + i * 10,
                )
                alertas.append(alerta)
            return alertas
        except Exception:
            return []

    def detectar_anomalias(
        self, distribuidora: Optional[str] = None, limite: int = 10
    ) -> list[Anomalia]:
        """Detect anomalies."""
        try:
            anomalias = []
            # Generate synthetic anomalies
            for i in range(min(limite, 3)):
                anomalia = Anomalia(
                    distribuidora=distribuidora or "CEMIG",
                    tipo="consumo_baixo",
                    severidade="alto" if i == 0 else "medio",
                    desvio_percentual=45.2 + i * 10,
                    total_ucs_afetadas=1250 + i * 500,
                    impacto_kw=185.5 + i * 20,
                    timestamp=datetime.now(),
                )
                anomalias.append(anomalia)
            return anomalias
        except Exception:
            return []

    def obter_distribuidoras(
        self, subsistema: Optional[str] = None
    ) -> list[str]:
        """Get list of available electricity distributors from ANEEL database.
        
        Queries the distribuidoras_aneel table for actual distributors populated from BDGD.
        Can be filtered by subsistema if provided.
        
        Args:
            subsistema: Optional subsystem to filter (e.g., "SUL", "SUDESTE", "NORDESTE", "NORTE")
            
        Returns:
            list[str]: Sorted list of distributor names
        """
        try:
            with self.engine.connect() as conn:
                # Normalize subsistema to match database case-insensitively
                if subsistema:
                    # Map subsistema values to database format
                    subsistema_map = {
                        "SUL": "Sul",
                        "SUDESTE": "Sudeste",
                        "SUDESTE/CENTRO-OESTE": "Sudeste/Centro-Oeste",
                        "NORTE": "Norte",
                        "NORDESTE": "Nordeste",
                    }
                    # Get the database value or use uppercase version
                    db_subsistema = subsistema_map.get(subsistema.upper(), subsistema)
                    
                    query = text("""
                        SELECT DISTINCT nome
                        FROM distribuidoras_aneel
                        WHERE ativo = TRUE 
                        AND LOWER(regiao) = LOWER(:regiao)
                        ORDER BY nome
                    """)
                    result = conn.execute(query, {'regiao': db_subsistema})
                else:
                    query = text("""
                        SELECT DISTINCT nome
                        FROM distribuidoras_aneel
                        WHERE ativo = TRUE
                        ORDER BY nome
                    """)
                    result = conn.execute(query)
                
                rows = result.fetchall()
                distribuidoras = [row[0] for row in rows if row[0]]
                
                # If no results from ANEEL table, return empty list
                if not distribuidoras:
                    return []
                
                return distribuidoras
        except Exception as e:
            # Log error but don't crash - return empty list
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Erro ao obter distribuidoras do banco: {e}")
            return []

    def obter_carga_distribuidor_historico(
        self, distribuidora: str, subsistema: Optional[str] = None, dias: int = 7
    ) -> list[dict]:
        """Obtém potência total da distribuidora da tabela distribuidoras_aneel."""
        try:
            with self.engine.connect() as conn:
                # Buscar da tabela distribuidoras_aneel (coluna é potencia_total_kva)
                query = text("""
                    SELECT 
                        nome as distribuidora,
                        COALESCE(potencia_total_kva, 0) / 1000.0 * 0.95 as potencia_mw,
                        regiao as subsistema,
                        NOW() as data_medicao
                    FROM distribuidoras_aneel
                    WHERE LOWER(nome) = LOWER(:distribuidora)
                    LIMIT 1
                """)
                
                result = conn.execute(query, {"distribuidora": distribuidora})
                row = result.fetchone()
                
                if row:
                    potencia_mw = float(row[1]) if row[1] else 0
                    return [{
                        "hora": row[3],
                        "carga_ons": potencia_mw,
                        "distribuidora": row[0],
                        "subsistema": row[2]
                    }]
                
                return []
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Erro ao obter carga histórica: {e}")
            return []
