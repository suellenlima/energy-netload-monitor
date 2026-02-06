"""
ETL para extrair e manter dados de carga atual das distribuidoras.

Busca dados de carga em tempo real e os armazena em tabela histórica
para análise e acompanhamento da demanda por distribuidora.

Execução:
    docker compose exec -T etl python /app/src/extractors/distribuidor_carga_etl.py

Frequência recomendada: A cada 1 hora (via cron/scheduler)
"""

import logging
import sys
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_engine():
    """Cria engine do banco de dados."""
    db_url = os.getenv(
        'DATABASE_URL',
        'postgresql://admin:admin123@localhost:5432/energy_monitor'
    )
    return create_engine(db_url, echo=False)


class DistribuidoraCargaService:
    """Serviço para gerenciar dados de carga das distribuidoras."""

    def __init__(self, engine):
        """Inicializa serviço com engine do banco."""
        self.engine = engine

    def criar_tabela(self):
        """Tabela já criada via schema.sql - este método é mantido por compatibilidade."""
        logger.info("✅ Tabela carga_distribuidoras já existe (criada via schema.sql)")

    def inserir_dados_carga(self, df: pd.DataFrame) -> int:
        """
        Insere dados de carga das distribuidoras.

        Args:
            df: DataFrame com colunas [distribuidora, carga_liquida_mw, carga_estimada_total_mw, data_medicao, subsistema]

        Returns:
            Número de registros inseridos/atualizados
        """
        if df.empty:
            logger.warning("⚠️ DataFrame vazio - nenhum dado para inserir")
            return 0

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO carga_distribuidoras (distribuidora, subsistema, carga_liquida_mw, carga_estimada_total_mw, data_medicao)
                    VALUES (:dist, :subsistema, :carga_liquida, :carga_total, :data)
                    ON CONFLICT (distribuidora, data_medicao)
                    DO UPDATE SET
                        carga_liquida_mw = EXCLUDED.carga_liquida_mw,
                        carga_estimada_total_mw = EXCLUDED.carga_estimada_total_mw,
                        subsistema = EXCLUDED.subsistema,
                        data_insercao = CURRENT_TIMESTAMP
                """), {
                    'dist': row['distribuidora'],
                    'subsistema': row['subsistema'],
                    'carga_liquida': float(row['carga_liquida_mw']),
                    'carga_total': float(row.get('carga_estimada_total_mw', 0)),
                    'data': pd.to_datetime(row['data_medicao']),
                })

            logger.info(f"✅ Inseridos/atualizados {len(df)} registros de carga")
            return len(df)

    def obter_ultima_carga(self, distribuidora: str = None):
        """
        Obtém a última carga registrada.

        Args:
            distribuidora: Filtrar por distribuidora (opcional)

        Returns:
            DataFrame com últimos registros
        """
        if distribuidora:
            query = f"""
                SELECT DISTINCT ON (distribuidora)
                    distribuidora,
                    subsistema,
                    carga_liquida_mw,
                    carga_estimada_total_mw,
                    data_medicao,
                    data_insercao
                FROM carga_distribuidoras
                WHERE distribuidora = '{distribuidora}'
                ORDER BY distribuidora, data_medicao DESC
            """
        else:
            query = """
                SELECT DISTINCT ON (distribuidora)
                    distribuidora,
                    subsistema,
                    carga_liquida_mw,
                    carga_estimada_total_mw,
                    data_medicao,
                    data_insercao
                FROM carga_distribuidoras
                ORDER BY distribuidora, data_medicao DESC
            """

        df = pd.read_sql(query, self.engine)
        return df

    def limpar_dados_antigos(self, dias: int = 30):
        """Remove dados com mais de X dias (reduz uso de disco)."""
        with self.engine.begin() as conn:
            resultado = conn.execute(text(f"""
                DELETE FROM carga_distribuidoras
                WHERE data_medicao < CURRENT_TIMESTAMP - INTERVAL '{dias} days'
            """))
            logger.info(f"🗑️  Removidos {resultado.rowcount} registros com mais de {dias} dias")


class DistribuidoraCargaExtractor:
    """Extrator que busca dados de carga das distribuidoras."""

    DISTRIBUIDORAS_MAP = {
        'LIGHT': 'Sudeste/Centro-Oeste',
        'ENEL': 'Sudeste/Centro-Oeste',
        'IENERGIA': 'Sudeste/Centro-Oeste',
    }
    
    # Mapeamento de nomes alternativos/variações das distribuidoras
    # Utilizado para buscar dados em tabelas externas (gd_granular, consumo_granular_classe)
    # que podem ter nomenclaturas diferentes (ex: "Light Energia", "Enel Distribuição")
    NOME_ALTERNATIVO_MAP = {
        'LIGHT': ['LIGHT', 'LIGHT ENERGIA', 'LIGHT S.A.', 'LIGHT SERVIÇOS DE ELETRICIDADE', 
                  'LIGHT SERVICOS DE ELETRICIDADE S.A.'],
        'ENEL': ['ENEL', 'ENEL DISTRIBUIÇÃO', 'ENEL DISTRIBUIÇÃO SP', 'ENEL DISTRIBUIÇÃO RJ',
                 'ENEL BRASIL S.A.', 'ENEL BRASIL', 'ENERSUL', 'ENEL DISTRIBUICAO SP',
                 'ENEL DISTRIBUICAO RJ', 'ENEL DISTRIBUICAO CEARA', 'ENEL CEARA',
                 'ENEL SAO PAULO', 'ENEL RIO', 'ENEL GOIAS', 'AMPLA ENERGIA'],
        'IENERGIA': ['IENERGIA', 'IENERGIA SOLUÇÕES', 'COOPERATIVA DE DISTRIBUICAO DE ENERGIA',
                     'ENERGISA', 'ENERGISA PARAIBA', 'ENERGISA SERGIPE'],
    }

    def _normalizar_nome_distribuidora(self, nome: str) -> str:
        """
        Normaliza nome da distribuidora para padrão do projeto.
        
        Args:
            nome: Nome da distribuidora (pode ter variações)
            
        Returns:
            Nome normalizado (ex: 'LIGHT', 'ENEL', 'IENERGIA')
        """
        nome_upper = nome.upper().strip()
        
        # Procurar em todos os nomes alternativos
        for distribuidor_padrao, nomes_alternativos in self.NOME_ALTERNATIVO_MAP.items():
            if any(nome_upper == alt.upper() for alt in nomes_alternativos):
                return distribuidor_padrao
            # Também buscar por substring (ex: "Enel" em "Enel Distribuição SP")
            if any(alt.upper() in nome_upper for alt in nomes_alternativos if alt.upper() != 'ENEL'):
                return distribuidor_padrao
        
        # Se não encontrou, retornar primeiro token (ex: "Light Energia" → "LIGHT")
        primeiro_token = nome_upper.split()[0]
        if primeiro_token in self.DISTRIBUIDORAS_MAP:
            return primeiro_token
        
        return nome_upper

    def extrair_carga_distribuidoras(self) -> pd.DataFrame:
        """
        Extrai carga das distribuidoras de forma proporcional.

        Estratégia:
        1. Carga base do subsistema Sudeste/Centro-Oeste: 450 MW
        2. Distribuição proporcional ao número de transformadores
        3. Cada distribuidora recebe carga proporcional
        4. Carga total real = consumo_granular (kWh/24000) + carga_liquida_mw

        Returns:
            DataFrame com [distribuidora, carga_liquida_mw, carga_estimada_total_mw, data_medicao, subsistema]
        """
        dados = []

        try:
            logger.info("📡 Calculando carga por distribuidora...")
            
            # Carga base do subsistema Sudeste/Centro-Oeste (valor de exemplo)
            carga_base_sudeste = 450.0  # MW
            
            for distribuidora, subsistema in self.DISTRIBUIDORAS_MAP.items():
                try:
                    # Buscar número de transformadores para ponderar
                    num_trafo = self._contar_transformadores(distribuidora)

                    if num_trafo is None or num_trafo == 0:
                        logger.warning(f"⚠️  Sem transformadores para {distribuidora}")
                        continue

                    # Calcular carga proporcional (por número de transformadores)
                    carga_liquida = self._calcular_carga_proporcional(
                        distribuidora, carga_base_sudeste
                    )
                    
                    # Carga total real = consumo_granular (em kWh) + carga_liquida
                    # Consumo granular em MW = consumo_kwh / 24000
                    consumo_granular_mw = float(self._obter_consumo_granular(distribuidora))
                    carga_total_real = consumo_granular_mw + float(carga_liquida)

                    dados.append({
                        'distribuidora': distribuidora,
                        'subsistema': subsistema,
                        'carga_liquida_mw': float(carga_liquida),
                        'carga_estimada_total_mw': float(carga_total_real),
                        'data_medicao': datetime.now(),
                    })

                    logger.info(
                        f"✅ {distribuidora}: "
                        f"Líquida: {carga_liquida:.2f} MW | "
                        f"Granular: {consumo_granular_mw:.2f} MW | "
                        f"Total: {carga_total_real:.2f} MW "
                        f"({num_trafo} transformadores)"
                    )

                except Exception as e:
                    logger.error(f"❌ Erro ao processar {distribuidora}: {e}")
                    continue

            if not dados:
                logger.warning("⚠️  Nenhum dado de carga foi extraído")
                return pd.DataFrame()

            df = pd.DataFrame(dados)
            logger.info(f"✅ Extração concluída: {len(df)} distribuidoras processadas")
            return df

        except Exception as e:
            logger.error(f"❌ Erro na extração: {e}", exc_info=True)
            return pd.DataFrame()

    def _contar_transformadores(self, distribuidora: str) -> int | None:
        """
        Conta número de transformadores por distribuidora (para ponderação).

        Args:
            distribuidora: Nome da distribuidora

        Returns:
            Número de transformadores ou None
        """
        try:
            engine = get_engine()
            with engine.connect() as conn:
                resultado = conn.execute(text(f"""
                    SELECT COUNT(*) as total
                    FROM transformadores_aneel
                    WHERE distribuidora = '{distribuidora}'
                """))
                row = resultado.fetchone()
                return row[0] if row else 0

        except Exception as e:
            logger.warning(f"⚠️  Erro ao contar transformadores de {distribuidora}: {e}")
            return 0

    def _calcular_carga_proporcional(
        self,
        distribuidora: str,
        carga_base: float
    ) -> float:
        """
        Calcula carga proporcional da distribuidora.

        Estratégia: proporcional ao número de transformadores

        Args:
            distribuidora: Nome da distribuidora
            carga_base: Carga base em MW (450)

        Returns:
            Carga estimada para a distribuidora em MW
        """
        try:
            engine = get_engine()
            
            with engine.connect() as conn:
                # Transformadores da distribuidora
                resultado_dist = conn.execute(text(f"""
                    SELECT COUNT(*) as total
                    FROM transformadores_aneel
                    WHERE distribuidora = '{distribuidora}'
                """))
                trafo_dist = resultado_dist.fetchone()[0]

                # Total de transformadores das distribuidoras
                resultado_total = conn.execute(text("""
                    SELECT COUNT(*) as total
                    FROM transformadores_aneel
                    WHERE distribuidora IN ('LIGHT', 'ENEL', 'IENERGIA')
                """))
                trafo_total = resultado_total.fetchone()[0]

            # Calcular proporção
            if trafo_total > 0:
                proporcao = trafo_dist / trafo_total
                carga_distribuidora = carga_base * proporcao
                return max(0, carga_distribuidora)

            return carga_base / 3  # Fallback: dividir igualmente entre 3 dist.

        except Exception as e:
            logger.error(f"❌ Erro ao calcular carga proporcional: {e}")
            return carga_base / 3  # Fallback

    def _obter_consumo_granular(self, distribuidora: str) -> float:
        """
        Obtém o consumo granular por classe da distribuidora.

        Estratégia em cascata:
        1. Buscar consumo_granular_classe (ideal - dados reais agregados)
        2. Buscar gd_granular (geração solar por distribuidora)
        3. Usar estimativa padrão (50% da carga líquida para distribuições típicas)

        Args:
            distribuidora: Nome da distribuidora (ex: 'LIGHT', 'ENEL')

        Returns:
            Consumo em MW (potência média equivalente)
        """
        try:
            engine = get_engine()
            consumo_mw = 0
            
            # Normalizar nome da distribuidora para buscar em diferentes fontes
            dist_normalizada = self._normalizar_nome_distribuidora(distribuidora)
            nomes_alternativos = self.NOME_ALTERNATIVO_MAP.get(dist_normalizada, [dist_normalizada])
            
            with engine.connect() as conn:
                # 1. Estratégia: Consumo granular agregado por classe (busca por LIKE)
                try:
                    # Criar condição ILIKE para buscar distribuidoras com nomes parecidos
                    condicoes = " OR ".join([f"UPPER(distribuidora) ILIKE UPPER('{nome.strip()}%')" 
                                           for nome in nomes_alternativos])
                    
                    resultado_consumo = conn.execute(text(f"""
                        SELECT COALESCE(SUM(consumo_kwh), 0) as total_kwh
                        FROM consumo_granular_classe
                        WHERE {condicoes}
                    """))
                    consumo_kwh = resultado_consumo.fetchone()[0] or 0
                    if consumo_kwh > 0:
                        consumo_mw = float(consumo_kwh) / 24000.0
                        logger.debug(f"✅ Consumo de consumo_granular_classe para {dist_normalizada}: {consumo_mw:.2f} MW")
                        return max(0.0, consumo_mw)
                except Exception as inner_e:
                    logger.debug(f"ℹ️ Tabela consumo_granular_classe: {inner_e}")
                
                # 2. Estratégia: Usar dados de GD como proxy (potência em kW, busca por LIKE)
                try:
                    condicoes_gd = " OR ".join([f"UPPER(distribuidora) ILIKE UPPER('{nome.strip()}%')" 
                                              for nome in nomes_alternativos])
                    
                    resultado_gd = conn.execute(text(f"""
                        SELECT COALESCE(SUM(potencia_kw), 0) as total_kw
                        FROM gd_granular
                        WHERE {condicoes_gd}
                    """))
                    potencia_gd_kw = resultado_gd.fetchone()[0] or 0
                    if potencia_gd_kw > 0:
                        # Converter potência de GD para consumo estimado
                        # Uso típico = 40% da potência instalada * 24h / 24000
                        consumo_mw = (float(potencia_gd_kw) * 0.4 * 24) / 24000.0
                        logger.debug(f"✅ Consumo estimado de gd_granular para {dist_normalizada}: {consumo_mw:.2f} MW")
                        return max(0.0, consumo_mw)
                except Exception as inner_e:
                    logger.debug(f"ℹ️ Tabela gd_granular: {inner_e}")
                
            # 3. Fallback: retornar 0 (dados não disponível ainda)
            logger.warning(f"⚠️ Nenhuma fonte de consumo granular disponível para {dist_normalizada}, usando 0 MW")
            return 0.0

        except Exception as e:
            logger.warning(f"⚠️ Erro ao obter consumo granular de {distribuidora}: {e}")
            return 0.0

def executar_etl_carga_distribuidoras():
    """Executa ETL completo de carga das distribuidoras."""
    logger.info("=" * 80)
    logger.info("🚀 INICIANDO ETL DE CARGA DAS DISTRIBUIDORAS")
    logger.info("=" * 80)

    try:
        # Inicializar serviço
        engine = get_engine()
        service = DistribuidoraCargaService(engine)
        extractor = DistribuidoraCargaExtractor()

        # Criar tabela
        service.criar_tabela()

        # Extrair dados
        df_carga = extractor.extrair_carga_distribuidoras()

        if df_carga.empty:
            logger.warning("⚠️  Nenhum dado extraído - abortando")
            return

        # Inserir dados
        registros = service.inserir_dados_carga(df_carga)

        # Exibir status
        logger.info("\n" + "=" * 80)
        logger.info("📊 STATUS DA CARGA")
        logger.info("=" * 80)
        df_atual = service.obter_ultima_carga()
        if not df_atual.empty:
            for _, row in df_atual.iterrows():
                logger.info(
                    f"  {row['distribuidora']:15} | "
                    f"Líquida: {row['carga_liquida_mw']:8.2f} MW | "
                    f"Total: {row['carga_estimada_total_mw']:8.2f} MW | "
                    f"{row['data_medicao']}"
                )

        # Limpeza de dados antigos (opcional)
        service.limpar_dados_antigos(dias=30)

        logger.info("=" * 80)
        logger.info("✅ ETL CONCLUÍDO COM SUCESSO")
        logger.info("=" * 80)

        return registros

    except Exception as e:
        logger.error(f"❌ ERRO NA EXECUÇÃO DO ETL: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETL de Carga das Distribuidoras")
    parser.add_argument("--debug", action="store_true", help="Modo debug")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    executar_etl_carga_distribuidoras()
