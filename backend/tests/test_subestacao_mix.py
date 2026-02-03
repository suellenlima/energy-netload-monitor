"""
Testes para mix de consumidores por subestação.

Valida que o mix de consumidores está correto:
- Retorna dados para subestações com UCs associadas
- Quantidades são não-negativas
- Totais são coerentes com detalhamento
- Potência total faz sentido
"""

import pytest
from unittest.mock import MagicMock, patch

from src.services.subestacoes_clustering import (
    get_uc_mix_by_subestacao,
    associate_ucs_to_nearest_subestacao,
)


class TestMixBasico:
    """Testes básicos do mix de consumidores."""

    @pytest.fixture
    def mock_db_result(self):
        """Mock de resultado do banco de dados."""
        return [
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="residencia",
                qtd_instalacoes=100,
                qtd_unidades=1000,
                potencia_total_mw=1.5,
            ),
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="predio_residencial",
                qtd_instalacoes=10,
                qtd_unidades=150,
                potencia_total_mw=0.3,
            ),
            MagicMock(
                classe_consumo="Comercial",
                tipo_estabelecimento="comercio",
                qtd_instalacoes=20,
                qtd_unidades=50,
                potencia_total_mw=0.8,
            ),
        ]

    def test_mix_retorna_dict(self, mock_db_result):
        """Verifica que mix retorna um dicionário."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            assert isinstance(mix, dict), "Mix deveria retornar dict"

    def test_mix_contem_classes(self, mock_db_result):
        """Verifica que mix contém as classes esperadas."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            assert "Residencial" in mix, "Falta classe Residencial"
            assert "Comercial" in mix, "Falta classe Comercial"

    def test_mix_campos_obrigatorios(self, mock_db_result):
        """Verifica que cada classe tem campos obrigatórios."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            for classe, dados in mix.items():
                assert "qtd_instalacoes" in dados, f"{classe}: falta qtd_instalacoes"
                assert "qtd_unidades_consumidoras" in dados, f"{classe}: falta qtd_unidades_consumidoras"
                assert "potencia_total_mw" in dados, f"{classe}: falta potencia_total_mw"


class TestAgregacao:
    """Testes para agregação de dados por classe."""

    @pytest.fixture
    def mock_db_multiplos_tipos(self):
        """Mock com múltiplos tipos da mesma classe."""
        return [
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="residencia",
                qtd_instalacoes=100,
                qtd_unidades=1000,
                potencia_total_mw=1.5,
            ),
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="predio_residencial",
                qtd_instalacoes=10,
                qtd_unidades=150,
                potencia_total_mw=0.3,
            ),
        ]

    def test_agregacao_por_classe(self, mock_db_multiplos_tipos):
        """Verifica que tipos da mesma classe são agregados."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_multiplos_tipos

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            # Residencial deve agregar residencia + predio_residencial
            residencial = mix["Residencial"]

            assert residencial["qtd_instalacoes"] == 110, (
                "Instalações não agregadas corretamente"
            )
            assert residencial["qtd_unidades_consumidoras"] == 1150, (
                "UCs não agregadas corretamente"
            )
            assert abs(residencial["potencia_total_mw"] - 1.8) < 0.01, (
                "Potência não agregada corretamente"
            )

    def test_detalhamento_por_tipo(self, mock_db_multiplos_tipos):
        """Verifica que detalhamento por tipo está presente."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_multiplos_tipos

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            residencial = mix["Residencial"]

            assert "por_tipo" in residencial, "Falta detalhamento por_tipo"
            assert "residencia" in residencial["por_tipo"], "Falta tipo residencia"
            assert "predio_residencial" in residencial["por_tipo"], "Falta tipo predio_residencial"


class TestValoresNumericos:
    """Testes para validação de valores numéricos."""

    @pytest.fixture
    def mock_db_result(self):
        """Mock de resultado válido."""
        return [
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="residencia",
                qtd_instalacoes=100,
                qtd_unidades=1000,
                potencia_total_mw=1.5,
            ),
        ]

    def test_quantidades_nao_negativas(self, mock_db_result):
        """Verifica que todas as quantidades são não-negativas."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            for classe, dados in mix.items():
                assert dados["qtd_instalacoes"] >= 0, (
                    f"{classe}: instalações negativas"
                )
                assert dados["qtd_unidades_consumidoras"] >= 0, (
                    f"{classe}: UCs negativas"
                )
                assert dados["potencia_total_mw"] >= 0, (
                    f"{classe}: potência negativa"
                )

    def test_ucs_maior_ou_igual_instalacoes(self, mock_db_result):
        """Verifica que UCs >= instalações (cada instalação tem pelo menos 1 UC)."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            for classe, dados in mix.items():
                assert dados["qtd_unidades_consumidoras"] >= dados["qtd_instalacoes"], (
                    f"{classe}: UCs ({dados['qtd_unidades_consumidoras']}) < "
                    f"instalações ({dados['qtd_instalacoes']})"
                )

    def test_potencia_media_razoavel(self, mock_db_result):
        """Verifica que potência média por UC está em range razoável."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            for classe, dados in mix.items():
                if dados["qtd_unidades_consumidoras"] == 0:
                    continue

                pot_media_kw = (dados["potencia_total_mw"] * 1000) / dados["qtd_unidades_consumidoras"]

                # Potência média razoável: 0.1 kW a 50 kW por UC
                # (residencial ~1-5 kW, comercial ~5-20 kW, industrial ~10-50 kW)
                assert 0.1 <= pot_media_kw <= 50, (
                    f"{classe}: potência média por UC ({pot_media_kw:.2f} kW) fora do range"
                )


class TestTotais:
    """Testes para validação de totais."""

    @pytest.fixture
    def mock_db_result(self):
        """Mock de resultado para testes de totais."""
        return [
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="residencia",
                qtd_instalacoes=100,
                qtd_unidades=1000,
                potencia_total_mw=1.5,
            ),
            MagicMock(
                classe_consumo="Comercial",
                tipo_estabelecimento="comercio",
                qtd_instalacoes=20,
                qtd_unidades=50,
                potencia_total_mw=0.8,
            ),
        ]

    def test_totais_presentes(self, mock_db_result):
        """Verifica que resposta contém totais."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            response = get_uc_mix_by_subestacao(subestacao_id=1, include_totals=True)

            assert "totais" in response, "Faltam totais na resposta"

    def test_totais_sao_soma_classes(self, mock_db_result):
        """Verifica que totais são a soma de todas as classes."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_db_result

            response = get_uc_mix_by_subestacao(subestacao_id=1, include_totals=True)

            mix = response["mix"]
            totais = response["totais"]

            # Somar manualmente
            total_instalacoes = sum(dados["qtd_instalacoes"] for dados in mix.values())
            total_ucs = sum(dados["qtd_unidades_consumidoras"] for dados in mix.values())
            total_mw = sum(dados["potencia_total_mw"] for dados in mix.values())

            assert totais["qtd_instalacoes"] == total_instalacoes, (
                "Total de instalações incorreto"
            )
            assert totais["qtd_unidades_consumidoras"] == total_ucs, (
                "Total de UCs incorreto"
            )
            assert abs(totais["potencia_total_mw"] - total_mw) < 0.01, (
                "Total de potência incorreto"
            )


class TestCasosEspeciais:
    """Testes para casos especiais e edge cases."""

    def test_subestacao_sem_ucs_retorna_vazio(self):
        """Verifica que subestação sem UCs retorna dict vazio."""
        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = []  # Sem resultados

            mix = get_uc_mix_by_subestacao(subestacao_id=999)

            assert isinstance(mix, dict), "Deveria retornar dict"
            assert len(mix) == 0, "Dict deveria estar vazio"

    def test_subestacao_apenas_uma_classe(self):
        """Verifica comportamento com apenas uma classe."""
        mock_result = [
            MagicMock(
                classe_consumo="Residencial",
                tipo_estabelecimento="residencia",
                qtd_instalacoes=10,
                qtd_unidades=100,
                potencia_total_mw=0.15,
            ),
        ]

        with patch("src.services.subestacoes_clustering._fetch_mix_from_db") as mock_fetch:
            mock_fetch.return_value = mock_result

            mix = get_uc_mix_by_subestacao(subestacao_id=1)

            assert len(mix) == 1, "Deveria ter apenas 1 classe"
            assert "Residencial" in mix, "Falta classe Residencial"


class TestAssociacaoUCs:
    """Testes para associação de UCs a subestações."""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock de conexão com banco de dados."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor

    def test_associacao_retorna_estatisticas(self, mock_db_connection):
        """Verifica que associação retorna estatísticas."""
        mock_conn, mock_cursor = mock_db_connection
        mock_cursor.fetchone.return_value = MagicMock(
            associadas=4500,
            nao_associadas=500,
            total=5000,
        )

        with patch("src.services.subestacoes_clustering._get_db_connection") as mock_get_conn:
            mock_get_conn.return_value = mock_conn

            stats = associate_ucs_to_nearest_subestacao(raio_km=10.0)

            assert "ucs_associadas" in stats, "Falta ucs_associadas"
            assert "ucs_sem_subestacao" in stats, "Falta ucs_sem_subestacao"
            assert "total_ucs" in stats, "Falta total_ucs"
            assert "percentual_associado" in stats, "Falta percentual_associado"

    def test_associacao_calcula_percentual(self, mock_db_connection):
        """Verifica que associação calcula percentual corretamente."""
        mock_conn, mock_cursor = mock_db_connection
        mock_cursor.fetchone.return_value = MagicMock(
            associadas=4500,
            nao_associadas=500,
            total=5000,
        )

        with patch("src.services.subestacoes_clustering._get_db_connection") as mock_get_conn:
            mock_get_conn.return_value = mock_conn

            stats = associate_ucs_to_nearest_subestacao(raio_km=10.0)

            percentual_esperado = (4500 / 5000) * 100
            percentual_retornado = stats["percentual_associado"]

            assert abs(percentual_retornado - percentual_esperado) < 0.1, (
                f"Percentual incorreto: esperado {percentual_esperado:.1f}%, "
                f"retornado {percentual_retornado:.1f}%"
            )

    def test_associacao_com_raio_invalido(self):
        """Verifica que raio inválido levanta erro."""
        with pytest.raises(ValueError):
            associate_ucs_to_nearest_subestacao(raio_km=-1)  # Raio negativo

        with pytest.raises(ValueError):
            associate_ucs_to_nearest_subestacao(raio_km=0)  # Raio zero

    def test_associacao_percentual_alto(self, mock_db_connection):
        """Verifica que percentual de associação é alto (>70%)."""
        mock_conn, mock_cursor = mock_db_connection
        mock_cursor.fetchone.return_value = MagicMock(
            associadas=4500,
            nao_associadas=500,
            total=5000,
        )

        with patch("src.services.subestacoes_clustering._get_db_connection") as mock_get_conn:
            mock_get_conn.return_value = mock_conn

            stats = associate_ucs_to_nearest_subestacao(raio_km=10.0)

            # Meta: 70%+ das UCs associadas
            assert stats["percentual_associado"] >= 70, (
                f"Percentual de associação ({stats['percentual_associado']:.1f}%) "
                "abaixo da meta de 70%"
            )


class TestConsistenciaEspacial:
    """Testes de consistência espacial."""

    def test_raio_maior_associa_mais_ucs(self):
        """Verifica que raio maior associa mais UCs."""
        # Mock com resultados diferentes para raios diferentes
        def mock_fetch(raio):
            if raio == 5.0:
                return MagicMock(associadas=3000, nao_associadas=2000, total=5000)
            elif raio == 15.0:
                return MagicMock(associadas=4500, nao_associadas=500, total=5000)

        with patch("src.services.subestacoes_clustering._get_db_connection") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            mock_cursor.fetchone.side_effect = [
                mock_fetch(5.0),
                mock_fetch(15.0),
            ]

            stats_5km = associate_ucs_to_nearest_subestacao(raio_km=5.0)
            stats_15km = associate_ucs_to_nearest_subestacao(raio_km=15.0)

            assert stats_15km["ucs_associadas"] >= stats_5km["ucs_associadas"], (
                "Raio maior deveria associar mais ou igual UCs"
            )


# Marcadores para execução seletiva
pytestmark = pytest.mark.integration
