"""
Testes para carga sintética por subestação.

Valida que o cálculo de carga sintética está correto:
- Retorna 24 valores (curva horária completa)
- Valores não negativos
- Fator de carga razoável
- Soma energética coerente
"""

import pytest
from unittest.mock import MagicMock, patch

from src.services.synthetic_load import (
    calculate_synthetic_load_by_subestacao,
    calculate_load_at_hour,
    CONSUMO_MEDIO_PADRAO_KWH_MES,
)


class TestCalculoBasico:
    """Testes básicos do cálculo de carga sintética."""

    @pytest.fixture
    def mock_mix_simples(self):
        """Mix de consumidores simplificado para testes."""
        return {
            "Residencial": {
                "qtd_instalacoes": 100,
                "qtd_unidades_consumidoras": 1000,
                "potencia_total_mw": 1.5,
            }
        }

    def test_curva_tem_24_valores(self, mock_mix_simples):
        """Verifica que a curva horária tem 24 valores."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva = resultado["curva_horaria_kw"]

            assert len(curva) == 24, "Curva não tem 24 valores"

    def test_valores_nao_negativos(self, mock_mix_simples):
        """Verifica que todos os valores da curva são não-negativos."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva = resultado["curva_horaria_kw"]

            assert all(v >= 0 for v in curva), "Curva contém valores negativos"

    def test_curva_tem_variacao(self, mock_mix_simples):
        """Verifica que a curva não é constante (tem variação)."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva = resultado["curva_horaria_kw"]

            # Pelo menos 2 valores diferentes
            valores_unicos = set(curva)
            assert len(valores_unicos) > 1, "Curva é constante (sem variação)"


class TestEstatisticas:
    """Testes para estatísticas calculadas (pico, vale, média, etc)."""

    @pytest.fixture
    def mock_mix_residencial(self):
        """Mix apenas residencial para testes."""
        return {
            "Residencial": {
                "qtd_instalacoes": 100,
                "qtd_unidades_consumidoras": 5000,
                "potencia_total_mw": 7.5,
            }
        }

    def test_pico_maior_que_vale(self, mock_mix_residencial):
        """Verifica que pico é sempre maior que vale."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_residencial

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            stats = resultado["estatisticas"]

            assert stats["pico_kw"] > stats["vale_kw"], (
                "Pico deveria ser maior que vale"
            )

    def test_media_entre_pico_e_vale(self, mock_mix_residencial):
        """Verifica que média está entre pico e vale."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_residencial

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            stats = resultado["estatisticas"]

            assert stats["vale_kw"] <= stats["media_kw"] <= stats["pico_kw"], (
                "Média deveria estar entre vale e pico"
            )

    def test_amplitude_coerente(self, mock_mix_residencial):
        """Verifica que amplitude = pico - vale."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_residencial

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            stats = resultado["estatisticas"]

            amplitude_calculada = stats["pico_kw"] - stats["vale_kw"]
            amplitude_retornada = stats["amplitude_kw"]

            assert abs(amplitude_calculada - amplitude_retornada) < 0.01, (
                f"Amplitude inconsistente: "
                f"calculada={amplitude_calculada:.2f}, retornada={amplitude_retornada:.2f}"
            )

    def test_fator_carga_valido(self, mock_mix_residencial):
        """Verifica que fator de carga está entre 0 e 1."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_residencial

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            stats = resultado["estatisticas"]

            assert 0 < stats["fator_carga"] <= 1, (
                f"Fator de carga inválido: {stats['fator_carga']:.3f}"
            )

    def test_fator_carga_calculo(self, mock_mix_residencial):
        """Verifica fórmula do fator de carga: média / pico."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_residencial

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            stats = resultado["estatisticas"]

            fator_calculado = stats["media_kw"] / stats["pico_kw"]
            fator_retornado = stats["fator_carga"]

            assert abs(fator_calculado - fator_retornado) < 0.01, (
                f"Fator de carga incorreto: "
                f"calculado={fator_calculado:.3f}, retornado={fator_retornado:.3f}"
            )


class TestMultiplasClasses:
    """Testes com mix de múltiplas classes."""

    @pytest.fixture
    def mock_mix_misto(self):
        """Mix com múltiplas classes."""
        return {
            "Residencial": {
                "qtd_instalacoes": 100,
                "qtd_unidades_consumidoras": 5000,
                "potencia_total_mw": 7.5,
            },
            "Comercial": {
                "qtd_instalacoes": 20,
                "qtd_unidades_consumidoras": 300,
                "potencia_total_mw": 3.0,
            },
            "Industrial": {
                "qtd_instalacoes": 2,
                "qtd_unidades_consumidoras": 10,
                "potencia_total_mw": 5.0,
            },
        }

    def test_contribuicao_por_classe_existe(self, mock_mix_misto):
        """Verifica que contribuição por classe está presente."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_misto

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            contrib = resultado["contribuicao_por_classe"]

            assert "Residencial" in contrib, "Falta contribuição Residencial"
            assert "Comercial" in contrib, "Falta contribuição Comercial"
            assert "Industrial" in contrib, "Falta contribuição Industrial"

    def test_soma_contribuicoes_igual_total(self, mock_mix_misto):
        """Verifica que soma das contribuições = carga total."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_misto

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva_total = resultado["curva_horaria_kw"]
            contrib = resultado["contribuicao_por_classe"]

            # Para cada hora, soma das contribuições = total
            for hora in range(24):
                soma_contrib = sum(
                    contrib[classe]["curva_horaria_kw"][hora]
                    for classe in contrib
                )
                total_hora = curva_total[hora]

                assert abs(soma_contrib - total_hora) < 0.1, (
                    f"Hora {hora}: soma contribuições ({soma_contrib:.2f}) "
                    f"!= total ({total_hora:.2f})"
                )


class TestCalculoPorHora:
    """Testes para cálculo de carga em uma hora específica."""

    @pytest.fixture
    def mock_mix_simples(self):
        """Mix simplificado para testes."""
        return {
            "Residencial": {
                "qtd_instalacoes": 100,
                "qtd_unidades_consumidoras": 1000,
                "potencia_total_mw": 1.5,
            }
        }

    def test_hora_invalida_levanta_erro(self, mock_mix_simples):
        """Verifica que hora inválida levanta ValueError."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            with pytest.raises(ValueError):
                calculate_load_at_hour(subestacao_id=1, hora=24)  # Hora inválida

            with pytest.raises(ValueError):
                calculate_load_at_hour(subestacao_id=1, hora=-1)  # Hora inválida

    def test_hora_valida_retorna_valor(self, mock_mix_simples):
        """Verifica que hora válida retorna um valor."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            carga = calculate_load_at_hour(subestacao_id=1, hora=12)
            assert isinstance(carga, (int, float)), "Carga não é numérica"
            assert carga >= 0, "Carga não pode ser negativa"

    def test_hora_pico_residencial(self, mock_mix_simples):
        """Verifica que hora de pico residencial (20h) tem valor alto."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_simples

            carga_pico = calculate_load_at_hour(subestacao_id=1, hora=20)  # Pico residencial
            carga_vale = calculate_load_at_hour(subestacao_id=1, hora=3)   # Vale madrugada

            assert carga_pico > carga_vale, (
                "Hora de pico residencial deveria ter carga maior que vale"
            )


class TestConsumoPadrao:
    """Testes para valores de consumo padrão."""

    def test_consumo_padrao_nao_negativo(self):
        """Verifica que consumos padrão são não-negativos."""
        for classe, consumo in CONSUMO_MEDIO_PADRAO_KWH_MES.items():
            assert consumo >= 0, (
                f"Consumo padrão de {classe} é negativo: {consumo}"
            )

    def test_consumo_padrao_coerente(self):
        """Verifica que consumos padrão seguem ordem esperada."""
        # Industrial > Comercial > Rural > Residencial (em geral)
        assert CONSUMO_MEDIO_PADRAO_KWH_MES["industrial"] > \
               CONSUMO_MEDIO_PADRAO_KWH_MES["comercial"], (
            "Industrial deveria consumir mais que Comercial"
        )

        assert CONSUMO_MEDIO_PADRAO_KWH_MES["comercial"] > \
               CONSUMO_MEDIO_PADRAO_KWH_MES["residencial"], (
            "Comercial deveria consumir mais que Residencial"
        )


class TestCasosEspeciais:
    """Testes para casos especiais e edge cases."""

    def test_subestacao_sem_ucs_retorna_zero(self):
        """Verifica que subestação sem UCs retorna carga zero."""
        mix_vazio = {}

        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mix_vazio

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=999)
            curva = resultado["curva_horaria_kw"]

            assert all(v == 0 for v in curva), (
                "Subestação sem UCs deveria ter carga zero"
            )

    def test_subestacao_uma_uc_calcula_corretamente(self):
        """Verifica cálculo com apenas 1 UC."""
        mix_uma_uc = {
            "Residencial": {
                "qtd_instalacoes": 1,
                "qtd_unidades_consumidoras": 1,
                "potencia_total_mw": 0.00015,  # ~150 kWh/mês
            }
        }

        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mix_uma_uc

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva = resultado["curva_horaria_kw"]

            # Curva não deve ser toda zero
            assert any(v > 0 for v in curva), (
                "Curva com 1 UC deveria ter valores > 0"
            )

            # Valores devem ser pequenos (ordem de 0.1-0.5 kW)
            assert max(curva) < 1.0, (
                "Carga de 1 UC residencial não deveria exceder 1 kW"
            )


class TestValidacaoEnergetica:
    """Testes de validação energética."""

    @pytest.fixture
    def mock_mix_conhecido(self):
        """Mix com valores conhecidos para validação energética."""
        return {
            "Residencial": {
                "qtd_instalacoes": 100,
                "qtd_unidades_consumidoras": 1000,
                "potencia_total_mw": 0.15,  # 150 kWh/mês por UC
            }
        }

    def test_energia_diaria_coerente(self, mock_mix_conhecido):
        """Verifica que energia diária é coerente com consumo mensal."""
        with patch("src.services.synthetic_load._get_mix_consumidores") as mock_get:
            mock_get.return_value = mock_mix_conhecido

            resultado = calculate_synthetic_load_by_subestacao(subestacao_id=1)
            curva = resultado["curva_horaria_kw"]

            # Energia diária: soma de 24 horas (cada valor em kW)
            energia_diaria_kwh = sum(curva)  # kW × 1h = kWh

            # Energia mensal esperada: 1000 UCs × 150 kWh/mês = 150.000 kWh/mês
            energia_mensal_esperada = 150_000

            # Energia diária esperada: energia_mensal / 30
            energia_diaria_esperada = energia_mensal_esperada / 30

            # Validar com margem de 5%
            diff_percentual = abs(energia_diaria_kwh - energia_diaria_esperada) / energia_diaria_esperada

            assert diff_percentual < 0.05, (
                f"Energia diária {energia_diaria_kwh:.1f} kWh "
                f"difere da esperada {energia_diaria_esperada:.1f} kWh "
                f"({diff_percentual:.1%})"
            )


# Marcadores para execução seletiva
pytestmark = pytest.mark.integration
