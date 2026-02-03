"""
Testes para perfis de carga típicos.

Valida que os perfis de carga estão corretos e atendem aos critérios:
- 24 horas completas
- Média = 1.0 ± 0.02
- Valores não negativos
- Pico no horário esperado para cada classe
"""

import pytest

from src.data.load_profiles import (
    PERFIS_TIPICOS,
    get_profile,
    get_profile_metadata,
    calibrate_profile,
)


class TestPerfisTipicos:
    """Testes para os perfis de carga típicos."""

    @pytest.fixture
    def todas_classes(self):
        """Lista de todas as classes disponíveis."""
        return list(PERFIS_TIPICOS.keys())

    def test_perfis_tem_24_valores(self, todas_classes):
        """Verifica que cada perfil tem exatamente 24 valores (uma para cada hora)."""
        for classe in todas_classes:
            perfil = get_profile(classe)
            assert len(perfil) == 24, f"Perfil {classe} não tem 24 valores"

    def test_perfis_media_igual_um(self, todas_classes):
        """Verifica que a média de cada perfil é ~1.0 (± 2%)."""
        for classe in todas_classes:
            perfil = get_profile(classe)
            media = sum(perfil) / len(perfil)
            assert 0.98 <= media <= 1.02, (
                f"Perfil {classe} tem média {media:.3f}, fora do range 0.98-1.02"
            )

    def test_perfis_nao_negativos(self, todas_classes):
        """Verifica que todos os valores dos perfis são não-negativos."""
        for classe in todas_classes:
            perfil = get_profile(classe)
            assert all(v >= 0 for v in perfil), (
                f"Perfil {classe} contém valores negativos"
            )

    def test_perfis_tem_amplitude_razoavel(self, todas_classes):
        """Verifica que a amplitude (pico - vale) está em um range razoável."""
        for classe in todas_classes:
            perfil = get_profile(classe)
            pico = max(perfil)
            vale = min(perfil)
            amplitude = pico - vale

            # Amplitude mínima: 0.5 (perfil não pode ser totalmente plano)
            # Amplitude máxima: 2.5 (pico não pode ser > 350% do vale)
            assert 0.5 <= amplitude <= 2.5, (
                f"Perfil {classe} tem amplitude {amplitude:.2f}, fora do range 0.5-2.5"
            )

    def test_perfil_residencial_pico_noturno(self):
        """Verifica que perfil residencial tem pico noturno (18h-22h)."""
        perfil = get_profile("residencial")
        hora_pico = perfil.index(max(perfil))
        assert 18 <= hora_pico <= 22, (
            f"Residencial deveria ter pico entre 18h-22h, mas pico é às {hora_pico}h"
        )

    def test_perfil_comercial_pico_diurno(self):
        """Verifica que perfil comercial tem pico diurno (9h-18h)."""
        perfil = get_profile("comercial")
        hora_pico = perfil.index(max(perfil))
        assert 9 <= hora_pico <= 18, (
            f"Comercial deveria ter pico entre 9h-18h, mas pico é às {hora_pico}h"
        )

    def test_perfil_industrial_mais_plano(self):
        """Verifica que perfil industrial é mais plano que residencial."""
        perfil_ind = get_profile("industrial")
        perfil_res = get_profile("residencial")

        amplitude_ind = max(perfil_ind) - min(perfil_ind)
        amplitude_res = max(perfil_res) - min(perfil_res)

        assert amplitude_ind < amplitude_res, (
            "Industrial deveria ter amplitude menor que residencial"
        )

    def test_perfil_rural_pico_matinal(self):
        """Verifica que perfil rural tem pico matinal (5h-7h) ou vespertino (17h-19h)."""
        perfil = get_profile("rural")
        hora_pico = perfil.index(max(perfil))

        # Rural pode ter pico na manhã (irrigação) ou tarde (retorno)
        assert (5 <= hora_pico <= 7) or (17 <= hora_pico <= 19), (
            f"Rural deveria ter pico entre 5h-7h ou 17h-19h, mas pico é às {hora_pico}h"
        )


class TestMetadados:
    """Testes para metadados dos perfis."""

    def test_metadados_contem_campos_obrigatorios(self):
        """Verifica que metadados contêm todos os campos esperados."""
        metadados = get_profile_metadata()

        for classe, meta in metadados.items():
            assert "hora_pico" in meta, f"{classe}: falta hora_pico"
            assert "fator_pico" in meta, f"{classe}: falta fator_pico"
            assert "hora_vale" in meta, f"{classe}: falta hora_vale"
            assert "fator_vale" in meta, f"{classe}: falta fator_vale"
            assert "amplitude" in meta, f"{classe}: falta amplitude"

    def test_metadados_hora_pico_valida(self):
        """Verifica que hora_pico está entre 0 e 23."""
        metadados = get_profile_metadata()

        for classe, meta in metadados.items():
            assert 0 <= meta["hora_pico"] <= 23, (
                f"{classe}: hora_pico {meta['hora_pico']} fora do range 0-23"
            )

    def test_metadados_amplitude_consistente(self):
        """Verifica que amplitude = fator_pico - fator_vale."""
        metadados = get_profile_metadata()

        for classe, meta in metadados.items():
            amplitude_calculada = meta["fator_pico"] - meta["fator_vale"]
            amplitude_meta = meta["amplitude"]

            assert abs(amplitude_calculada - amplitude_meta) < 0.01, (
                f"{classe}: amplitude inconsistente "
                f"(calculada: {amplitude_calculada:.2f}, meta: {amplitude_meta:.2f})"
            )


class TestCalibracao:
    """Testes para calibração de perfis com dados BDGD."""

    def test_calibrate_profile_retorna_24_valores(self):
        """Verifica que calibração retorna 24 valores."""
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal_kwh=150)
        assert len(perfil_calibrado) == 24

    def test_calibrate_profile_escala_corretamente(self):
        """Verifica que calibração escala perfil corretamente."""
        consumo_mensal = 150  # kWh/mês
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal)

        # Potência média esperada: 150 kWh / 720h = 0.208 kW
        potencia_media_esperada = consumo_mensal / 720

        # Média do perfil calibrado deve ser próxima da potência média
        media_perfil = sum(perfil_calibrado) / len(perfil_calibrado)

        assert abs(media_perfil - potencia_media_esperada) < 0.01, (
            f"Média do perfil calibrado {media_perfil:.3f} "
            f"difere da esperada {potencia_media_esperada:.3f}"
        )

    def test_calibrate_profile_mantem_forma(self):
        """Verifica que calibração mantém a forma do perfil original."""
        consumo_mensal = 150
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal)
        perfil_original = get_profile("residencial")

        # Hora de pico deve ser a mesma
        hora_pico_calibrado = perfil_calibrado.index(max(perfil_calibrado))
        hora_pico_original = perfil_original.index(max(perfil_original))

        assert hora_pico_calibrado == hora_pico_original, (
            "Calibração mudou a hora de pico do perfil"
        )

    def test_calibrate_profile_nao_negativo(self):
        """Verifica que calibração não gera valores negativos."""
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal_kwh=150)
        assert all(v >= 0 for v in perfil_calibrado), (
            "Perfil calibrado contém valores negativos"
        )

    def test_calibrate_profile_zero_consumo(self):
        """Verifica comportamento com consumo zero."""
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal_kwh=0)
        assert all(v == 0 for v in perfil_calibrado), (
            "Perfil com consumo zero deveria ser todo zero"
        )


class TestConsistenciaEntreClasses:
    """Testes de consistência cruzada entre classes."""

    def test_todas_classes_tem_mesmo_numero_de_valores(self):
        """Verifica que todas as classes têm o mesmo número de valores (24)."""
        tamanhos = {classe: len(perfil) for classe, perfil in PERFIS_TIPICOS.items()}
        tamanhos_unicos = set(tamanhos.values())
        assert len(tamanhos_unicos) == 1, (
            f"Classes têm tamanhos diferentes: {tamanhos}"
        )
        assert list(tamanhos_unicos)[0] == 24

    def test_classes_tem_perfis_distintos(self):
        """Verifica que classes têm perfis realmente diferentes."""
        classes = list(PERFIS_TIPICOS.keys())

        for i, classe1 in enumerate(classes):
            for classe2 in classes[i+1:]:
                perfil1 = get_profile(classe1)
                perfil2 = get_profile(classe2)

                # Perfis não devem ser idênticos
                assert perfil1 != perfil2, (
                    f"Perfis de {classe1} e {classe2} são idênticos"
                )

                # Calcular correlação simples (produto escalar normalizado)
                prod_escalar = sum(v1 * v2 for v1, v2 in zip(perfil1, perfil2))
                norma1 = sum(v**2 for v in perfil1) ** 0.5
                norma2 = sum(v**2 for v in perfil2) ** 0.5
                correlacao = prod_escalar / (norma1 * norma2)

                # Perfis devem ter correlação < 0.95 (serem suficientemente distintos)
                assert correlacao < 0.95, (
                    f"Perfis de {classe1} e {classe2} têm correlação muito alta: {correlacao:.3f}"
                )


class TestValidacaoEnergetica:
    """Testes de validação energética."""

    def test_soma_perfil_calibrado_igual_energia_mensal(self):
        """Verifica que soma do perfil calibrado × 30 dias = energia mensal."""
        consumo_mensal = 150  # kWh/mês
        perfil_calibrado = calibrate_profile("residencial", consumo_mensal)

        # Energia diária: soma de 24 horas (cada valor é kW)
        energia_diaria_kwh = sum(perfil_calibrado)  # kW × 1h = kWh

        # Energia mensal: 30 dias
        energia_mensal_calculada = energia_diaria_kwh * 30

        # Deve ser igual ao consumo mensal (± 1%)
        assert abs(energia_mensal_calculada - consumo_mensal) / consumo_mensal < 0.01, (
            f"Energia mensal calculada {energia_mensal_calculada:.1f} "
            f"difere do esperado {consumo_mensal}"
        )


# Fixtures globais para todos os testes
@pytest.fixture(scope="module")
def perfis_disponiveis():
    """Lista de perfis disponíveis no sistema."""
    return list(PERFIS_TIPICOS.keys())


# Marcadores para execução seletiva
pytestmark = pytest.mark.integration
