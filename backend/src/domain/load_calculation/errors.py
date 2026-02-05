"""Domain errors for LoadCalculation Module."""


class LoadCalculationError(Exception):
    """Base error for Load domain."""

    pass


class PerfilNaoEncontradoError(LoadCalculationError):
    """Load profile not found."""

    pass


class DadosConsumoInvalidosError(LoadCalculationError):
    """Invalid consumption data."""

    pass


class ClasseConsumoInvalidaError(LoadCalculationError):
    """Invalid consumption class."""

    pass


class CalibracaoNaoDisponibleError(LoadCalculationError):
    """Calibration not available."""

    pass


class PrevisaoNaoCalculadaError(LoadCalculationError):
    """Forecast not calculated."""

    pass
