"""Domain errors for RealTimeEstimation Module."""


class RealTimeEstimationError(Exception):
    """Base error for RealTime domain."""

    pass


class EstadoNaoDisponibleError(RealTimeEstimationError):
    """Real-time state not available."""

    pass


class DadosIrradianciaInvalidosError(RealTimeEstimationError):
    """Invalid irradiance data."""

    pass


class CargaONSNaoObtidaError(RealTimeEstimationError):
    """ONS load data not obtained."""

    pass


class GeracaoMMGDNaoCalculadaError(RealTimeEstimationError):
    """MMGD generation not calculated."""

    pass


class ConfiabilidadeEstimativaError(RealTimeEstimationError):
    """Confidence estimation failed."""

    pass
