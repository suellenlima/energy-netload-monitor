"""Domain errors for Analise Module."""


class AnaliseError(Exception):
    """Base error for Analise domain."""

    pass


class DistribuidoraNotFoundError(AnaliseError):
    """Distribuidora not found."""

    pass


class DadosInsuficientesError(AnaliseError):
    """Insufficient data for analysis."""

    pass


class AnomaliaDeteccaoError(AnaliseError):
    """Error during anomaly detection."""

    pass


class PerfilCargaNotFoundError(AnaliseError):
    """Load profile not found."""

    pass


class EstadoAtualNaoDisponivelError(AnaliseError):
    """Current state not available."""

    pass


class AlertaFraudeNaoEncontradoError(AnaliseError):
    """Fraud alert not found."""

    pass


class ConsumoInvalidoError(AnaliseError):
    """Invalid consumption data."""

    pass
