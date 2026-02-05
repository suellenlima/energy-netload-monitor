"""
Domain errors for Satellite module.
"""


class SateliteError(Exception):
    """Base error for satellite domain"""
    pass


class TransformadorNotFoundError(SateliteError):
    """Raised when transformador is not found"""
    pass


class CoordenadasInvalidasError(SateliteError):
    """Raised when coordinates are invalid"""
    pass


class AreaCoberturaNaoCalculadaError(SateliteError):
    """Raised when coverage area has not been calculated"""
    pass


class FonteNaoDisponibleError(SateliteError):
    """Raised when satellite source is not available"""
    pass


class QuotaExcedidaError(SateliteError):
    """Raised when quota is exceeded"""
    pass


class HistoricoVazioError(SateliteError):
    """Raised when there's no request history"""
    pass
