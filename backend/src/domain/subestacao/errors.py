"""Domain Errors - Subestacao"""


class SubestacaoError(Exception):
    """Erro base de domínio para Subestacao"""
    pass


class SubestacaoNotFoundError(SubestacaoError):
    """Subestacao não encontrada"""
    
    def __init__(self, codigo: str):
        super().__init__(f"Subestacao {codigo} not found")
        self.codigo = codigo


class SubestacaoInvalidError(SubestacaoError):
    """Dados inválidos para Subestacao"""
    
    def __init__(self, message: str):
        super().__init__(f"Invalid subestacao: {message}")


class SubestacaoTensaoInvalidaError(SubestacaoInvalidError):
    """Tensão nominal inválida"""
    
    def __init__(self, tensao: float):
        super().__init__(f"Invalid tension: {tensao} kV")


class SubestacaoPotenciaInvalidaError(SubestacaoInvalidError):
    """Potência nominal inválida"""
    
    def __init__(self, potencia: float):
        super().__init__(f"Invalid power: {potencia} MVA")
