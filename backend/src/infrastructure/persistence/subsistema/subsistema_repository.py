"""SQLAlchemy implementation of SubsistemaRepository."""

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ....domain.subsistema import Subsistema, SubsistemaRepository


class SubsistemaRepositorySQLAlchemy(SubsistemaRepository):
    """Implementação SQLAlchemy para SubsistemaRepository."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def listar_todos(self) -> list[Subsistema]:
        """Lista todos os subsistemas disponíveis da tabela."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT subsistema, subsistema_codigo, nome_completo, regiao, descricao, ativo
                        FROM subsistema_ons_regiao
                        ORDER BY subsistema
                    """)
                )
                
                subsistemas = []
                for row in result.fetchall():
                    subsistemas.append(
                        Subsistema(
                            subsistema=row[0],
                            codigo=row[1],
                            nome_completo=row[2],
                            regiao=row[3],
                            descricao=row[4],
                            ativo=row[5]
                        )
                    )
                return subsistemas
        except Exception as exc:
            raise RuntimeError(f"Erro ao listar subsistemas: {exc}") from exc
    
    def obter_por_codigo(self, codigo: str) -> Subsistema | None:
        """Obtém um subsistema pelo código."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT subsistema, subsistema_codigo, nome_completo, regiao, descricao, ativo
                        FROM subsistema_ons_regiao
                        WHERE subsistema_codigo = :codigo
                    """),
                    {"codigo": codigo}
                )
                
                row = result.fetchone()
                if not row:
                    return None
                
                return Subsistema(
                    subsistema=row[0],
                    codigo=row[1],
                    nome_completo=row[2],
                    regiao=row[3],
                    descricao=row[4],
                    ativo=row[5]
                )
        except Exception as exc:
            raise RuntimeError(f"Erro ao obter subsistema: {exc}") from exc
    
    def listar_nomes(self) -> list[str]:
        """Lista apenas os nomes/identificadores dos subsistemas."""
        try:
            subsistemas = self.listar_todos()
            return [s.regiao for s in subsistemas]
        except Exception as exc:
            raise RuntimeError(f"Erro ao listar nomes de subsistemas: {exc}") from exc
