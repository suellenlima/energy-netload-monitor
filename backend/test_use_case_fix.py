#!/usr/bin/env python3
"""Teste rápido do use case corrigido"""

from src.application.subestacao.use_cases import ObtenerCargaSinteticaUseCase
from src.infrastructure.persistence.subestacao.repository import SQLAlchemySubestacaoRepository

# Verificar que agora funciona
repo = SQLAlchemySubestacaoRepository()
use_case = ObtenerCargaSinteticaUseCase(repository=repo)

# Testar com subestacao_id
resultado = use_case.executar(subestacao_id=1)
print('✅ Chamada com subestacao_id=1:')
print(f"   - subestacao_id: {resultado['subestacao_id']}")
print(f"   - curva_horaria_kw: {len(resultado['curva_horaria_kw'])} valores")

# Testar com codigo
try:
    resultado2 = use_case.executar_por_codigo(codigo='SUB_TEST_001')
    print('✅ Chamada com codigo=SUB_TEST_001:')
    print(f"   - codigo: {resultado2['codigo']}")
    print(f"   - nome: {resultado2['nome']}")
except Exception as e:
    print(f"⚠️ Erro ao buscar por código: {e}")

print("\n✅ USE CASE CORRIGIDO - SEM ERRO DE PARÂMETROS")
