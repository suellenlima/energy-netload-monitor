#!/usr/bin/env python
"""
Script de teste rápido para o ETL de subestações.
Executa e mostra saída passo a passo.
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Executa comando e exibe resultado."""
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"{'='*60}")
    print(f"Command: {cmd}\n")
    
    result = subprocess.run(cmd, shell=True, capture_output=False, text=True)
    return result.returncode == 0

def main():
    """Executa teste completo do ETL."""
    
    print("🚀 TESTE DE SUBESTAÇÕES ETL")
    print("="*60)
    
    base_dir = Path(__file__).parent
    
    tests = [
        ("docker-compose ps", "1️⃣ Verificar containers em execução"),
        ("docker-compose exec -T postgres psql -U admin -d energy_monitor -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='subestacoes_ons';\"", 
         "2️⃣ Verificar se tabela subestacoes_ons existe"),
        ("docker-compose exec etl python src/extractors/subestacoes_client.py", 
         "3️⃣ Executar ETL de subestações"),
        ("docker-compose exec -T postgres psql -U admin -d energy_monitor -c \"SELECT COUNT(*) FROM subestacoes_ons;\"", 
         "4️⃣ Contar subestações carregadas"),
        ("docker-compose exec -T postgres psql -U admin -d energy_monitor -c \"SELECT nome, sigla_se, tensao_kv FROM subestacoes_ons LIMIT 3;\"", 
         "5️⃣ Ver amostra de subestações"),
    ]
    
    results = []
    for cmd, desc in tests:
        success = run_command(cmd, desc)
        results.append((desc, success))
    
    # Resumo
    print(f"\n{'='*60}")
    print("📊 RESUMO DOS TESTES")
    print(f"{'='*60}")
    
    for desc, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {desc}")
    
    passed = sum(1 for _, s in results if s)
    total = len(results)
    
    print(f"\n{passed}/{total} testes passaram")
    
    if passed == total:
        print("\n🎉 TUDO OK! ETL funcionando corretamente")
        sys.exit(0)
    else:
        print("\n⚠️ Alguns testes falharam. Veja ETL_DIAGNOSTICO.md")
        sys.exit(1)

if __name__ == "__main__":
    main()
