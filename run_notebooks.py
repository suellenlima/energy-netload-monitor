#!/usr/bin/env python
"""Script para executar notebooks em sequência."""

import subprocess
import sys
from pathlib import Path

notebooks = [
    'notebooks/06_transfer_learning_real.ipynb',
    'notebooks/07_advanced_detection_techniques.ipynb'
]

for nb in notebooks:
    print("\n" + "="*80)
    print(f"Executando: {nb}")
    print("="*80 + "\n")
    
    result = subprocess.run(
        [sys.executable, '-m', 'jupyter', 'nbconvert', 
         '--to', 'notebook', '--execute', '--inplace', nb],
        cwd='.'
    )
    
    if result.returncode != 0:
        print(f"\nErro ao executar {nb}")
        sys.exit(1)

print("\n" + "="*80)
print("✅ Todos os notebooks executados com sucesso!")
print("="*80)
print("\nAgora execute notebook 08 para ver o benchmark:")
print("  notebooks/08_comparison_benchmark.ipynb")
