03:

Algoritmo de Detecção Heurística (OpenCV). Painéis solares têm uma assinatura visual muito específica:

São retangulares.

Têm cor azul-escura/preta (diferente de telhados de barro ou vegetação).

Têm bordas bem definidas.

Notebook que baixa uma imagem de satélite, encontra essas áreas "azuis e retangulares" e calcula a área total. Se a área for maior que o registrado na ANEEL, temos um "Gato" (Carga Oculta Não Registrada).