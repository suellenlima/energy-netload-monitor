# Análise DDD - Energy Netload Monitor

## 📊 Estado Atual da Implementação

### ✅ Pontos Fortes

1. **Separação de Camadas Apropriada**
   - `api/` → Controllers (entrada HTTP)
   - `services/` → Use Cases (orquestração)
   - `repositories/` → Persistência
   - `schemas/` → DTOs (Pydantic)
   - `data/` → Infraestrutura

2. **Abstração de Persistência**
   - `BaseRepository` abstrata com engine SQLAlchemy
   - Repositories especializadas por entidade (Transformador, Subestacao, etc)
   - Isolamento da lógica de consulta SQL

3. **Dependency Injection**
   - Funções `get_*_service()` no FastAPI com `Depends()`
   - Facilita testes e substituição de implementações

4. **Logging Estruturado**
   - Logger em cada serviço e repositório
   - Rastreamento de operações

---

## ❌ Problemas Identificados

### 1. **Falta de Entidades de Domínio**
**Problema:** O diretório `domain/` está vazio  
**Impacto:** Lógica de negócio misturada em serviços

```
Situação Atual:
API → Service → Repository → DB

Deveria ser:
API → Service → [Domain Entities/Aggregates] → Repository → DB
```

**Exemplo:** Classe `Transformador` deveria estar em `domain/`:
```python
# domain/transformador.py
class Transformador:
    def __init__(self, id: int, nome: str, potencia_kva: float, latitude: float, longitude: float):
        self.id = id
        self.nome = nome
        self.potencia_kva = potencia_kva
        self.localizacao = Localizacao(latitude, longitude)
        self.validar()
    
    def validar(self):
        if self.potencia_kva <= 0:
            raise ValueError("Potência deve ser positiva")
```

### 2. **Services com Responsabilidades Múltiplas**
**Problema:** Services mixam orquestração, validação e formatação

```python
# transformador_service.py - Faz TUDO:
- Buscar dados (orquestração)
- Converter para GeoJSON (formatação)
- Validar áreas (lógica de negócio)
- Calcular distâncias (lógica)
```

**Solução:** Separar em Use Cases específicos

### 3. **Schemas Genéricos, sem Validações de Domínio**
**Problema:** Validações de negócio não estão nas entidades

```python
# schemas/subestacao.py
class SubestacaoDetectadaResponse(BaseModel):
    potencia_total_mw: float | None = None  # ← Sem validação!

# Deveria ter validação em domain/
class Subestacao:
    def __init__(self, potencia_mw: float):
        if potencia_mw <= 0:
            raise SubestacaoPotenciaInvalidaError()
        self.potencia_mw = potencia_mw
```

### 4. **Value Objects Não Definidos**
**Problema:** Tipos primitivos em vez de objetos de domínio

```python
# Atual: latitude: float, longitude: float
# Deveria ser: localizacao: Localizacao (Value Object)

class Localizacao(ValueObject):
    def __init__(self, latitude: float, longitude: float):
        if not (-90 <= latitude <= 90):
            raise ValueError("Latitude inválida")
        if not (-180 <= longitude <= 180):
            raise ValueError("Longitude inválida")
        self.latitude = latitude
        self.longitude = longitude
```

### 5. **Falta de Especificação de Agregados**
**Problema:** Não está claro qual é a raiz agregada de cada contexto

```
Subestacao
  ├─ Transformadores (?)
  └─ Painéis Solares (?)

Telhado
  ├─ Painéis (?)
  └─ Medições (?)
```

### 6. **Sem Exceções de Domínio Específicas**
**Problema:** Usando HTTPException genérica

```python
# Atual
if not trans:
    raise HTTPException(status_code=404, detail="Transformador não encontrado")

# Deveria ser
if not trans:
    raise TransformadorNaoEncontradoError()
    # Depois a camada de API converte para HTTPException
```

---

## 🎯 Recomendações de Melhoria

### Prioridade 1: Criar Entidades de Domínio

```
domain/
├── transformador/
│   ├── __init__.py
│   ├── entidade.py          # Classe Transformador
│   ├── value_objects.py     # Localizacao, Potencia, etc
│   └── erros.py             # Exceções específicas
├── subestacao/
│   ├── __init__.py
│   ├── entidade.py
│   ├── value_objects.py
│   └── erros.py
├── painel_solar/
│   ├── __init__.py
│   ├── entidade.py
│   ├── value_objects.py
│   └── erros.py
└── comum/
    ├── value_objects.py    # Localizacao, Temperatura, etc (reutilizáveis)
    └── exceções.py         # Base para todas as exceções de domínio
```

### Prioridade 2: Refatorar Services em Use Cases

```
services/
├── transformador/
│   ├── obter_transformador_use_case.py
│   ├── listar_transformadores_use_case.py
│   ├── criar_transformador_use_case.py
│   └── calcular_area_cobertura_use_case.py
└── ...
```

### Prioridade 3: Implementar Repositories com Interfaces

```python
# repositories/transformador/interface.py
from abc import ABC, abstractmethod
from typing import Optional
from domain.transformador import Transformador

class ITransformadorRepository(ABC):
    @abstractmethod
    def obter_por_id(self, id: int) -> Optional[Transformador]:
        pass

# repositories/transformador/sqlalchemy_repository.py
class TransformadorRepository(ITransformadorRepository):
    def obter_por_id(self, id: int) -> Optional[Transformador]:
        # Implementação SQLAlchemy
        pass
```

### Prioridade 4: Criar Mapeadores (Mappers)

```
integration/
├── transformador_mapper.py    # Domain ↔ Schema
├── subestacao_mapper.py
└── ...
```

---

## 📈 Estrutura Proposta Final

```
backend/src/
├── domain/                           # ⭐ LÓGICA DE NEGÓCIO
│   ├── transformador/
│   │   ├── entidade.py              # Transformador
│   │   ├── value_objects.py         # Localizacao, Potencia
│   │   ├── repositorio_interface.py # ITransformadorRepository
│   │   └── erros.py                 # TransformadorError, etc
│   ├── subestacao/
│   ├── painel_solar/
│   └── comum/
│       ├── value_objects.py
│       └── erros.py
│
├── aplicacao/                        # ⭐ USE CASES
│   ├── transformador/
│   │   ├── obter_transformador.py
│   │   ├── listar_transformadores.py
│   │   └── ...
│   └── ...
│
├── infraestrutura/                   # ⭐ IMPLEMENTAÇÕES TÉCNICAS
│   ├── persistencia/
│   │   ├── transformador_repository.py
│   │   └── subestacao_repository.py
│   ├── integrações/
│   │   ├── google_maps_client.py
│   │   ├── inpe_client.py
│   │   └── ...
│   └── mapeadores/
│       ├── transformador_mapper.py
│       └── ...
│
├── apresentacao/                     # ⭐ HTTP LAYER
│   ├── api/
│   │   ├── transformadores.py
│   │   ├── subestacoes.py
│   │   └── ...
│   ├── schemas/                      # DTOs
│   │   ├── transformador.py
│   │   └── ...
│   └── dependencias.py              # Dependency Injection
│
├── core/                            # Configurações, exceções HTTP
│   ├── config.py
│   ├── logging.py
│   ├── erros_http.py
│   └── database.py
│
└── main.py
```

---

## 🔄 Fluxo de Dados Melhorado

```
1. API Request
   ↓
2. Controller (transformadores.py)
   ↓
3. Use Case (obter_transformador_use_case.py)
   ├─ Aplica regras de negócio
   ├─ Valida com Domain Entities
   └─ Chama Repository
   ↓
4. Repository (transformador_repository.py)
   ├─ Busca no BD
   └─ Converte em Domain Entity
   ↓
5. Mapper (transformador_mapper.py)
   ├─ Domain Entity → Response Schema
   ↓
6. Response Schema (schemas/transformador.py)
   ├─ Validação Pydantic
   └─ Serialização JSON
```

---

## 💡 Próximos Passos (Sequenciados)

### Fase 1: Fundação (1-2 dias)
1. [ ] Criar estrutura `domain/transformador`
2. [ ] Definir `Transformador` como entidade
3. [ ] Definir `Localizacao` como value object
4. [ ] Criar `TransformadorError` base

### Fase 2: Integração (1-2 dias)
5. [ ] Criar `ITransformadorRepository` interface
6. [ ] Refatorar `TransformadorRepository` para implementar interface
7. [ ] Criar `TransformadorMapper`

### Fase 3: Use Cases (2-3 dias)
8. [ ] Criar `ObtherTransformadorUseCase`
9. [ ] Criar `ListarTransformadoresUseCase`
10. [ ] Refatorar API para usar Use Cases

### Fase 4: Escalação (2-3 dias)
11. [ ] Repetir Fase 1-3 para `Subestacao`
12. [ ] Repetir Fase 1-3 para `PainelSolar`
13. [ ] Repetir Fase 1-3 para `Telhado`

---

## 📚 Benefícios da Implementação Completa

| Aspecto | Antes | Depois |
|--------|-------|--------|
| **Testabilidade** | Difícil (acoplado) | Fácil (isolado) |
| **Manutenção** | Complexa (lógica espalhada) | Simples (centralizada) |
| **Escalabilidade** | Limitada | Alta (fácil adicionar contextos) |
| **Reutilização** | Baixa | Alta |
| **Clareza de Domínio** | Nenhuma | Explícita no código |
| **Validações** | Dispersas | Centralizadas |

---

## 📖 Referências

- Domain-Driven Design - Eric Evans
- Clean Architecture - Robert C. Martin
- CQRS Pattern (opcional para leitura/escrita)
