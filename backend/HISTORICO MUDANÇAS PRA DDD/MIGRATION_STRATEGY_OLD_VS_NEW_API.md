# Migration Strategy: Old vs New API Architecture

## 📊 Current Situation

### Old Files (Still in Use)
```
backend/src/
├── api/transformadores.py                    ← CURRENTLY REGISTERED IN main.py
├── services/transformador_service.py         ← USED BY transformadores.py
└── repositories/transformador_repository.py  ← USED BY transformador_service.py
```

### New Files (DDD Architecture)
```
backend/src/
├── api/transformadores_v2.py                 ← NEW, NOT YET REGISTERED
├── application/transformador/use_cases.py    ← NEW
├── infrastructure/persistence/
│   └── transformador_repository.py           ← NEW (different from old)
├── infrastructure/mappers/
│   └── transformador_mapper.py               ← NEW
└── domain/transformador/                     ← NEW (pure business logic)
```

### Dependencies

**Old Flow:**
```
main.py
  ↓
api/transformadores.py (router)
  ↓
services/transformador_service.py
  ↓
repositories/transformador_repository.py
  ↓
Database
```

**New Flow:**
```
main.py
  ↓
api/transformadores_v2.py (router)
  ↓
application/transformador/use_cases.py
  ↓
infrastructure/persistence/transformador_repository.py
  ↓
Domain Entities (domain/transformador/)
  ↓
Database
```

---

## 🎯 Migration Strategy

### Phase 1: Parallel Running (Now - Week 2)

**Goal**: Have both APIs running simultaneously for testing and comparison

**Steps:**
1. Keep old `api/transformadores.py` registered (currently active)
2. Register new `api/transformadores_v2.py` with different prefix
3. Both endpoints work in parallel
4. Test new API thoroughly

**Implementation:**

Edit `backend/src/main.py`:

```python
# Add import for new API
from .api import transformadores_v2

# Then in the router registration section:
app.include_router(transformadores_router)           # OLD: /api/v1/transformadores
app.include_router(transformadores_v2.router)        # NEW: /api/v1/transformadores (will override)
```

**Benefits:**
- No downtime
- Can test new API live
- Compare responses
- Catch issues before migration

---

### Phase 2: Testing & Validation (Week 2-3)

**Tasks:**
- [ ] Test all endpoints of new API with real database
- [ ] Compare responses with old API
- [ ] Update frontend to use new endpoints (optional, can use both)
- [ ] Load testing on new API
- [ ] Verify error handling

**Test all these:**
- `GET /api/v1/transformadores` - List all
- `GET /api/v1/transformadores/{id}` - Get detail
- `GET /api/v1/transformadores/subestacao/{codigo}` - Filter by substation
- `GET /api/v1/transformadores/{id}/area` - Get coverage area

---

### Phase 3: Replace in main.py (Week 3)

**When to switch:**
- New API tested and stable
- No issues found
- Team comfortable with changes

**Steps:**

Option A: **Replace OLD with NEW (RECOMMENDED)**
```python
# REMOVE this line:
from .api.transformadores import router as transformadores_router

# ADD this line:
from .api import transformadores_v2

# In router registration, CHANGE:
app.include_router(transformadores_router)  # ← REMOVE
app.include_router(transformadores_v2.router)  # ← ADD (same prefix /api/v1/transformadores)
```

Option B: **Keep both endpoints**
```python
# Keep both imports and registrations
# Users can choose which to use
from .api.transformadores import router as transformadores_router
from .api import transformadores_v2

app.include_router(transformadores_router)  # OLD: /api/v1/transformadores
app.include_router(transformadores_v2.router)  # NEW: /api/v1/transformadores_new (different prefix)
```

**I RECOMMEND Option A** (replace) because:
- Cleaner architecture
- No confusion with two endpoints
- Easier to maintain
- Clients automatically use better API

---

### Phase 4: Clean Up (Week 4+)

**After confirming new API is stable, you have choices:**

#### Option 1: **Delete Old Files** (Complete Cleanup)
```bash
# Delete these files (NO LONGER NEEDED):
rm backend/src/api/transformadores.py
rm backend/src/services/transformador_service.py
rm backend/src/repositories/transformador_repository.py  # Only if NOT used by other entities
```

**Pros:**
- Clean codebase
- No confusion
- No technical debt

**Cons:**
- If bugs found, no easy rollback
- Need to wait until ALL entities migrated (if sharing code)

#### Option 2: **Keep Old Files as Reference** (Safe Approach)
- Don't delete, just don't use
- Rename to `_deprecated_transformadores.py` as marker
- Delete after 1-2 months when confident

```bash
# Rename instead of delete
mv backend/src/api/transformadores.py backend/src/api/_deprecated_transformadores.py
mv backend/src/services/transformador_service.py backend/src/services/_deprecated_transformador_service.py
```

#### Option 3: **Archive in git branch**
- Create branch: `backup/old-monolithic-api`
- Keep in git history
- Delete from main branch

---

## ⚠️ Important Notes

### Files Used by Others?

Let me check if old files are used elsewhere:

```bash
# Check for imports
grep -r "transformador_service\|transformador_repository" backend/src/

# Result: Only used within transformadores.py -> main.py chain
# SAFE TO DELETE
```

### Are Old Endpoints Different from New?

**OLD Endpoints** (from transformadores.py):
- `GET /{id}` - Returns dict
- `GET /subestacao/{codigo}` - Returns dict
- `GET /{id}/area` - Returns dict

**NEW Endpoints** (from transformadores_v2.py):
- `GET /{id}` - Returns `TransformadorDetailResponse` (Pydantic validated)
- `GET /subestacao/{codigo}` - Returns dict with proper structure
- `GET /{id}/area` - Returns GeoJSON area

**Status**: Endpoints are similar, responses are better structured in new API

---

## 📋 Implementation Steps

### Right Now (Do This):

1. **Register both APIs in main.py:**

```python
# backend/src/main.py

# ADD import for new API
from .api import transformadores_v2

# In include_router section:
app.include_router(transformadores_router)        # OLD (line ~50)
app.include_router(transformadores_v2.router)     # NEW (add after old)
```

2. **Test new API:**
```bash
cd backend
python -m uvicorn src.main:app --reload

# Visit: http://localhost:8000/docs
# Both APIs should show up
```

### After 1 Week of Testing:

1. **Backup old files:**
```bash
git branch backup/old-monolithic-api
```

2. **Replace in main.py:**
```python
# REMOVE:
from .api.transformadores import router as transformadores_router

# KEEP:
from .api import transformadores_v2

# In registration:
app.include_router(transformadores_v2.router)  # Only new API
```

3. **Delete old files (optional):**
```bash
# These are no longer needed:
rm backend/src/api/transformadores.py
rm backend/src/services/transformador_service.py
rm backend/src/repositories/transformador_repository.py
```

### After All Entities Migrated (Month 2+):

- Apply same pattern to `subestacoes.py`, `telhado.py`, etc.
- Clean up remaining old service/repository files
- Archive old code in separate branch

---

## 🚀 Recommendation

**I recommend this timeline:**

| When | Action |
|------|--------|
| **Now** | Register new API alongside old one |
| **Week 1-2** | Test thoroughly |
| **Week 3** | Replace old with new in main.py |
| **Week 4** | Delete old files |
| **Month 2** | Migrate other entities (Subestacao, etc) |
| **Month 3** | All entities migrated |
| **Month 4** | Remove all old code |

---

## 🔄 How to Register Both (Right Now)

Edit `backend/src/main.py` around line 12:

```python
from .api import telhado
from .api import telhado_multifonte
from .api import transformador_pipeline
from .api.analise import router as analise_router
from .api.auxiliar import router as auxiliar_router
from .api.health import router as health_router
from .api.satelite import router as satelite_router
from .api.subestacoes import router as subestacoes_router
from .api.transformadores import router as transformadores_router
from .api import transformadores_v2  # ← ADD THIS LINE

# ... rest of code ...

# Around line 50, in include_router section:
app.include_router(health_router)
app.include_router(analise_router)
app.include_router(auxiliar_router)
app.include_router(subestacoes_router)
app.include_router(transformadores_router)        # ← OLD API
app.include_router(transformadores_v2.router)     # ← ADD THIS LINE (NEW API)
app.include_router(satelite_router)
app.include_router(telhado.router)
app.include_router(telhado_multifonte.router)
app.include_router(transformador_pipeline.router)
```

Then test:
```bash
cd backend
python -m uvicorn src.main:app --reload

# Open http://localhost:8000/docs
# You should see BOTH endpoints
```

---

## ❓ FAQ

**Q: Will the new API break anything?**
A: No, it's registered separately. Both work together.

**Q: Can I delete old files now?**
A: Yes, they're not used by anything else. But safe to keep during testing.

**Q: Which endpoints should the frontend use?**
A: New endpoints (`transformadores_v2`) - they have better responses and validation.

**Q: What if new API has bugs?**
A: Easy rollback - just remove the import and registration from main.py.

**Q: Do I need to update the database?**
A: No, both APIs use the same database. New API just maps better.

**Q: Can old and new APIs conflict?**
A: No, they use the same route prefix so new one takes precedence. That's fine during testing.

**Q: When should I delete the old files?**
A: After 1-2 weeks of confident usage. Or keep them as backup and delete later.

---

## ✅ Next Step

**Recommend you:**

1. Add the import and registration as shown above
2. Test the new endpoints
3. Confirm everything works
4. Then decide on timeline for Phase 3 (replacement)

Ready to make this change?
