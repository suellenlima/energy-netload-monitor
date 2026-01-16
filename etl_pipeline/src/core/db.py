from typing import Iterable, Mapping, Optional, Sequence

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine


def create_db_engine(db_url: str, *, echo: bool = False) -> Engine:
    return create_engine(db_url, echo=echo)


def table_exists(engine: Engine, table: str) -> bool:
    inspector = inspect(engine)
    return bool(inspector.has_table(table))


def delete_all_rows(engine: Engine, table: str) -> int:
    if not table_exists(engine, table):
        return 0
    with engine.begin() as conn:
        result = conn.execute(text(f"DELETE FROM {table}"))
    return int(result.rowcount or 0)


def delete_time_window(
    engine: Engine,
    table: str,
    time_column: str,
    start,
    end,
    *,
    filters: Optional[Mapping[str, object]] = None,
) -> int:
    conditions = [f"{time_column} >= :start", f"{time_column} <= :end"]
    params = {"start": start, "end": end}

    if filters:
        for index, (column, value) in enumerate(filters.items()):
            key = f"f_{index}"
            if isinstance(value, (list, tuple, set)):
                conditions.append(f"{column} = ANY(:{key})")
                params[key] = list(value)
            else:
                conditions.append(f"{column} = :{key}")
                params[key] = value

    query = f"DELETE FROM {table} WHERE " + " AND ".join(conditions)
    with engine.begin() as conn:
        result = conn.execute(text(query), params)
    return int(result.rowcount or 0)


def make_upsert_method(
    conflict_columns: Sequence[str],
    *,
    update_columns: Optional[Sequence[str]] = None,
):
    conflict_columns = tuple(conflict_columns)

    def _upsert(table, conn, keys: Iterable[str], data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return 0
        insert_stmt = insert(table).values(rows)
        if update_columns is None:
            update_cols = {c: insert_stmt.excluded[c] for c in keys if c not in conflict_columns}
        else:
            update_cols = {c: insert_stmt.excluded[c] for c in update_columns}
        if update_cols:
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_cols,
            )
        else:
            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=conflict_columns,
            )
        result = conn.execute(upsert_stmt)
        return int(result.rowcount or 0)

    return _upsert