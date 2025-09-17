from typing import Iterable
def rows_to_dicts(rows: Iterable) -> list[dict]:
    return [dict(r) for r in rows]