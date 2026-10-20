import os
from pathlib import Path

import pyarrow.parquet as pq

DSN = os.getenv("DATABASE_URL", "postgresql://shop:shop_pw@localhost:5434/shop")
BRONZE = Path(os.getenv("BRONZE_DIR", "data/bronze"))


def write_parquet_atomic(table, path):
    """Write to a temp file, then rename, so readers never see a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, str(tmp))
    tmp.replace(path)