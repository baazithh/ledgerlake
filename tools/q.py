import sys

import duckdb

duckdb.sql(sys.argv[1]).show(max_width=200)