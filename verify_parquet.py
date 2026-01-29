import pyarrow.parquet as pq
import sys

try:
    table = pq.read_table('brick/data.parquet')
    print(f"Read {table.num_rows} rows.")
    print(f"Columns: {table.column_names}")
    if table.num_rows > 0:
        sys.exit(0)
    else:
        sys.exit(1)
except Exception as e:
    print(e)
    sys.exit(1)
