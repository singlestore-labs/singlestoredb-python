import singlestoredb as s2
from singlestoredb.config import set_option
import pandas as pd
import time

BATCH_SIZE = 10000
QUERY_3_COL = 'SELECT i, d, t FROM 3_col_test'

HOST = '127.0.0.1'
PASSWORD = 'p'
USERNAME = 'root'
PORT = 3306
DATABASE = 'x_db'


def timeit(f):
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print(f'func:{f.__name__} args:[{args}, {kw}] took: {te-ts:.4f} sec')
        return result, te-ts
    return timed


@timeit
def regular_read(conn, query):
    all_rows = []
    with conn.cursor() as cur:
        cur.execute(query)
        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            all_rows += rows
    print(f'regular_read rows: {len(all_rows)}')


@timeit
def arrow_read_pandas(conn, query):
    all_rows = []
    with conn.cursor() as cur:
        cur.execute_arrow(query, result_batch_size=BATCH_SIZE)
        while True:
            rows = cur.fetchmany_arrow(result_type='pandas.DataFrame')
            if rows is None or rows.empty:
                break
            rows.columns = list(range(len(rows.columns)))
            all_rows.append(rows)

    result_df = pd.concat(all_rows)
    print(f'arrow_read_pandas, shape: {result_df.shape}')


def main():
    set_option('debug.queries', True)
    conn = s2.connect(host=HOST, port=PORT, user=USERNAME,
                      password=PASSWORD, database=DATABASE)
    n_runs = 5
    regular_total = 0
    arrow_total = 0
    for i in range(n_runs):
        _, t = regular_read(conn, QUERY_3_COL)
        regular_total += t
        _, t = arrow_read_pandas(conn, QUERY_3_COL)
        arrow_total += t
    print(f'Total regular: {regular_total}, total arrow: {arrow_total}')


if __name__ == '__main__':
    main()
