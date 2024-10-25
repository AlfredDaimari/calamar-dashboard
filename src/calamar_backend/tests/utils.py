import timeit
from calamar_backend import interface as inf
from calamar_backend import database as db

def test_string_sql_format() -> bool:
    date = '2023-10-11 00:00:00'
    try:
        dt = inf.Time(date).get_date_strf_index_sql()
        print(f"string_sql_query: {dt}")
        bt:bool = dt == '2023-10-11 00:00:00+00:00'

        db_ = db.Database()
        cursor = db_.conn.cursor()
        cursor.execute(f"SELECT * FROM nifty50_price WHERE Date='{dt}'")
        rows = cursor.fetchall()
        print(f"test_string_sql_format_results: {rows}")
        bt = len(rows) > 0 and bt

    except Exception as e:
        print(e)
        return False
    
    return bt

def main():
    print("==== Utils testing ====")
    OKGREEN = "\033[92m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    tick = OKGREEN + "\N{check mark}" + ENDC
    cross = FAIL + "\N{cross mark}" + ENDC

    emoji = lambda x: tick if x else cross

    start_time = timeit.default_timer()
    tst_string_sql_format = test_string_sql_format()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Utils test results ====")
    print(f"test_string_sql_format: {emoji(tst_string_sql_format)}")

    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")

if __name__ == '__main__':
    main()
