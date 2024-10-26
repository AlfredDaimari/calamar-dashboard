from calamar_backend import database as db
import timeit


def test_create_index_table() -> bool:
    ticker = "nifty50"

    try:
        db_ = db.Database()
        # db_.create_index_table(ticker, "2019-12-10", "2024-10-25")
        cur = db_.conn.cursor()
        cur.execute(f"SELECT Date, Close FROM {ticker}_price LIMIT 2")
        rows = cur.fetchall()
        print(f"\ntest_create_index_table_results: {rows}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_index_nav_table() -> bool:
    ticker = "nifty50"

    try:
        db_ = db.Database()
        res = db_.create_index_nav_table(ticker)
        print(f"\ntest_create_index_nav_table_results: {str(res)}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_trade_report_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_trade_report_table()
        cur = db_.conn.cursor()
        cur.execute(f'SELECT ISIN, Quantity, "Trade Date"  FROM trade_report LIMIT 2')
        rows = cur.fetchall()
        print(f"\ntest_create_trade_report_table_results: {rows}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_bank_statment_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_bank_statment_table()
        cur = db_.conn.cursor()
        cur.execute(f"SELECT * FROM bank_statement LIMIT 2")
        rows = cur.fetchall()
        print(f"\ntest_create_bank_statement_table_results: {rows}")

    except Exception as e:
        print(e)
        return False
    return True


def test_get_day_zero_bank_statements() -> bool:
    try:
        db_ = db.Database()
        bnk_statements = db_.get_day_zero_bank_statements()
        print(
            f"\ntest_get_zero_day_bank_statements_results: {list(map(str,bnk_statements))}"
        )
    except Exception as e:
        print(e)
        return False
    return True


def main():
    print("==== Database testing ====")
    OKGREEN = "\033[92m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    tick = OKGREEN + "\N{check mark}" + ENDC
    cross = FAIL + "\N{cross mark}" + ENDC

    emoji = lambda x: tick if x else cross

    start_time = timeit.default_timer()
    tst_create_index_table: bool = test_create_index_table()
    tst_create_trade_report_table: bool = test_create_trade_report_table()
    tst_create_bank_statement_table: bool = test_create_bank_statment_table()
    tst_get_day_zero_bank_statements: bool = test_get_day_zero_bank_statements()
    tst_create_index_nav_table: bool = test_create_index_nav_table()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Database test results ====")
    print(f"test_create_index_table: {emoji(tst_create_index_table)}")
    print(f"test_create_trade_report_table: {emoji(tst_create_trade_report_table)}")
    print(f"test_create_bank_statement_table: {emoji(tst_create_bank_statement_table)}")
    print(
        f"test_get_day_zero_bank_statements: {emoji(tst_get_day_zero_bank_statements)}"
    )
    print(f"test_create_index_nav_table: {emoji(tst_create_index_nav_table)}")
    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
