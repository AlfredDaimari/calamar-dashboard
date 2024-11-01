import calamar_backend.database as db
import calamar_backend.table_interface as inf
import timeit
import calamar_backend.time as time

ticker = "nifty50"
start = "2019-12-10"
end = "2024-10-25"


def test_create_index_table() -> bool:
    inf.Index.set(ticker)
    inf.Index.set_date(start, end)

    try:
        db_ = db.Database()
        db_.create_index_table(ticker, start, end)
        rows = []
        rows += inf.Index.get(
            db_.conn, time.convert_date_strf_to_strp("2019-12-12 00:00:00")
        )
        rows += inf.Index.get(
            db_.conn, time.convert_date_strf_to_strp("2020-12-15 00:00:00")
        )
        print(f"\ntest_create_index_table_results: {list(map(str, rows))}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_index_nav_table() -> bool:
    ticker = "nifty50"

    try:
        db_ = db.Database()
        inf.IndexNAV.set(ticker)
        db_.create_index_nav_table(ticker)
        rows = []
        rows += inf.IndexNAV.get(
            db_.conn, time.convert_date_strf_to_strp("2019-12-12 00:00:00")
        )
        rows += inf.IndexNAV.get(
            db_.conn, time.convert_date_strf_to_strp("2020-12-15 00:00:00")
        )
        print(f"\ntest_create_index_nav_table_results: {list(map(str, rows))}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_trade_report_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_trade_report_table()
        # cur = db_.conn.cursor()
        # cur.execute(
        #    f'SELECT ISIN, Quantity, "Trade Date"  FROM trade_report LIMIT 2'
        # )
        rows = inf.TradeReport.get_day_zero(db_.conn)
        print(
            f"\ntest_create_trade_report_table_results: {list(map(str,rows))}"
        )

    except Exception as e:
        print(e)
        return False

    return True


def test_create_bank_statment_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_bank_statment_table()
        # cur = db_.conn.cursor()
        # cur.execute(f"SELECT * FROM bank_statement LIMIT 2")
        rows = inf.BankStatement.get_day_zero(db_.conn)
        print(
            f"\ntest_create_bank_statement_table_results: {list(map(str,rows))}"
        )

    except Exception as e:
        print(e)
        return False
    return True


def test_create_portfolio_table() -> bool:
    try:
        db_ = db.Database()
        trade_nav = db_.create_portfolio_table()
        rows = inf.Portfolio.get_day_zero(db_.conn)
        print(
            f"\ntest_create_portfolio_nav_table_results: {list(map(str, rows))}"
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
    tst_create_index_nav_table: bool = test_create_index_nav_table()
    tst_create_portfolio_nav_table: bool = test_create_portfolio_table()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Database test results ====")
    print(f"test_create_index_table: {emoji(tst_create_index_table)}")
    print(
        f"test_create_trade_report_table: {emoji(tst_create_trade_report_table)}"
    )
    print(
        f"test_create_bank_statement_table: {emoji(tst_create_bank_statement_table)}"
    )
    print(f"test_create_index_nav_table: {emoji(tst_create_index_nav_table)}")
    print(
        f"test_create_portfolio_table: {emoji(tst_create_portfolio_nav_table)}"
    )
    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
