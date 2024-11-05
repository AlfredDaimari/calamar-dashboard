import calamar_backend.database as db
import calamar_backend.table_interface as inf
import timeit
import calamar_backend.time as time

ticker = "nifty50"
start = "2019-12-10"


def test_create_index_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_index_table(ticker, start)
        db_.change_index_table(ticker)
        rows = []
        if db_.index_table is not None:
            rows += db_.index_table.get(
                db_.conn, time.convert_date_strf_to_strp("2019-12-12 00:00:00")
            )
            rows += db_.index_table.get(
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
        db_.create_index_nav_table(ticker)
        rows = []
        if db_.index_nav_table is not None:
            rows += db_.index_nav_table.get(
                db_.conn, time.convert_date_strf_to_strp("2019-12-12 00:00:00")
            )
            rows += db_.index_nav_table.get(
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
        rows = db_.tr_table.get_day_zero(db_.conn)
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
        rows = db_.bnk_table.get_day_zero(db_.conn)
        print(
            f"\ntest_create_bank_statement_table_results: "
            f"{list(map(str,rows))}"
        )

    except Exception as e:
        print(e)
        return False
    return True


def test_create_portfolio_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_portfolio_table()
        rows = db_.pft_table.get_day_zero(db_.conn)
        print(f"\ntest_create_portfolio_table_results: {list(map(str, rows))}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_portfolio_nav_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_portfolio_nav_table()
        rows = db_.pft_nav_table.get_day_zero(db_.conn)
        print(
            f"\ntest_create_portfolio_nav_table_results:{list(map(str, rows))}"
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
    tst_create_portfolio_table: bool = test_create_portfolio_table()
    tst_create_portfolio_nav_table: bool = test_create_portfolio_nav_table()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Database test results ====")
    print(f"test_create_index_table: {emoji(tst_create_index_table)}")
    print(
        "test_create_trade_report_table: "
        f"{emoji(tst_create_trade_report_table)}"
    )
    print(
        "test_create_bank_statement_table: "
        f"{emoji(tst_create_bank_statement_table)}"
    )
    print(f"test_create_index_nav_table: {emoji(tst_create_index_nav_table)}")
    print(f"test_create_portfolio_table: {emoji(tst_create_portfolio_table)}")
    print(
        "test_create_portfolio_nav_table: "
        f"{emoji(tst_create_portfolio_nav_table)}"
    )
    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
