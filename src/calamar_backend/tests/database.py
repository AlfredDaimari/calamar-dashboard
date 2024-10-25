from calamar_backend import database as db
import timeit

def test_create_index_table() -> bool:
    ticker = "nifty50"

    try:
        db_ = db.Database()
        db_.create_index_table(ticker, "2023-12-12", "2024-05-07")
        cur = db_.conn.cursor()
        cur.execute(f"SELECT Date, Close FROM {ticker}_price LIMIT 2")
        rows = cur.fetchall()
        print(f"test_create_index_table_results: {rows}")

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
        print(f"test_create_trade_report_table_results: {rows}")

    except Exception as e:
        print(e)
        return False

    return True


def test_create_bank_statment_table() -> bool:
    try:
        db_ = db.Database()
        db_.create_bank_statment_table()
        cur = db_.conn.cursor()
        cur.execute(
            f"SELECT particulars, cost_center, posting_date FROM bank_statement LIMIT 2"
        )
        rows = cur.fetchall()
        print(f"test_create_bank_statement_table_results: {rows}")

    except Exception as e:
        print(e)
        return False
    return True


def main():
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
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Database test results ====")
    print(f"test_create_index_table: {emoji(tst_create_index_table)}")
    print(f"test_create_trade_report_table: {emoji(tst_create_trade_report_table)}")
    print(f"test_create_bank_statement_table: {emoji(tst_create_bank_statement_table)}")
    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
