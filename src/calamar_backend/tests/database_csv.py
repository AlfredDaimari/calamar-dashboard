import timeit
from calamar_backend import interface as inf
from calamar_backend import database_csv as db


def test_read() -> bool:
    try:
        db_ = db.DatabaseCSV(5)
        d = inf.Time("2023-10-05 00:00:00")
        output = db_.read("reliance", d.date)
        print(f"\ntest_read_results:{output}")

    except Exception as e:
        print(e)
        return False

    return True


def main():
    print("=== Database_csv testing ===")
    OKGREEN = "\033[92m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    tick = OKGREEN + "\N{check mark}" + ENDC
    cross = FAIL + "\N{cross mark}" + ENDC

    emoji = lambda x: tick if x else cross

    start_time = timeit.default_timer()
    tst_read = test_read()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Database csv test results ====")
    print(f"test_read: {emoji(tst_read)}")

    print("\n")
    print(f"Total elapsed time for database csv tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
