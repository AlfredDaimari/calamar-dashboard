import timeit
import os
import calamar_backend.time as time
import calamar_backend.utils as ut
from calamar_backend import database_csv as db


def test_read() -> bool:
    try:
        ticker = "RELIANCE"
        isin = "IFK345"
        db_ = db.DatabaseCSV(3)
        dates = {
            "d24": time.convert_date_strf_to_strp("2023-10-05 00:00:00"),
            "d23": time.convert_date_strf_to_strp("2022-10-06 00:00:00"),
            "d22": time.convert_date_strf_to_strp("2021-10-04 00:00:00"),
            "d21": time.convert_date_strf_to_strp("2020-10-07 00:00:00"),
        }

        # removing files for better tests
        for key in dates:
            fy: int = time.date_fy(dates[key])
            file_path = db_.get_csv_file_path("RELIANCE.NS", fy)

            if os.path.exists(file_path):
                os.remove(file_path)

        # reading files
        dates_keys = list(dates.keys())
        for i in range(len(dates_keys)):
            [loc, _] = db_.read(isin, dates[dates_keys[i]], ticker)
            assert loc == -1  # all files show be downloaded from yf

        [loc, output] = db_.read(isin, dates["d21"], ticker)
        assert loc == 2
        [loc, _] = db_.read(isin, dates["d24"], ticker)
        assert loc == -1

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
