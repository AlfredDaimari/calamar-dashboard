import timeit
import datetime
from calamar_backend import interface as inf
from calamar_backend import database as db

def test_get_current_date() -> bool:
    utc_now = datetime.datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    utc_now_minus_1 = utc_now - datetime.timedelta(days=1)
    bl = False

    try:
        cur_date = inf.Time.get_current_date()
        cur_date_str = inf.Time.convert_date_to_strf(cur_date)
        bl = cur_date_str == utc_now_minus_1.strftime(inf.Time.DATE_FORMAT)
        print(f"\ntest_get_current_date_results: {cur_date}")

    except Exception as e:
        print(e)
        return False
    return bl


def main():
    print("==== Utils testing ====")
    OKGREEN = "\033[92m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    tick = OKGREEN + "\N{check mark}" + ENDC
    cross = FAIL + "\N{cross mark}" + ENDC

    emoji = lambda x: tick if x else cross

    start_time = timeit.default_timer()
    tst_get_current_date = test_get_current_date()
    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time

    print("\n\n==== Utils test results ====")
    print(f"test_get_current_date: {emoji(tst_get_current_date)}")

    print("\n")
    print(f"Total elapsed time for database tests: {elapsed_time}")
    print("\n")


if __name__ == "__main__":
    main()
