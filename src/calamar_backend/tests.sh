# 
# Description:
#   This script should only be run form the root calamar_backend directory
#   run using -> pipenv run bash tests.sh

export PYTHONPATH=.
export CALAMAR_DB=/home/alfred/Code/projects/calamar_dashboard/src/.temp/database.db
export TICKER_MAP=/home/alfred/Code/projects/calamar_dashboard/src/.temp/map.yaml
export ZERODHA_TRADE_REPORT=/home/alfred/Code/projects/calamar_dashboard/data/portfolio.csv
export ZERODHA_BANK_STATEMENT=/home/alfred/Code/projects/calamar_dashboard/data/ledger-FP9847.csv
export CALAMAR_CSV_DB=/home/alfred/Code/projects/calamar_dashboard/src/.temp

# add tests to run
tests=(tests/database.py tests/utils.py tests/database_csv.py)
for test in ${tests[@]}
do
  echo "running ${test}"
  python3 $test
done
