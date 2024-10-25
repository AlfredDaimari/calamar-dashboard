# 
# Description:
#   This script should only be run form the root calamar_backend directory
#

export PYTHONPATH=.
export CALAMAR_DB=/home/alfred/Code/projects/calamar_dashboard/src/calamar_backend/.temp/database.db
export TICKER_MAP=/home/alfred/Code/projects/calamar_dashboard/src/calamar_backend/.temp/map.yaml
export ZERODHA_TRADE_REPORT=/home/alfred/Code/projects/calamar_dashboard/data/portfolio.csv
export ZERODHA_BANK_STATEMENT=/home/alfred/Code/projects/calamar_dashboard/data/ledger-FP9847.csv

# add tests to run
tests=(tests/database.py tests/utils.py)
for test in ${tests[@]}
do
  echo "running ${test}"
  python3 $test
done