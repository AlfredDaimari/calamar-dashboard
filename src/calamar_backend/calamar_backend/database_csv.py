"""
CSV Database
    Read:
        - read from CSV Dir
        - read from LRU
        - read from yahoo finance
"""
import datetime
import pandas as pd
import os
import pathlib
import enum
import typing

import calamar_backend.time as time
from calamar_backend.maps import calamar_ticker_map
from calamar_backend.price import download_price as yf_download_price


class TickerType(enum.Enum):
    isin = 0
    ticker = 1
    map_ = 2


class DatabaseCSV:
    """
    Reads and writes FY equity price csv data
    """

    def __init__(self, mem_slots: int) -> None:
        self.csv_dir_path = os.getenv("CALAMAR_CSV_DB")

        if self.csv_dir_path is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'CALAMAR_CSV_DB' not set"
            )
        self.mem_slots = mem_slots
        self.lru: list[tuple[str, int, pd.DataFrame]] = []

    def __ticker_to_yf_ticker(self, ticker: str) -> str:
        """
        Returns the yahoo ticker format
        """
        return ticker + ".NS"

    def get_csv_file_path(self, ticker_fy: tuple[str, int]) -> str:
        return f"{self.csv_dir_path}/{ticker_fy[0]}_{ticker_fy[-1]}"

    def __file_exists(
        self, isin_fy: tuple[str, int], ticker: str = "", map_: str = ""
    ) -> tuple[bool, TickerType]:
        """
        - Checks if file exists in csv database
          checks for all types (isin, ticker, map_)
        """
        csv_isin_file = pathlib.Path(self.get_csv_file_path(isin_fy))
        ret = csv_isin_file.is_file()
        if ret:
            return (ret, TickerType.isin)

        # now test for ticker
        csv_ticker_file = pathlib.Path(
            self.get_csv_file_path((ticker, isin_fy[1]))
        )
        ret = csv_ticker_file.is_file()
        if ret:
            return (ret, TickerType.ticker)

        try:
            csv_map_file = pathlib.Path(
                self.get_csv_file_path((map_, isin_fy[1]))
            )
            ret = csv_map_file.is_file()
            return (ret, TickerType.map_)

        except:
            return (False, TickerType.map_)

    def __lru_find_dataframe(
        self,
        ticker_fy: tuple[str, int],
        ticker: str = "",
        map_: str = "",
        file_type: TickerType = TickerType.isin,
    ) -> int:
        """
        Get df location if it exists in memory

        Returns:
            location or -1
        """
        [ticker_, fy] = ticker_fy

        if file_type == TickerType.isin:
            ticker = ticker_

        if file_type == TickerType.map_:
            ticker = map_

        for i in range(len(self.lru)):
            if self.lru[i][0] == ticker and self.lru[i][1] == fy:
                return i
        else:
            return -1

    def lru_append_data(
        self, ticker_fy: tuple[str, int], df: pd.DataFrame
    ) -> None:
        """
        Implements an LRU memory storage using pandas
        Only a certain amount of df's are kept in memory

        :parameter ticker_fy: (ticker, int)
        """
        [ticker, fy] = ticker_fy
        mem_loc = self.__lru_find_dataframe(ticker_fy)

        if mem_loc == -1:
            if len(self.lru) == self.mem_slots:
                self.lru.pop(0)
                self.lru.append((ticker, fy, df))

            else:
                self.lru.append((ticker, fy, df))
        else:
            self.lru.pop(mem_loc)
            self.lru.append((ticker, fy, df))

    def __read_df_from_lru(self, loc: int) -> pd.DataFrame:
        """
        Read from LRU
        """
        df = self.lru[loc][-1]
        ticker_fy: tuple[str, int] = (self.lru[loc][0], self.lru[loc][1])
        self.lru_append_data(ticker_fy, df)
        return df

    def __read_df_from_csv_dir(
        self, ticker_fy: tuple[str, int]
    ) -> pd.DataFrame:
        """
        Read data from CSV directory
        """
        df = pd.read_csv(self.get_csv_file_path(ticker_fy))
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
        self.lru_append_data(ticker_fy, df)
        return df

    def __read_df_from_yf(
        self, ticker_fy: tuple[str, int], ticker: str = "", map_: str = ""
    ) -> pd.DataFrame:
        """
        Read data from yahoo finance
        ticker :parameter: unique isin number
        """
        [isin, fy] = ticker_fy
        [start, end] = time.date_in_fy_start_end(fy)
        df: typing.Optional[pd.DataFrame] = None

        useTicker = False
        useMap = False

        try:
            df = yf_download_price(isin, start, end)
        except:
            useTicker = True

        if useTicker:
            try:
                df = yf_download_price(ticker, start, end)
                ticker_fy = (ticker, fy)
            except:
                useMap = True

        if useMap:
            try:
                df = yf_download_price(map_, start, end)
                ticker_fy = (map_, fy)
            except:
                raise Exception(
                    f"{str(datetime.datetime.now())}: Cannot "
                    "download price using isin, ticker, map"
                )

        if df is not None:
            df.to_csv(
                self.get_csv_file_path(ticker_fy),
                index=True,
                index_label="Date",
            )

            self.lru_append_data(ticker_fy, df)
            return df
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: db_csv:__read_from_yf: "
                "something went wrong!"
            )

    def read(
        self, isin: str, date: datetime.datetime, ticker: str = ""
    ) -> tuple[int, pd.Series | None]:
        """
        - get fy year
        - check if file exists
        - download file if it does not exist
        - add file to lru
        - return series or None

        Returns:
        [int, pd.Series| None]
        the integer variable is the location of df in LRU
        """

        loc: int = -1  # location of DF in LRU
        fy = time.date_fy(date)
        ret = None
        df = None
        dt = time.convert_date_to_strf(date)
        count = 7
        file_exists = False

        try:
            map_ = calamar_ticker_map.get(ticker)
        except:
            map_ = ""

        ticker = self.__ticker_to_yf_ticker(ticker)

        # loop until price is found
        while True:
            try:
                [file_exists, file_type] = self.__file_exists(
                    (isin, fy), ticker, map_
                )

                if file_exists:
                    loc = self.__lru_find_dataframe(
                        (isin, fy), ticker, map_, file_type
                    )

                    if file_type == TickerType.isin:
                        ticker_fy = (isin, fy)
                    elif file_type == TickerType.ticker:
                        ticker_fy = (ticker, fy)
                    else:
                        ticker_fy = (map_, fy)

                    if loc != -1:
                        df = self.__read_df_from_lru(loc)
                    else:
                        df = self.__read_df_from_csv_dir(ticker_fy)
                else:
                    df = self.__read_df_from_yf((isin, fy), ticker, map_)

                ret = df.loc[dt]
                break

            except KeyError:
                # go forward by one date (limit of upto 7)
                if count <= 0:
                    raise Exception(
                        f"{str(datetime.datetime.now())}: "
                        "something went wrong, can't get "
                        f"price for {str(date)} {ticker} - {isin} "
                        f"file_exists: {file_exists}, loc:{self.lru[loc]}"
                    )

                count -= 1
                date += datetime.timedelta(days=1)
                fy = time.date_fy(date)
                dt = time.convert_date_to_strf(date)
                continue

        return (loc, ret)


db_csv = DatabaseCSV(50)
