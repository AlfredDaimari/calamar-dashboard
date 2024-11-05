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
import calamar_backend.utils as ut
import calamar_backend.errors as er


class TickerType(enum.Enum):
    isin = 0
    ticker = 1
    map_ = 2
    nan = 3


class DatabaseCSV:
    """
    Reads and writes FY equity price csv data
    """

    csv_dir_path = None

    def __init__(self, mem_slots: int) -> None:
        DatabaseCSV.csv_dir_path = os.getenv("CALAMAR_CSV_DB")

        if DatabaseCSV.csv_dir_path is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'CALAMAR_CSV_DB' not set"
            )

        self.mem_slots = mem_slots
        self.lru: list[tuple[str, int, pd.DataFrame]] = []

    @classmethod
    def get_csv_file_path(cls, isin: str, fy: int) -> str:
        """
        isin :parameter: isin or yahoo ticker
        fy: :parameter: financial year
        """
        return f"{cls.csv_dir_path}/{isin}_{fy}"

    @classmethod
    def file_exists(
        cls, isin: str, fy: int, ticker: str, map_: str = ""
    ) -> tuple[bool, TickerType]:
        """
        Checks if file exists in csv database
        """
        csv_isin_file = pathlib.Path(cls.get_csv_file_path(isin, fy))
        ret = csv_isin_file.is_file()
        if ret:
            return (ret, TickerType.isin)

        # now test for ticker
        csv_ticker_file = pathlib.Path(cls.get_csv_file_path(ticker, fy))
        ret = csv_ticker_file.is_file()
        if ret:
            return (ret, TickerType.ticker)

        # testing for map
        if map_ != "":
            csv_map_file = pathlib.Path(cls.get_csv_file_path(map_, fy))
            ret = csv_map_file.is_file()
            if ret:
                return (ret, TickerType.map_)

        return (False, TickerType.nan)

    def __lru_find_dataframe(
        self,
        isin: str,
        fy: int,
    ) -> int:
        """
        Get df location if it exists in memory
        isin :paramter: can be isin, ticker or map_

        Returns:
            location or -1
        """
        for i in range(len(self.lru)):
            if self.lru[i][0] == isin and self.lru[i][1] == fy:
                return i

        return -1

    def lru_append_data(self, isin: str, fy: int, df: pd.DataFrame) -> None:
        """
        Implements an LRU memory storage using pandas
        Only a certain amount of df's are kept in memory

        :parameter isin: can be isin, ticker or map_
        """
        mem_loc = self.__lru_find_dataframe(isin, fy)

        if mem_loc == -1:
            if len(self.lru) == self.mem_slots:
                self.lru.pop(0)
                self.lru.append((isin, fy, df))

            else:
                self.lru.append((isin, fy, df))
        else:
            self.lru.pop(mem_loc)
            self.lru.append((isin, fy, df))

    def __read_df_from_lru(self, loc: int) -> pd.DataFrame:
        """
        Read from LRU
        """
        df = self.lru[loc][-1]
        [isin, fy] = [self.lru[loc][0], self.lru[loc][1]]
        self.lru_append_data(isin, fy, df)
        return df

    def __read_df_from_csv_dir(self, isin: str, fy: int) -> pd.DataFrame:
        """
        Read data from CSV directory
        """
        df = pd.read_csv(self.get_csv_file_path(isin, fy))
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
        self.lru_append_data(isin, fy, df)
        return df

    def __read_df_from_yf(
        self, isin: str, fy: int, ticker: str, map_: str = ""
    ) -> pd.DataFrame:
        """
        Read data from yahoo finance
        ticker :parameter: unique isin number
        """
        [start, end] = time.date_in_fy_start_end(fy)

        df = yf_download_price(isin, start, end)

        if len(df) == 0:  # now use ticker
            df = yf_download_price(ticker, start, end)
            isin = ticker

            if len(df) == 0 and map_ != "":  # now use map_
                df = yf_download_price(map_, start, end)
                isin = map_

        if len(df) == 0:
            raise Exception(
                f"{str(datetime.datetime.now())}: db_csv:__read_from_yf: "
                "isin, ticker, map_!"
            )

        df.to_csv(
            self.get_csv_file_path(isin, fy),
            index=True,
            index_label="Date",
        )

        self.lru_append_data(isin, fy, df)
        return df

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
        count = 5
        file_exists = False

        try:
            map_ = calamar_ticker_map.get(ticker)
        except er.NoTickerMappingError:
            map_ = ""

        ticker = ut.ticker_to_yf_ticker(ticker)

        # loop until price is found
        while True:
            try:
                [file_exists, file_type] = self.file_exists(
                    isin, fy, ticker, map_
                )

                match file_type:
                    case TickerType.ticker:
                        tmp_isin = ticker
                    case TickerType.map_:
                        tmp_isin = map_
                    case _:
                        tmp_isin = isin  # keep isin as isin

                if file_exists:
                    loc = self.__lru_find_dataframe(tmp_isin, fy)
                    if loc != -1:
                        df = self.__read_df_from_lru(loc)
                    else:
                        df = self.__read_df_from_csv_dir(tmp_isin, fy)

                else:
                    df = self.__read_df_from_yf(isin, fy, ticker, map_)

                ret = df.loc[dt]
                break

            except KeyError:
                # go forward by one date (limit of upto 5)
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
