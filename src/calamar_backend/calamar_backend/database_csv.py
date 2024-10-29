"""
CSV Database
    Read:
        - read from CSV Dir
        - read from LRU
        - read from yahoo finance
"""
import datetime
import typing
import pandas as pd
import os
import pathlib
from calamar_backend.price import download_price as yf_download_price
from calamar_backend.maps import TickerMap
from calamar_backend.interface import Time
from calamar_backend.errors import DayClosePriceNotFoundError


class DatabaseCSV:
    """
    Reads and writes FY equity price csv data
    """

    def __init__(self, mem_slots: int) -> None:
        self.map = TickerMap()  # make a global ticker map variable
        self.csv_dir_path = os.getenv("CALAMAR_CSV_DB")

        if self.csv_dir_path is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'CALAMAR_CSV_DB' not set"
            )
        self.mem_slots = mem_slots
        self.lru: list[tuple[str, int, pd.DataFrame]] = []

    def get_csv_file_path(self, ticker_fy: tuple[str, int]) -> str:
        return f"{self.csv_dir_path}/{ticker_fy[0]}_{ticker_fy[-1]}"

    def __file_exists(self, ticker_fy: tuple[str, int]) -> bool:
        csv_file = pathlib.Path(self.get_csv_file_path(ticker_fy))
        return csv_file.is_file()

    def __lru_find_dataframe(self, ticker_fy: tuple[str, int]) -> int:
        """
        Get df location if it exists in memory

        Returns:
            df or None
        """
        [ticker, fy] = ticker_fy

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

    def __read_df_from_lru(
        self, ticker_fy: tuple[str, int], loc: int
    ) -> pd.DataFrame:
        """
        Read from LRU
        """
        df = self.lru[loc][-1]
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

    def __read_df_from_yf(self, ticker_fy: tuple[str, int]) -> pd.DataFrame:
        """
        Read data from yahoo finance
        """
        [ticker, fy] = ticker_fy
        [start, end] = Time.date_in_fy_start_end(fy)

        df = yf_download_price(self.map.get(ticker), start, end)
        df.to_csv(
            self.get_csv_file_path(ticker_fy),
            index=True,
            index_label="Date",
        )

        self.lru_append_data(ticker_fy, df)
        return df

    def read(
        self, ticker: str, date: datetime.datetime
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
        fy = Time.date_fy(date)
        ret: pd.Series

        if self.__file_exists((ticker, fy)):
            loc = self.__lru_find_dataframe((ticker, fy))

            if loc != -1:
                df = self.__read_df_from_lru((ticker, fy), loc)
            else:
                df = self.__read_df_from_csv_dir((ticker, fy))
        else:
            df = self.__read_df_from_yf((ticker, fy))

        try:
            ret = df.loc[Time.convert_date_to_strf(date)]

        except KeyError:
            raise DayClosePriceNotFoundError

        except Exception as e:
            raise Exception(
                f"{datetime.datetime.now()}: "
                f"db_csv.read({ticker},{Time.convert_date_to_strf(date)}): "
                f"something went wrong - {e}!"
            )

        return (loc, ret)
