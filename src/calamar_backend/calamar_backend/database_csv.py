# file to implement a basic file directory which acts as a database for equity price information
import datetime
import typing
import pandas as pd
import os
import pathlib
from calamar_backend.price import download_price as yf_download_price
from calamar_backend.maps import TickerMap
from calamar_backend.interface import Time


class DatabaseCSV:
    """
    Reads and writes FY equity price csv data
    """

    def __init__(self, mem_slots: int) -> None:
        self.map = TickerMap()  # make a global ticker map variable
        self.csv_dir_path = os.getenv("CALAMAR_CSV_DB")

        if self.csv_dir_path == None:
            raise Exception(
                f"{str(datetime.datetime.now())}: environment variable 'CALAMAR_CSV_DB' not set"
            )
        self.mem_slots = mem_slots
        self.lru: list[tuple[str, int, pd.DataFrame]] = []

    def __file_exists(self, ticker_fy: tuple[str, int]) -> bool:
        csv_file = pathlib.Path(
            f"{self.csv_dir_path}/{ticker_fy[0] + '_' + str(ticker_fy[-1])}"
        )
        return csv_file.is_file()

    def __lru_find_file(self, ticker_fy: tuple[str, int]) -> int:
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
        mem_loc = self.__lru_find_file(ticker_fy)

        if mem_loc == -1:
            if len(self.lru) == self.mem_slots:
                self.lru.pop(0)
                self.lru.append((ticker, fy, df))
            else:
                self.lru.append((ticker, fy, df))
        else:
            self.lru.pop(mem_loc)
            self.lru.append((ticker, fy, df))

    def read(self, ticker: str, date: datetime.datetime) -> pd.Series | None:
        """
        - get fy year
        - check if file exists
        - download file if it does not exist
        - add file to lru
        - return series or None

        Returns:
            df: pd.Dataframe
            None: when data does not exist
        """
        fy = Time.date_fy(date)
        ret: pd.Series

        if self.__file_exists((ticker, fy)):
            loc = self.__lru_find_file((ticker, fy))

            if loc != -1:
                df = self.lru[loc][-1]
                ret = df.loc[Time.convert_date_to_strf(date)]
                self.lru_append_data((ticker, fy), df)

            else:
                df = pd.read_csv(f"{self.csv_dir_path}/{ticker}_{fy}")
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date")
                self.lru_append_data((ticker, fy), df)
                ret = df.loc[Time.convert_date_to_strf(date)]

        else:
            [start, end] = Time.date_fy_start_end(date)

            df = yf_download_price(self.map.get(ticker), start, end)
            df.to_csv(
                f"{self.csv_dir_path}/{ticker}_{fy}",
                index=True,
                index_label="Date",
            )

            self.lru_append_data((ticker, fy), df)
            ret = df.loc[Time.convert_date_to_strf(date)]

        return ret
