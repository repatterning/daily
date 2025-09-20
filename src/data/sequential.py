import collections
import pandas as pd
import dask

import src.data.api
import src.elements.partitions as pr

class Sequential:

    def __init__(self, starting: str, ending: str, partitions: pr.Partitions):
        """

        :param starting:
        :param ending:
        :param partitions:
        """

        self.__data: list[dict] = src.data.api.API().exc(
            starting=starting, ending=ending)
        self.__partitions = partitions

        self.__codes = self.__get_codes()
        self.__rename = {'Timestamp': 'timestamp', 'Value': 'value', 'Quality Code': 'quality_code'}

    def __get_codes(self):
        """

        :return:
        """

        listings = [{int(p.ts_id): int(p.catchment_id)} for p in self.__partitions]

        return dict(collections.ChainMap(*listings))

    def __get_values(self, index: int):
        """

        :param index:
        :return:
        """

        if int(self.__data[index]['rows']) == 0:
            return pd.DataFrame()

        columns = self.__data[index]['columns'].split(',')
        frame = pd.DataFrame.from_records(data=self.__data[index]['data'], columns=columns)
        frame.rename(columns=self.__rename, inplace=True)
        frame = frame.assign(ts_id=int(self.__data[index]['ts_id']), )

        return frame

    def __updating(self, index: int, frame: pd.DataFrame):
        """
        Upcoming: A call to an updating class

        :param index:
        :param frame:
        :return:
        """

        ts_id = int(self.__data[index]['ts_id'])
        catchment_id = int(self.__data[index]['catchment_id'])

        if ts_id in self.__codes.keys():
            return True

        return False

    def exc(self):
        """

        :return:
        """

        computation = []
        for index in range(len(self.__data)):
            pass
