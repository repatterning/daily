"""Module data/sequential.py"""
import logging

import dask
import pandas as pd

import src.data.api
import src.data.updating
import src.elements.s3_parameters as s3p


class Sequential:
    """

    """

    def __init__(self, s3_parameters: s3p.S3Parameters, attributes: dict, codes: dict):
        """

        :param s3_parameters:
        :param attributes:
        :param codes:
        """

        self.__s3_parameters = s3_parameters

        # The latest data: each dictionary is the latest data of a gauge.
        self.__data: list[dict] = src.data.api.API().continuous(
            starting=attributes.get('starting'), period=attributes.get('period'))

        # An instance for updating
        self.__updating = src.data.updating.Updating(s3_parameters=s3_parameters, attributes=attributes)

        # key: ts_id, value: catchment_id
        self.__codes = codes

        # Renaming
        self.__rename = {'Timestamp': 'timestamp', 'Value': 'value', 'Quality Code': 'quality_code'}

    @dask.delayed
    def __get_series(self, index: int):
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

    @dask.delayed
    def __update_series(self, index: int, frame: pd.DataFrame) -> str:
        """
        Upcoming: A call to an updating class

        :param index:
        :param frame:
        :return:
        """

        ts_id = int(self.__data[index]['ts_id'])
        catchment_id = int(self.__data[index]['catchment_id'])

        if ts_id in self.__codes.keys():
            return self.__updating.exc(frame=frame, ts_id=ts_id, catchment_id=catchment_id)

        return f'Inapplicable: {ts_id}, {catchment_id}'

    def exc(self):
        """

        :return:
        """

        computations = []
        for index in range(len(self.__data)):
            frame = self.__get_series(index=index)
            message = self.__update_series(index=index, frame=frame)
            computations.append(message)

        calculations = dask.compute(computations, scheduler='threads')[0]
        logging.info(calculations)
