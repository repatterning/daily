"""Module filtering.py"""
import logging
import sys

import pandas as pd

import src.functions.cache


class Filtering:
    """
    Filtering
    """

    def __init__(self):
        pass

    @staticmethod
    def __filter(data: pd.DataFrame, attributes: dict) -> pd.DataFrame:
        """

        :param data:
        :return:
        """

        codes: list = attributes.get('excerpt')

        # Feed
        catchments = data.copy().loc[data['ts_id'].isin(codes), 'catchment_id'].unique()
        if sum(catchments) == 0:
            src.functions.cache.Cache().exc()
            sys.exit('None of the time series codes is valid')

        _gauges = data.copy().loc[data['catchment_id'].isin(catchments), :]

        # Logging
        elements = _gauges['ts_id'].unique()
        logging.info('The feed is requesting emergency intelligence for %s gauges, %s.  '
                     'Intelligence is possible for %s gauges, %s', len(codes), codes, elements.shape[0], elements)

        return _gauges

    def exc(self, data: pd.DataFrame, attributes: dict) -> pd.DataFrame:
        """

        :param data:
        :param attributes:
        :return:
        """

        return self.__filter(data=data, attributes=attributes)
