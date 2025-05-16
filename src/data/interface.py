"""Module interface.py"""
import logging
import sys
import pandas as pd

import src.data.gauges
import src.data.partitions
import src.data.points
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.cache


class Interface:
    """
    Interface
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param service:
        :param s3_parameters:
        :param attributes: A set of data acquisition attributes.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

    @staticmethod
    def __filter(blob: pd.DataFrame, codes: list[int]) -> pd.DataFrame:
        """

        :param blob:
        :param codes:
        :return:
        """

        states = blob['ts_id'].isin(codes)
        if sum(states) == 0:
            src.functions.cache.Cache().exc()
            sys.exit('None of the time series codes is valid')

        focus = blob.loc[states, 'ts_id'].unique()
        logging.info('The feed is requesting emergency intelligence for %s gauges, %s.  '
                     'Intelligence is possible for %s gauges, %s', len(codes), codes, focus.shape[0], focus)

        return blob.copy().loc[states, :]

    def exc(self, codes: list[int] | None):
        """

        :param codes:
        :return:
        """

        # Latest
        latest = src.data.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()
        if codes is not None:
            latest = self.__filter(blob=latest, codes=codes)

        # Partitions for parallel data retrieval; for parallel computing.
        partitions = src.data.partitions.Partitions(data=latest).exc(attributes=self.__attributes)

        # Retrieving time series points
        src.data.points.Points(period=self.__attributes.get('period')).exc(partitions=partitions)
