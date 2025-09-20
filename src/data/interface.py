"""Module interface.py"""
import logging
import collections
import sys
import datetime

import boto3
import pandas as pd

import src.data.gauges

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.elements.partitions as pr
import src.functions.cache


class Interface:
    """
    Interface
    """

    def __init__(self, connector: boto3.session.Session, service: sr.Service,
                 s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param service:
        :param s3_parameters:
        :param attributes: A set of data acquisition attributes.
        """

        self.__connector = connector
        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

    def __filter(self, gauges: pd.DataFrame) -> pd.DataFrame:
        """

        :param gauges:
        :return:
        """

        codes: list = self.__attributes.get('excerpt')

        # Daily
        if len(codes) == 0:
            return gauges

        # Feed
        catchments = gauges.copy().loc[gauges['ts_id'].isin(codes), 'catchment_id'].unique()
        if sum(catchments) == 0:
            src.functions.cache.Cache().exc()
            sys.exit('None of the time series codes is valid')

        _gauges = gauges.copy().loc[gauges['catchment_id'].isin(catchments), :]

        # Logging
        elements = _gauges['ts_id'].unique()
        logging.info('The feed is requesting emergency intelligence for %s gauges, %s.  '
                     'Intelligence is possible for %s gauges, %s', len(codes), codes, elements.shape[0], elements)

        return _gauges

    def exc(self):
        """

        :return:
        """

        # Gauges
        gauges = src.data.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()
        gauges = self.__filter(gauges=gauges.copy())

        # Partitions for parallel data retrieval; for parallel computing.
        gauges_: pd.DataFrame = gauges.copy()[['catchment_id', 'ts_id']]
        objects: pd.Series = gauges_.apply(lambda x: pr.Partitions(**dict(x)), axis=1)
        partitions: list[pr.Partitions] = objects.tolist()

        # Logic
        stamp = datetime.datetime.now()
        if (stamp.month == 1) & (stamp.day == 1):
            logging.info('Split arithmetic')
        else:
            logging.info(list(map(lambda x: x.ts_id, partitions)))
            listings = [{int(p.ts_id): int(p.catchment_id)} for p in partitions]
            logging.info(dict(collections.ChainMap(*listings)))
