"""Module interface.py"""
import logging
import datetime

import boto3
import pandas as pd

import src.data.gauges

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.cache
import src.data.filtering
import src.data.structure


class Interface:
    """
    Interface
    """

    def __init__(self, connector: boto3.session.Session, service: sr.Service,
                 s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :param attributes: A set of data acquisition attributes.
        """

        self.__connector = connector
        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

        # Instances
        self.__filtering = src.data.filtering.Filtering()

    def exc(self):
        """
        logging.info(list(map(lambda x: x.ts_id, partitions)))

        :return:
        """

        # Gauges
        gauges = src.data.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()
        codes = self.__attributes.get('excerpt')
        gauges = gauges if len(codes) == 0 else self.__filtering.exc(gauges=gauges, attributes=self.__attributes)

        # Partitions for parallel data retrieval; for parallel computing.
        gauges_: pd.DataFrame = gauges.copy()[['catchment_id', 'ts_id']]
        codes = gauges_.set_index(keys='ts_id', drop=True).to_dict(orient='dict')['catchment_id']
        logging.info(codes)

        # Logic
        stamp = datetime.datetime.now()
        yesterday = stamp - datetime.timedelta(days=1)
        structure = src.data.structure.Structure(connector=self.__connector, s3_parameters=self.__s3_parameters, codes=codes)
        if (stamp.month == 1) & (stamp.day == 1):
            logging.info('Straddling')
            settings = {'starting': f'{stamp.year}-01-01',
                        'period': 'P2D', 'year': stamp.year}
            structure.continuous(settings=settings)
            settings = {'starting': yesterday.strftime('%Y-%m-%d'),
                        'ending': stamp.strftime('%Y-%m-%d'), 'year': yesterday.year}
            structure.limiting(settings=settings)
        else:
            logging.info('Single')
            settings = {'starting': yesterday.strftime(format='%Y-%m-%d'),
                        'period': 'P2D', 'year': stamp.year}
            structure.continuous(settings=settings)
