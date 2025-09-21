"""Module gauges.py"""
import itertools
import os

import dask
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys
import src.data.filtering


class Gauges:
    """
    Retrieves the catchment & time series codes of the gauges in focus.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :param attributes: A set of data acquisition attributes.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

        # Instances
        self.__objects = src.s3.keys.Keys(service=self.__service, bucket_name=self.__s3_parameters.internal)
        self.__filtering = src.data.filtering.Filtering()

    def __get_codes(self, data: pd.DataFrame) -> dict:
        """

        :param data:
        :return:
        """

        data = data if len(self.__attributes.get('excerpt')) == 0 \
            else self.__filtering.exc(data=data, attributes=self.__attributes)
        instances: pd.DataFrame = data.copy()[['catchment_id', 'ts_id']]
        codes: dict = instances.set_index(keys='ts_id', drop=True).to_dict(orient='dict')['catchment_id']

        return codes

    @dask.delayed
    def __get_section(self, listing: str) -> pd.DataFrame:
        """

        :param listing:
        :return:
        """

        catchment_id = os.path.basename(os.path.dirname(listing))

        # The corresponding prefixes
        prefixes = self.__objects.excerpt(prefix=listing, delimiter='/')
        series_ = [os.path.basename(os.path.dirname(prefix)) for prefix in prefixes]

        # A frame of catchment & time series identification codes
        frame = pd.DataFrame(
            data={'catchment_id': itertools.repeat(catchment_id, len(series_)),
                  'ts_id': series_})

        return frame

    def exc(self) -> dict:
        """

        :return:
        """

        listings = self.__objects.excerpt(prefix='data/series/', delimiter='/')

        computations = []
        for listing in listings:
            frame = self.__get_section(listing=listing)
            computations.append(frame)
        frames = dask.compute(computations, scheduler='threads')[0]
        data = pd.concat(frames, ignore_index=True, axis=0)

        data['catchment_id'] = data['catchment_id'].astype(dtype=np.int64)
        data['ts_id'] = data['ts_id'].astype(dtype=np.int64)

        return self.__get_codes(data=data)
