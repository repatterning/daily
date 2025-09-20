"""Module atypical.py"""
import datetime

import boto3

import src.data.api
import src.data.sequential
import src.elements.s3_parameters as s3p


class Atypical:
    """
    Addresses the straddling of years
    """

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters, codes: dict):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param s3_parameters:
        :param codes:
        """

        self.__s3_parameters = s3_parameters
        self.__codes = codes

        # An instance for reading gauge data
        self.__api = src.data.api.API(connector=connector)

    def continuous(self, settings: dict):
        """

        :param settings:
        :return:
        """

        # Each dictionary encodes the data of a gauge.
        data: list[dict] = self.__api.continuous(
            starting=settings.get('starting'), period=settings.get('period'))

        sequential = src.data.sequential.Sequential(
            data=data, s3_parameters=self.__s3_parameters, settings=settings, codes=self.__codes)
        sequential.exc()

    def limiting(self, settings: dict):
        """

        :param settings:
        :return:
        """

        # Each dictionary encodes the data of a gauge.
        data: list[dict] = self.__api.limiting(
            starting=settings.get('starting'), ending=settings.get('ending'))

        src.data.sequential.Sequential(
            data=data, s3_parameters=self.__s3_parameters, settings=settings, codes=self.__codes)
