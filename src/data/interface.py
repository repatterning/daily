"""Module interface.py"""
import src.data.gauges
import src.data.partitions
import src.data.points
import src.elements.s3_parameters as s3p
import src.elements.service as sr


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


    def exc(self):
        """

        :return:
        """

        # Latest
        latest = src.data.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()

        # Partitions for parallel data retrieval; for parallel computing.
        partitions = src.data.partitions.Partitions(data=latest).exc(attributes=self.__attributes)

        # Retrieving time series points
        src.data.points.Points(period=self.__attributes.get('period')).exc(partitions=partitions)
