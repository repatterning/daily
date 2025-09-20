"""Module api.py"""
import boto3

import src.data.special


class API:
    """
    All gauges
    """

    def __init__(self, connector: boto3.session.Session):
        """

        :param connector:
        """

        self.__special = src.data.special.Special(connector=connector)

    def limiting(self, starting: str, ending: str) -> dict | list[dict]:
        """
        self.__objects.api(
            url=url.format(starting=starting, ending=ending))

        :param starting: Format yyyy-mm-dd
        :param ending: Format yyyy-mm-dd
        :return:
        """

        url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
               '&request=getTimeseriesValues&ts_path=1/*/SG/15m.Cmd&from={starting}&to={ending}'
               '&returnfields=Timestamp,Value,Quality Code&metadata=true'
               '&md_returnfields=ts_id,station_id,catchment_id&dateformat=UNIX&format=json')

        return self.__special.exc(
            url=url.format(starting=starting, ending=ending))

    def continuous(self, period: str, starting: str) -> dict | list[dict]:
        """
        self.__objects.api(
            url=url.format(period=period, starting=starting))

        :param period: e.g., P2D -> period 2 days
        :param starting: Format yyyy-mm-dd
        :return:
        """

        url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
               '&request=getTimeseriesValues&ts_path=1/*/SG/15m.Cmd&period={period}&from={starting}'
               '&returnfields=Timestamp,Value,Quality Code&metadata=true'
               '&md_returnfields=ts_id,station_id,catchment_id&dateformat=UNIX&format=json')

        return self.__special.exc(
            url=url.format(period=period, starting=starting))
