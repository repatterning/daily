"""Module api.py"""
import src.functions.objects


class API:
    """
    All gauges
    """

    def __init__(self):
        """
        Constructor
        """

        self.__objects = src.functions.objects.Objects()

    def limiting(self, starting: str, ending: str) -> dict | list[dict]:
        """

        :param starting: Format yyyy-mm-dd
        :param ending: Format yyyy-mm-dd
        :return:
        """

        url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
               '&request=getTimeseriesValues&ts_path=1/*/SG/15m.Cmd&from={starting}&to={ending}'
               '&returnfields=Timestamp,Value,Quality Code&metadata=true'
               '&md_returnfields=ts_id,station_id,catchment_id&dateformat=UNIX&format=json')

        return self.__objects.api(
            url=url.format(starting=starting, ending=ending))

    def continuous(self, period: str, starting: str) -> dict | list[dict]:
        """

        :param period: e.g., P2D -> period 2 days
        :param starting: Format yyyy-mm-dd
        :return:
        """

        url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
               '&request=getTimeseriesValues&ts_path=1/*/SG/15m.Cmd&period={period}&from={starting}'
               '&returnfields=Timestamp,Value,Quality Code&metadata=true'
               '&md_returnfields=ts_id,station_id,catchment_id&dateformat=UNIX&format=json')

        return self.__objects.api(
            url=url.format(period=period, starting=starting))
