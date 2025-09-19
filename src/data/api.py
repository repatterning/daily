
import src.functions.objects


class API:

    def __init__(self):

        self.__url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
                      '&request=getTimeseriesValues&ts_path=1/*/SG/15m.Cmd&from={starting}&to={ending}'
                      '&returnfields=Timestamp,Value,Quality Code&metadata=true'
                      '&md_returnfields=ts_id,station_id,catchment_id&dateformat=UNIX&format=json')

    def exc(self, starting: str, ending: str) -> dict | list[dict]:
        """

        :param starting: Format yyyy-mm-dd
        :param ending: Format yyyy-mm-dd
        :return:
        """

        url = self.__url.format(starting=starting, ending=ending)

        return src.functions.objects.Objects().api(url=url)
