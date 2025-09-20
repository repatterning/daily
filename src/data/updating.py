"""Module data/updating.py"""
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.text_attributes as txa
import src.functions.streams


class Updating:
    """

    """

    def __init__(self, s3_parameters: s3p.S3Parameters, settings: dict):
        """

        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :param settings: starting, ending, period, year
        """

        self.__s3_parameters = s3_parameters
        self.__settings = settings

        # Instances
        self.__streams = src.functions.streams.Streams()

    def __update(self, uri: str, frame: pd.DataFrame) -> str:
        """

        :param uri: Storage path.
        :param frame: The data of a gauge.
        :return:
        """

        # If a file of interest does not exist, an empty data frame is returned
        text = txa.TextAttributes(uri=uri, header=0)
        original = self.__streams.read(text=text)

        # Concatenating
        instances = pd.concat([original, frame], axis=0, ignore_index=True)
        instances.drop_duplicates(inplace=True)

        return self.__streams.write(blob=instances, path=uri)

    def exc(self, frame: pd.DataFrame, ts_id: int, catchment_id: int) -> str:
        """

        :param frame: The data of a gauge.
        :param ts_id: A gauge's time series code.
        :param catchment_id: The gauge's catchment code.
        :return:
        """

        date = self.__settings.get('year') + '-01-01'
        uri = (f's3://{self.__s3_parameters.internal}/{self.__s3_parameters.path_internal_data}series/'
               f'{catchment_id}/{ts_id}/{date}.csv')

        return self.__update(uri=uri, frame=frame)
