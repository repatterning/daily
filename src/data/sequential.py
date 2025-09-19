
import src.data.api
import src.elements.partitions as pr

class Sequential:

    def __init__(self, starting: str, ending: str, partitions: pr.Partitions):

        self.__starting = starting
        self.__ending = ending

        self.__data: list[dict] = src.data.api.API().exc(
            starting=self.__starting, ending=self.__ending)

        self.__codes = list(map(lambda x: x.ts_id, partitions))

    def __get_values(self):
        pass

    def exc(self):
        pass
