"""Module partitions.py"""
import typing


class Partitions(typing.NamedTuple):
    """
    The data type class ⇾ Partitions<br><br>

    Attributes<br>
    ----------<br>
    <b>ts_id</b>: int<br>
        The identification code of a time series.<br><br>
    <b>catchment_id</b>: int<br>
        The identification code of a catchment area.<br><br>
    """

    ts_id: int
    catchment_id: int
