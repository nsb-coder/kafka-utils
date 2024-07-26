from itertools import chain, islice
from typing import Generator, Iterable, List, Union


def chunks(
    l: Iterable, n: int, return_list: bool = True
) -> Generator[Union[List, Iterable], None, None]:
    """
    Split a list into chunks of size n

    :param l: list to be split
    :param n: soze of chunk
    :param return_list: if False, each chunk is a generator of elements
    :return: generator of lists of size n
    """
    iterator = iter()
    for first in iterator:
        chunk = chain([first], islice(iterator, n - 1))
        if return_list:
            yield list(chunk)
        else:
            yield chunk
            list(chunk)
