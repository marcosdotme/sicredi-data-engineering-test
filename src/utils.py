from random import choice
from typing import Dict, List, Union


def query_list_dict(list_dict: List[Dict], key: str, value: Union[str, int]) -> Dict:
    """Search for a value in an list of dictionaries and return the dictionary.

    If there's more than one match, return only the first match.

    Arguments
    ---------
        list_dict `List[Dict]`: Dictionary to search.
        key `str`: Key dictionary to search.
        value `Union[str, int]`: Key value to search.

    Returns
    -------
        `Dict`

    Example usage
    -------------
    >>> my_list = [{'id': 1, 'name': 'Chris'}, {'id': 2, 'name': 'Josh'}]
    >>> query_list_dict(list_dict = my_list, key = 'id', value = 1)
    {'id': 1, 'name': 'Chris'}
    """
    result = [item for item in list_dict if item[key] == value]
    return result[0]


def get_random_list_dict_item(list_dict: List[Dict]) -> List[Dict]:
    """Gets a random element from a list of dictionaries.

    Arguments
    ---------
        list_dict `List[Dict]`: List to get the random item.

    Raises
    ------
        `TypeError`: Raise exception if all elements in list are not `dict` type.

    Returns
    -------
        `List[Dict]`

    Example usage
    -------------
    >>> my_list = [{'id': 1, 'name': 'Chris'}, {'id': 2, 'name': 'Josh'}]
    >>> get_random_list_dict_item(list_dict = my_list)
    [{'id': 2, 'name': 'Josh'}]
    """
    if not all(isinstance(item, dict) for item in list_dict):
        raise TypeError(f'`list_dict` argument must be a list of dict.')

    return [choice(list_dict)]
