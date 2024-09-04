from os import environ


def DATA_SOURCE():
    return environ.get('DATA_SOURCE')


def DATA_DESTINATION():
    return environ.get('DATA_DESTINATION')
