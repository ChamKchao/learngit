import pandas as pd


def getObservations():
    columns = ['site', 'year', 'month', 'measurement']

    observations = pd.read_excel('download/observations.xlsx', sheet_name='data', names=columns,
                                 header=0, index_col=None, nrows=576, usecols=[1, 2, 3, 4])

    return observations
