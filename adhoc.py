import pandas as pd
import numpy as np
import observation as ob

data = ob.getObservations()

sites = ['THL0' + str(n) for n in range(0, 9)]
sites.remove('THL02')

years = data.year.unique()
months = [n for n in range(1, 13)]

allDfList = []
for site in sites:
    monthData = []
    df = pd.DataFrame()
    for year in years:
        monthData = data[(data.site == site) & (
            data.year == year)].measurement.values
    df = pd.DataFrame(np.column_stack([months, monthData]),
                      columns=['month', 'TP'])
    print(df)
    print("." * 20)
    allDfList.append(df)


# [(observations.year == 2003) &
#                     (observations.site == 'THL08')]
# print(data.values)
