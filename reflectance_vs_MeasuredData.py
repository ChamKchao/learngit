from os import set_inheritable
import pandas as pd
import numpy as np
import time
import datetime
import json
import modis


def saveJsonToFile(data, filename):
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)


def saveDataFrameToFile(data, filename):
    savedData = json.loads(data.to_json(orient='split'))
    with open(filename, 'w') as outfile:
        json.dump(savedData, outfile)


observations = pd.read_excel('download/observations.xlsx', sheet_name='data',
                             header=0, index_col=None, nrows=576, usecols=[1, 4])
sites = pd.read_excel('download/sites.xlsx', sheet_name='site',
                      header=0, index_col=0, nrows=576, usecols=[1, 2, 3])

product = 'MOD09A1'
kmAboveBelow = 1
kmLeftRight = 1
defaultLat = 31.2580
defaultLon = 120.2051
startDate = 'A2003001'
endDate = 'A2003001'
qualityBand = 'sur_refl_qc_500m'
stateBand = 'sur_refl_state_500m'


reflectanceBands = ['sur_refl_b0' + str(n) for n in range(1, 8)]

scale = 1.0     # placeholder value
scaleSet = set()

for band in reflectanceBands:
    modisResponse = modis.getSubset(product, band, defaultLat, defaultLon,
                                    startDate, endDate, kmAboveBelow, kmLeftRight)
    scale = float(modisResponse['scale'])
    print(f'Scale value for band {band} is {scale}')
    scaleSet.add(scale)
if len(scaleSet) > 1:
    raise Exception('scale value is not unique across all reflectance bands.')

# Assume that scale is unique

dates = [
    'A2001017',
    'A2001049',
    'A2001073',
    'A2001105',
    'A2001137',
    'A2001161',
    'A2001193',
    'A2001225',
    'A2001257',
    'A2001289',
    'A2001321',
    'A2001345',
    'A2002017',
    'A2002049',
    'A2002073',
    'A2002105',
    'A2002137',
    'A2002169',
    'A2002193',
    'A2002225',
    'A2002257',
    'A2002289',
    'A2002321',
    'A2002345',
    'A2003017',
    'A2003049',
    'A2003073',
    'A2003105',
    'A2003137',
    'A2003169',
    'A2003193',
    'A2003225',
    'A2003257',
    'A2003289',
    'A2003321',
    'A2003345',
    'A2004017',
    'A2004049',
    'A2004073',
    'A2004105',
    'A2004137',
    'A2004169',
    'A2004193',
    'A2004225',
    'A2004257',
    'A2004289',
    'A2004321',
    'A2004345',
    'A2005017',
    'A2005049',
    'A2005073',
    'A2005105',
    'A2005137',
    'A2005169',
    'A2005193',
    'A2005225',
    'A2005257',
    'A2005289',
    'A2005321',
    'A2005345',
    'A2006017',
    'A2006049',
    'A2006073',
    'A2006105',
    'A2006137',
    'A2006169',
    'A2006193',
    'A2006225',
    'A2006257',
    'A2006289',
    'A2006321',
    'A2006345'
]

numDates = len(dates)
# Convert to real dates
indexDates = [(datetime.datetime(int(date[1:5]), 1, 1) +
               datetime.timedelta(int(date[5:]))).strftime('%Y-%m-%d') for date in dates]

columnBands = ['band' + str(n) for n in range(1, 8)]

selectedSites = ['THL00', 'THL01', 'THL03',
                 'THL04', 'THL05', 'THL06', 'THL07', 'THL08']

numSites = len(selectedSites)

locations = [[sites.loc[id]['lat'], sites.loc[id]['lon']
              ]
             for id in selectedSites]

selectedObservationMask = observations['Site'].isin(selectedSites)
selectedObservations = observations[selectedObservationMask]['Measurement']

startTime = time.time()
goodQuality = 1073741824
fillValue = -28672

reflectanceData = pd.DataFrame(columns=['date'] + columnBands)

siteCounter = 0
for lat, lon in locations:
    siteCounter += 1
    dateCounter = 0
    reflectance = []
    # Iterate through the list of dates and submit subset requests for each date:
    for dt in dates:
        bandData = []
        dateCounter += 1
        # Iterate through the list of bands
        print(f'({lat}, {lon}) - {dt}: process site {siteCounter} of {numSites} / date {dateCounter} of {numDates}')
        modisResponse = modis.getSubset(product, lat, lon,
                                        dt, dt, kmAboveBelow, kmLeftRight)
        qualityFlags = list(filter(
            lambda subset: subset['band'] == 'sur_refl_qc_500m', modisResponse['subset']))[0]['data']
        qualityFlags = pd.Series(qualityFlags)
        for band in reflectanceBands:
            rawData = list(filter(
                lambda subset: subset['band'] == band, modisResponse['subset']))[0]['data']
            rawData = pd.Series(rawData)
            rawData = rawData[qualityFlags == goodQuality]
            if(len(rawData) > 0):
                average = rawData.mean() * scale
                bandData.append(average)
            else:
                # will use as flag to drop "bad" row
                bandData.append(fillValue)
        reflectance.append(bandData)
    reflectance = pd.DataFrame(np.column_stack(
        [indexDates, reflectance]), columns=['date'] + columnBands)

    reflectanceData = reflectanceData.append(reflectance)
    reflectanceData.reset_index(inplace=True, drop=True)
    elapsedTime = time.time() - startTime
    print(time.strftime("%H:%M:%S", time.gmtime(elapsedTime)))

print(reflectanceData)
saveDataFrameToFile(reflectanceData, 'reflectanceData.json')

forProcessingData = pd.DataFrame(np.column_stack([reflectanceData.values, selectedObservations.values]),
                                 columns=['date'] + columnBands + ['measured_TP'])
# define mask to filter out (drop) row with fillValue = -28672
mask = (reflectanceData != fillValue).all(axis=1)
# filter out row with fillValue
forProcessingData = forProcessingData[mask]
forProcessingData.to_csv('working.csv')
saveDataFrameToFile(forProcessingData, 'working.json')
