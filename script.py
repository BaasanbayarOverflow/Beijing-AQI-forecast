# This python script's main purpose is to combine all of csv data files into one ultimate CSV
# 1. Fill the all missing data 
# 2. Resample the data
# 3. Combine all the CSVs by averaging them

import numpy as np
import pandas as pd

from glob import glob
from tqdm import tqdm
from os import mkdir, path, remove


DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def fillMissingByAverage(tm_name:str, col_name:str, series):
    array = np.array(series, dtype=np.float32)

    count = 0
    length_of_array = len(array)
    for i in range(length_of_array):
        if (np.isnan(array[i])):
            prev_ = i if i == 0 else array[i - 1]
            next_ = 0 if i == (length_of_array - 1) else array[i + 1]
            next_ = prev_ if np.isnan(next_) else next_
            array[i] = (prev_ + next_) / 2
            count += 1
    print(f'File name: {tm_name}   >=======<   column name: {col_name}   >=======<   filled row count: {count}') 

    series = pd.Series(array)    
    return series


def processDataFrame(df_name:str, dataframe):
    columns = ['PM2.5', 'PM10', 'SO2', 'NO2', 'CO', 'O3']
    for col in columns:
        dataframe[col] = fillMissingByAverage(df_name, col, dataframe[col])
    
    dataframe['dtime'] = pd.to_datetime(dataframe['year'].astype(str) + '-' + \
                                                dataframe['month'].astype(str) + '-' + \
                                                    dataframe['day'].astype(str) + ' ' + \
                                                        dataframe['hour'].astype(str) + ':00:00' , format=DATE_FORMAT) 
    dataframe.set_index(pd.DatetimeIndex(dataframe['dtime']), inplace=True)

    dataframe = dataframe.drop(
        columns=[
                    'No', 'year', 'month', 'day', 'hour', 
                    'PRES', 'DEWP', 'RAIN', 'wd', 
                    'WSPM', 'station', 'dtime'
                ]
    )

    columns = dataframe.columns
    for col in columns:
        dataframe[col] = dataframe[col].resample('D').mean()
    dataframe = dataframe.dropna()

    return dataframe


def main():
    folder_name = r'.\data'
    data_files = glob(folder_name + r'\*.*')[1:]

    dir_name = r'.\data_transformed'
    if (not path.exists(dir_name)):
        mkdir(dir_name)

    for file in tqdm(data_files):
        file_name = file.split('_')[2]
        df = pd.read_csv(file)        
        df = processDataFrame(file_name, df)

        file_name = dir_name + f'\\{file_name}' + '.csv'
        if (path.exists(file_name)):
            print('Same file found. Now deleting...')
            remove(file_name)
        df.to_csv(file_name)
    
    data_files_t = glob(dir_name + r'\*.*')
    dataframe_temp = pd.read_csv(data_files_t[0])

    div = 0
    for file in data_files_t[1:]:
        dataframe = pd.read_csv(file)

        dataframe_temp['PM2.5'] += dataframe['PM2.5']
        dataframe_temp['PM10'] += dataframe['PM10']
        dataframe_temp['SO2'] += dataframe['SO2']
        dataframe_temp['NO2'] += dataframe['NO2']
        dataframe_temp['CO'] += dataframe['CO']
        dataframe_temp['O3'] += dataframe['O3']
        div += 1

    dataframe_temp['PM2.5'] /= div
    dataframe_temp['PM10'] /= div
    dataframe_temp['SO2'] /= div
    dataframe_temp['NO2'] /= div
    dataframe_temp['CO'] /= div
    dataframe_temp['O3'] /= div
    
    if (path.exists(r'./ultimate.csv')):
        print('Deleting...')

    try:
        dataframe_temp.to_csv(r'./ultimate.csv', index=False)
        print('Successfully saved the Ultimate file!')
    except:
        print('Could not save the Ultimate File!')


if __name__ == '__main__':
    main()


"""
from datetime import datetime

def toDatetime(dataframe):
    date_format_str = '%Y/%m/%d %H:%M:%S'
    data = str(int(dataframe[0])) + '/' + str(int(dataframe[1])) + '/' + \
                str(int(dataframe[2])) + ' ' + str(int(dataframe[3])) + ':00:00'
    return datetime.strptime(data, date_format_str)
    
dataframe_copy['date'] = dataframe_copy.apply(toDatetime, axis=1)


fig.add_traces(
    go.Line(
        x = dataframe['PM2.5'].index, 
        y = dataframe['PM2.5'],
        name = 'PM2.5',
        line = dict(color='#0E1326'),
       )
)

fig.add_traces(
    go.Line(
        x = prediction_list[0].pd_dataframe().index, 
        y = prediction_list[0].pd_dataframe()['PM2.5'],
        name = 'PM2.5',
        line = dict(color='#0E1326'),
       )
)

fig.add_traces(
    go.Line(
        x = dataframe['PM10'].index,
        y = dataframe['PM10'],
        name = 'PM10',
        line = dict(color='#005AA9'),
    )
)



"""