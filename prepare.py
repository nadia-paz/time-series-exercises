import pandas as pd
from datetime import timedelta, datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from env import user, password, host
import os
from acquire import wrangle_store_data


########### SALES DATABASE ###########

def acquire_store_data():
    '''
    Checks for a local cache of tsa_store_data.csv and if not present will 
    run the get_store_data() function which acquires data from Codeup's mysql server
    '''
    filename = 'tsa_store_data.csv'
    
    if os.path.isfile(filename):
        df = pd.read_csv(filename)
    else:
        query = '''
        SELECT *
        FROM items
        JOIN sales USING(item_id)
        JOIN stores USING(store_id) 
        '''
        
        df = pd.read_sql(query, get_db_url('tsa_item_demand'))
        
        df.to_csv(filename, index=False)
        
    return df

def wrangle_store_data(df):
    '''
    the function accepts a store data frame as a parameter
    sets a sale date as an index
    creates columns: month, day of the week, total sales
    returns an updated data frame
    '''
    # change sale_date from object to date type
    df.sale_date = pd.to_datetime(df.sale_date)
    
    # set sale_date as an index
    df = df.set_index("sale_date").sort_index()
    
    # create a column which holds a month name
    df['month'] = df.index.month_name()

    # create a column that holds a day of the week name
    df['day_of_week'] = df.index.day_name()
    
    df['sales_total'] = df.sale_amount * df.item_price
    
    return df

######## Germany Electricity Data #################


def get_energy_data():
    '''
    the function pulls the ~Germany energy concumption data set
    cleans the data (low case column names, date as an index, column for the month name, fills null values)
    returns a clean data frame
    '''
    # save a data location to an url
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    # save the data into a data frame
    df = pd.read_csv(url)
    # make column names low case
    df.columns = df.columns.str.lower()
    # rename wind+solar into wind_and_solar
    df.rename(columns = {'wind+solar':'wind_and_solar'}, inplace=True)
    # convert date columns into datetime data type
    df.date = pd.to_datetime(df.date)
    # set a date as an index
    df = df.set_index('date').sort_index()
    # create a column that holds names of the months
    df['month'] = df.index.month_name()
    
    # fill missing values. 
    # wind and solr with 0's
    # wind_and_solar with wind+solar
    df.wind = df.wind.fillna(0)
    df.solar = df.solar.fillna(0)
    df.wind_and_solar = df.wind_and_solar.fillna(df.wind+df.solar)
    
    return df