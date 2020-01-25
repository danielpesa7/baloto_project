#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    File name: data_base.py
    Author: Daniel G Perico Sánchez
    Date created: 24/01/2020
    Date last modified: 24/01/2020
    Python Version: 3.8.1
'''

#Import needed libraries
from sqlalchemy import create_engine
import pandas as pd

#Database credentials
db_string = 'postgres://daniel_sql:here&now777@localhost/baloto'
db = create_engine(db_string)

#Query to create the new table if doesn't exist already.
create_table_query = '''CREATE TABLE IF NOT EXISTS baloto_results(numero_sorteo INT PRIMARY KEY,fecha_sorteo DATE, b_1 CHAR(2),b_2 CHAR(2),b_3 CHAR(2),
										 b_4 CHAR(2),b_5 CHAR(2),b_s CHAR(2),r_1 CHAR(2),r_2 CHAR(2),r_3 CHAR(2),r_4 CHAR(2),
										 r_5 CHAR (2), r_s CHAR(2), gano_baloto BOOLEAN, gano_revancha BOOLEAN)'''

#Creates a new table if not exists already.
db.execute(create_table_query)

def clean_csv(file):
    '''
    This function takes a csv file and return a dataframe with some needed transformation on some of the columns. 
    It:
        *Formats the date column to a pandas datetimestamp.
        *Formats the "gano_baloto" and "gano_revancha" columns to a boolean data type.
    '''
    data_df = pd.read_csv(file)
    data_df.drop_duplicates(subset = 'numero_sorteo',inplace = True)
    data_df['fecha_sorteo'] = pd.to_datetime(data_df['fecha_sorteo'], format = '%d/%m/%y')
    data_df['fecha_sorteo'] = data_df['fecha_sorteo'].apply(lambda x : str(x.month) + '/' + str(x.day) + '/' + str(x.year))
    data_df['gano_baloto'] = data_df['gano_baloto'].astype('bool')
    data_df['gano_revancha'] = data_df['gano_revancha'].astype('bool')
    return data_df

def need_update():
    '''
    This function compares the database length with the actual data frame length and determines if there is some needed update to the database.
    Returns True if the database needs an update.
    Returns False if the database doesn't need an update.
    '''
    compare_lenght = '''SELECT COUNT (numero_sorteo) FROM baloto_results'''
    global db_lenght
    db_lenght = db.execute(compare_lenght)
    db_lenght = [i[0] for i in db_lenght]
    data_df = clean_csv('baloto_results.csv')
    if  db_lenght[0] == data_df.shape[0]:
        return False
    else:
        return True

#If the database needs an update inserts the rows that are missing in the database.
if need_update():
    data_df = clean_csv('baloto_results.csv')
    difference = data_df.shape[0] - db_lenght[0]
    if difference == 1:
        print('The Database has {} missing records.'.format(difference))
        to_update = data_df.loc[0:0,:]
        print('Inserting: ')
        print(to_update)
        to_update.to_sql('baloto_results', con = db, index = False, if_exists = 'append')
        print('The update has been succesfull')
    else:
        print('The Database has {} missing records.'.format(difference))
        to_update = data_df.loc[0:difference,:]
        print('Inserting: ')
        print(to_update)
        to_update.to_sql('baloto_results', con = db, index = False, if_exists = 'append')
        print('The update has been succesfull')
else:
    print('The Database is up to date, nothing to do.')
