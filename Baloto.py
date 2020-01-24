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
from bs4 import BeautifulSoup
import pandas as pd
import requests
import urllib.request
import time
import numpy as np

def scrape_baloto():
    '''
    This function performs the whole web scraping from the Baloto official webpage to obtain the historical results from the Baloto draw since 2001 to 2020.
    Returns a pandas dataframe with all the Baloto historical data.
    '''
    years = list(range(2001,2021))
    years = [str(i) for i in years]

    base_url = 'https://www.baloto.com/historicoano/'

    df = pd.DataFrame(columns = ['numero_sorteo', 'fecha_sorteo', 'b_1', 'b_2', 'b_3', 'b_4', 'b_5', 'b_s',
        'r_1', 'r_2', 'r_3', 'r_4', 'r_5', 'r_s', 'gano_baloto',
        'gano_revancha'])

    for year in years:

        url = base_url + year
        print('Conectándose a los resultados de Baloto del año: {}'.format(year))
        response = requests.get(url)

        soup = BeautifulSoup(response.text, 'html.parser')

        class_list = soup.find_all(attrs = {'class' : 'col-lg-2 col-sm-2 vertcal'})

        class_list = [i.text for i in class_list]

        class_list = [i.replace('\n','') for i in class_list]
        class_list = [i.replace(' ','') for i in class_list]

        numero_sorteo = [i for i in class_list if '/' not in i]
        fecha_sorteo = [i for i in class_list if '/' in i]
        print('Extrayendo resultados de Baloto del año: {}'.format(year))
        lista_clase_balotas_b = soup.find_all(attrs = {'class':'col-lg-3 col-sm-3 vertcal'})
        lista_clase_balotas_r = soup.find_all(attrs = {'class':'col-lg-3 col-sm-3 boder-right vertcal'})

        lista_clase_balotas_b = [i.text for i in lista_clase_balotas_b]
        lista_clase_balotas_r = [i.text for i in lista_clase_balotas_r]

        lista_clase_balotas_b = [i.strip() for i in lista_clase_balotas_b]
        lista_clase_balotas_r = [i.strip() for i in lista_clase_balotas_r]

        listas_balotas_b = [i.split('\n') for i in lista_clase_balotas_b]
        listas_balotas_r = [i.split('\n') for i in lista_clase_balotas_r]

        cayo_tag = soup.find_all(attrs = {'class' : 'col-lg-1 col-sm-1 vertcal'})

        lista_cayo = [1 if i.find('img') != None else 0 for i in cayo_tag ]

        enumerate_cayo = list(enumerate(lista_cayo))

        gano_b = [j for i,j in enumerate(lista_cayo) if i%2 == 0]
        gano_r = [j for i,j in enumerate(lista_cayo) if i%2 == 1]

        b_1 = [i[0] for i in listas_balotas_b]
        b_2 = [i[1] for i in listas_balotas_b]
        b_3 = [i[2] for i in listas_balotas_b]
        b_4 = [i[3] for i in listas_balotas_b]
        b_5 = [i[4] for i in listas_balotas_b]
        b_s = [i[5] for i in listas_balotas_b]

        r_1 = [i[0] for i in listas_balotas_r]
        r_2 = [i[1] for i in listas_balotas_r]
        r_3 = [i[2] for i in listas_balotas_r]
        r_4 = [i[3] for i in listas_balotas_r]
        r_5 = [i[4] for i in listas_balotas_r]
        r_s = [i[5] for i in listas_balotas_r]

        print('Guardando los resultados en un dataframe')

        df_temp = pd.DataFrame({'numero_sorteo' : numero_sorteo, 'fecha_sorteo' : fecha_sorteo, 'b_1' : b_1, 'b_2' : b_2, 'b_3' : b_3,
                        'b_4' : b_4, 'b_5' : b_5, 'b_s' : b_s, 'r_1' : r_1, 'r_2' : r_2, 'r_3' : r_3, 'r_4' : r_4, 
                        'r_5' : r_5, 'r_s' : r_s, 'gano_baloto' : gano_b, 'gano_revancha' : gano_r})

        df = df.append(df_temp)
    df['numero_sorteo'] = df['numero_sorteo'].astype('int64')
    df.sort_values(by = 'numero_sorteo', ascending = False, inplace = True)
    return df


def update_baloto():
    '''
    This function performs web scraping to obtain the latest Baloto results (Year : 2020).
    It compares the dataframe length from the initial web scraping to the most recent results and updates the csv file if necessary.
    Returns a pandas dataframe with the new Baloto results.
'''
    url = 'https://www.baloto.com/historicoano/2020'
    actual_df = pd.read_csv('baloto_results.csv')
    ultimo_numero_sorteo = actual_df['numero_sorteo'].max()

    df = pd.DataFrame(columns = ['numero_sorteo', 'fecha_sorteo', 'b_1', 'b_2', 'b_3', 'b_4', 'b_5', 'b_s',
        'r_1', 'r_2', 'r_3', 'r_4', 'r_5', 'r_s', 'gano_baloto',
        'gano_revancha'])

    print('Conectándose a la página oficial de Baloto')
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    class_list = soup.find_all(attrs = {'class' : 'col-lg-2 col-sm-2 vertcal'})

    class_list = [i.text for i in class_list]

    class_list = [i.replace('\n','') for i in class_list]
    class_list = [i.replace(' ','') for i in class_list]

    numero_sorteo = [i for i in class_list if '/' not in i]
    fecha_sorteo = [i for i in class_list if '/' in i]
    print('Extrayendo los resultados históricos de Baloto')
    lista_clase_balotas_b = soup.find_all(attrs = {'class':'col-lg-3 col-sm-3 vertcal'})
    lista_clase_balotas_r = soup.find_all(attrs = {'class':'col-lg-3 col-sm-3 boder-right vertcal'})

    lista_clase_balotas_b = [i.text for i in lista_clase_balotas_b]
    lista_clase_balotas_r = [i.text for i in lista_clase_balotas_r]

    lista_clase_balotas_b = [i.strip() for i in lista_clase_balotas_b]
    lista_clase_balotas_r = [i.strip() for i in lista_clase_balotas_r]

    listas_balotas_b = [i.split('\n') for i in lista_clase_balotas_b]
    listas_balotas_r = [i.split('\n') for i in lista_clase_balotas_r]

    cayo_tag = soup.find_all(attrs = {'class' : 'col-lg-1 col-sm-1 vertcal'})

    lista_cayo = [1 if i.find('img') != None else 0 for i in cayo_tag ]

    enumerate_cayo = list(enumerate(lista_cayo))

    gano_b = [j for i,j in enumerate(lista_cayo) if i%2 == 0]
    gano_r = [j for i,j in enumerate(lista_cayo) if i%2 == 1]

    b_1 = [i[0] for i in listas_balotas_b]
    b_2 = [i[1] for i in listas_balotas_b]
    b_3 = [i[2] for i in listas_balotas_b]
    b_4 = [i[3] for i in listas_balotas_b]
    b_5 = [i[4] for i in listas_balotas_b]
    b_s = [i[5] for i in listas_balotas_b]

    r_1 = [i[0] for i in listas_balotas_r]
    r_2 = [i[1] for i in listas_balotas_r]
    r_3 = [i[2] for i in listas_balotas_r]
    r_4 = [i[3] for i in listas_balotas_r]
    r_5 = [i[4] for i in listas_balotas_r]
    r_s = [i[5] for i in listas_balotas_r]

    print('Guardando los resultados en un dataframe')

    df_temp = pd.DataFrame({'numero_sorteo' : numero_sorteo, 'fecha_sorteo' : fecha_sorteo, 'b_1' : b_1, 'b_2' : b_2, 'b_3' : b_3,
                    'b_4' : b_4, 'b_5' : b_5, 'b_s' : b_s, 'r_1' : r_1, 'r_2' : r_2, 'r_3' : r_3, 'r_4' : r_4, 
                    'r_5' : r_5, 'r_s' : r_s, 'gano_baloto' : gano_b, 'gano_revancha' : gano_r})

    ultimo_numero_sorteo_bd = int(max(actual_df['numero_sorteo']))
    ultimo_numero_sorteo_baloto = int(max(df_temp['numero_sorteo']))
    cantidad_insertar = ultimo_numero_sorteo_baloto - ultimo_numero_sorteo_bd

    if ultimo_numero_sorteo_bd == ultimo_numero_sorteo_baloto:
        print('El archivo csv está actualizado')
    else:
        print('El archivo csv no está actualizado')
        print('El último número de sorteo actualmente registrado es: {}'.format(numero_sorteo[0]))
        print('El último número en la página de Baloto es: {} '.format(ultimo_numero_sorteo))
        print('Se insertarán {} nuevos registros en el archivo csv'.format(cantidad_insertar))
        df_add = df_temp.loc[0:cantidad_insertar-1,:]
        print(df_add)
        df = actual_df.append(df_add)
        df['numero_sorteo'] = df['numero_sorteo'].astype('int64')
        df.sort_values(by = 'numero_sorteo', inplace = True, ascending = False)
    return df

if __name__ == '__main__':
    #Run this if you want to scrape all the historical Baloto results.
    #df = scrape_baloto()
    #print('Guardando los resultados de Baloto en un archivo csv')
    #df.to_csv('baloto_results.csv', index = False)
    #----------------------\\-----------------------
    #Run this if you want to scrape the newest Baloto results and you already have a initial csv file.
    dff = update_baloto()
    dff.to_csv('baloto_results.csv',index = False)