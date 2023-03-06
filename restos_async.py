#!/usr/bin/python3

import os
import requests
import pandas as pd
import numpy as np

import asyncio
import aiohttp
from bs4 import BeautifulSoup

from aiohttp import ClientSession

URL_1 = [f"https://www.tripadvisor.com/Restaurants-g293740-oa{n}-South_Africa.html" for n in range(20, 740, 20)]

cookies = { #get and insert the cookies
}

headers = { #get and insert the headers
}

formated_links = set()
formated_links_2 = set()
final_data = set()


def get_soup(url):
    """ Get the soup object for urls """
    r = requests.get(url=url, cookies=cookies, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup

def get_main_cities_restos(main_url):
    """ Get restos links and append to list """
    soup = get_soup(main_url)
    try:        
        if soup.find('ul', {'class': 'geoList'}) != (None or ''):
            link_find = soup.find('ul', {'class': 'geoList'})
            link_finds = link_find.find_all('li')
            for ad_link in link_finds:
                ad_link = ad_link.a['href']
                if ad_link.startswith('/Resta') and not None:
                    f_link = "https://www.tripadvisor.com" + ad_link
                    print(f_link)
                    formated_links.add(f_link)
                else:
                    continue
        else:
            pass
    except:
        pass

def get_city_restos_data(link):
    """ Get cities restos links and append to list """
    soup = get_soup(link)
    description = soup.find('div', id="component_2")
    link_find = description.find_all('a', href=True)
    for ad_link in link_find:
        adl = ad_link['href']
        if adl.startswith('/Resta') and not None and not adl.endswith('REVIEWS'):
            f_link = "https://www.tripadvisor.com" + adl
            formated_links_2.add(f_link)
        else:
            continue

def get_each_record(response, link):
    """ Get each_resto data and return a tuple """
    soup = BeautifulSoup(response, 'lxml')
    try:
        if soup.find('div', id="taplc_details_card_0").text != '' and not None:
            description = (soup.find('div', {'class': 'VOzxM'}).text)
            if soup.find('div', {'class': 'lBkqB _T'}):
                restaurant_info = soup.find('div', {'class': 'lBkqB _T'})
                name = restaurant_info.h1.text
                info = restaurant_info.find('div', {'class': 'vQlTa H3'}).next_sibling
                telephone = info.children
                address = next(telephone).text
                number = next(telephone).text
                aditional_information = address + number
                website = str(link)
                theme = 'Restaurants'
                final_data.add((name, website, description, aditional_information, theme))
        else:
            final_data.add((np.nan, np.nan, np.nan, np.nan, np.nan))
    except KeyError as e:
        print(f"{e}")
        

async def fetch_all(url_set):
    """ Get records and add to set"""
    tasks = []
    async with ClientSession() as session:
        for url in url_set:
            try:
                task = asyncio.ensure_future(fetch_html_1(url, session))
                tasks.append(task)
                await asyncio.gather(*tasks)
            except:
                continue

async def fetch_html_1(url, session):
    """ Function to return response object from url"""
    if url != None:
        try:
            async with session.get(url) as response:
                r = await response.text()
                return get_each_record(r, url)
        except:
            pass

def fetch_async(urls):
    """ Function who coordinates the coroutines for data """
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_all(urls))
    loop.run_until_complete(future)

def make_df():
    """ Make pandas dataframe to manipulate data """
    df = pd.DataFrame(final_data,
                      columns=['Name', 'Url', 'Description', 'Aditional_information', 'Theme'])
    df = df.dropna()
    return df

def make_excel(df):
    """ Make excel file from dataframe """
    if os.path.exists('async_resto.xlsx'):
        df_source = pd.read_excel('async_resto.xlsx')
        vertical_concat = pd.concat([df_source, df], axis=0)
        vertical_concat.drop_duplicates(keep='first')
        vertical_concat.to_excel('async_resto.xlsx', index=False)
    else:
        df.to_excel('async_resto.xlsx', index=False)

if __name__=='__main__':
    for ur in URL_1:
        get_main_cities_restos(ur)
        print(len(formated_links))
        print("Primer paso")
    for link in formated_links:
        try:
            get_city_restos_data(link)
        except:
            continue
    print(len(formated_links_2))
    print("Segundo paso")
    fetch_async(formated_links_2)
    df = make_df()
    make_excel(df)
