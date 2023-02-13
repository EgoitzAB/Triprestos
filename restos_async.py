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

cookies = {
    'TADCID': 'NBhu4oNS3xCkI9trABQCXdElnkGETRW-Svh01l3nWnNoC8IxhwO6irNdZoSoTh-lPPVIa10SityJwGyQYLXYNoY7b5ZavizVIxs',
    'TAUnique': '%1%enc%3A5yOE8aPgi9NB3fKM3sTaWqkK%2Fet2DEnbsRARHfmCuuc2jHwltRJPGQ%3D%3D',
    'TASSK': 'enc%3AAECgI8PSQJs9iND5PgYkYymvQlShc0dIfNTfUmzojkWTYXMk5RrrlxJ81oJOgtgchSYtneriVohf8L%2FeOwqncipB8BlHKSgC4itfgBaZDyEgIK%2FUJTFceAVFbl4OyN%2Br4A%3D%3D',
    'PAC': 'AKdZak-Cf7or72qdcgybl5A5y85I7nMtvCq26nELli48XsrxiePLc_A2dvPWbqV24Zn0SM_Qa2N7ZnJTtzyivqy_LJUIRoql8m70pJmvIQxS_EfPTV2VZQr8zVwqsLSJb5ZJRJ94wxlYmSCnv_8hbn4gvb3Bf8rm9B8B6bneo6kM',
    'PMC': 'V2*MS.2*MD.20230131*LD.20230207',
    'TATravelInfo': 'V2*A.2*MG.-1*HP.2*FL.3*RS.1',
    'TAUD': 'LA-1675192623282-1*RDD-1-2023_01_31*ARC-160187284*LG-617873566-2.1.F.*LD-617873567-.....',
    'datadome': '7frqzavIEHRCDTtp2PcpUasoKAHdf_yRTz29zr8nyKVxh5UF5Or0RCc4aJ7zFlozamTsxbBR6i4~qkaoAo5HxH35G9ZE_-66SlXStGhjhRjvHjUvPXgWj_h6McuXNRzD',
    '_abck': 'EE956BD611E3EA00C406D426CF999771~-1~YAAQXdMRApBwKB+GAQAA8e8WLgnFlHOSJC6Js77rqMmrbxrVEbwhdIUDXytJR4g7qj88bChbi7W0+NIdYEVs1KOQ7kclr7IHFzY1TLVAguhrqb9X4SNmQgQdDuZIXWpboRUK3X8Hy7fZgcM4O+199pjVFAE/7lIlrIctFD/rNwdDdI/J5q55kWVDcuPFkO2yfMxdAvGW7wEWieVKVofBjYs+jIHO4lh+OfzldEb6QMy3RYpjYZooXEqF5va+P6gT6nVgvzxd6QCDyGnkYd0LXX2qimQAgeEtBdv7azedro3jJzcx2ZXkMAG+JA9fPV+Kj5mccfVw4uA6jbSnd2oi5h2WsqJcYUoTe4F2Z3LXmLqbl4hGuGSg4nTIlQ8NvZz1Mh/C18AQvWOLTr+IHX/Dcq3qgnHw/EIDwQbYn9kd~-1~-1~-1',
    'OptanonConsent': 'isGpcEnabled=1&datestamp=Tue+Feb+07+2023+23%3A54%3A56+GMT%2B0100+(hora+est%C3%A1ndar+de+Europa+central)&version=202209.1.0&hosts=&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0%2CSTACK42%3A0&consentId=fe19a896-a1ec-4af2-9c94-3d54ca4cc06b&landingPath=NotLandingPage&isIABGlobal=false&geolocation=ES%3BMD&interactionCount=1&AwaitingReconsent=false',
    'eupubconsent-v2': 'CPmz0BgPmz0BgAcABBENC2CgAAAAAAAAACiQAAAAAAFAgBgAOgAuADZAHgARAAwgCdAFyAM4AbYA7QCBwQAQADoAVwBEADCAJ0AZwA7QCBwYAMADoAiABhAGcAO0AgcIADgA6AIgAYQBOgDOAHaAQOFAAwDCAM4AgcMACgCuAMIAzgBtgEDhwAgAHQArgCIAGEAToAzgB2gEDiAAMAwgDOAIHEgAYBEADCAQOKACAAdACuAIgAYQBOgDOAHaAQOA.YAAAAAAAAAAA',
    'TATrkConsent': 'eyJvdXQiOiJBRFYsQU5BLEZVTkNUSU9OQUwsU09DSUFMX01FRElBIiwiaW4iOiIifQ==',
    'TART': '%1%enc%3AQd3yjN7E2lqeLbkL%2BtNPn0kcO4B21p5X4ulaKXbRmzK6vl8KBsRprd9q1fOTvjh6CeyaC1LeImE%3D',
    '__vt': 'F0BIc1ufGddY5vczABQCwDrKuA05TCmUEEd0_4-PPCNFq7Q7E44-g3kocK6GFhX8rHbwXIHjxiSjW1kvv8AiZKPhzYcrcERdKDEWMQ2v6G8R6LoCrEaqi67VzdtvDAIYIdpoUuFtdrcZKJg6VIQJClzrEVA',
    'TASession': 'V2ID.A5B706E7B1414C51BEFC198206A0648D*SQ.5*LS.PageMoniker*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*TRA.true*LD.293740*EAU._',
    'SRT': 'TART_SYNC',
    'ServerPool': 'C',
    'TASID': 'A5B706E7B1414C51BEFC198206A0648D',
    'ak_bmsc': '13CB305B30E12C0AA232156EE34B9DA6~000000000000000000000000000000~YAAQXdMRAvhsKB+GAQAAGsMWLhL1lYSKDBFF27FE5/EAe/a4KLIuzXatIGq+lc9J2WeKz9X8jLp98QdoXdFDhnbGbm9bTlxazomLZ6zlIvHVgunuNZDC3fGalDKb4CJ4u/z6sBAt4Zo7uEWalROBUlcSrfY0uw1VN+hFjtRqmDsFLH1Hn0ocZyJbCIwvQhO6zRzEHWur+vet0m+hLABBguBITDlYeGYB4R4KlVyq86Z4+6QNlKuTTmhLTxU871YlpwK6NqTJJ3ZhhlI+Iw3xS58A23LGUKrp3L1Nt6izLn4m7XzQZ59KIdlRlud4p3kzg13tCgmaOA0NgYXmsMpINE7pfYz7dHbfE7wN9SPxSfGWheqGfjgUrud/py+M1H64knLlEtlhO6c7UekpuA==',
    'bm_sz': 'C42DB26D2CD71A6F00474A92C57B5064~YAAQXdMRAvlsKB+GAQAAGsMWLhIQpc+1yAE1o6t880JqyW6V3orNytNjAT2dnUIKOB0/es65gVdaQPOcBvnM+DiuR1jyZbcJdT1jc2Ms4eQcXXQEes4ANgKyqwE/TI8fuJbKw/mvR5BQhMY8SWcb8iy0u/VnrxpIP9SFNN87X9f3zcXWh8wF2ynyXNIQRs3YYYG/tbVqObhOXdn1rumB3NMy8I5Fk9Rihf4lUvER6aRHgm4XgktQytyB9UnrRWAtOG45NPPz/SG0s9WwEL35g041QrZJLooD/ZWOlXlCb8d5WF4aku1L1A==~3163703~3553605',
    'TAReturnTo': '%1%%2FRestaurants-g293740-South_Africa.html',
    'bm_sv': '27377C177122407E5D7262DCE240664A~YAAQXdMRAjJxKB+GAQAA3vQWLhKeMbIIzs2Cum+yeQTCQZdK9foC8ggGEUawK6GknIWSW9TqN5/onohQE6c2+4sP8eFpvlQ5PH9TxVmvsY/6Qgi0V+DM2PvXW/Eo/vamR9OV994yEDdx6haXbZ48Hw9vYjQ9DYbdNh2VnaYZa67zQgQYKQIYvYxPKhO/9FYfXWngrHsCuPw2w4CfZpGCYYYyRRVLU8AQJhh3lSI6c0v3gA/Pz1UjYlbvWmdXXuoNUWEVCuU=~1',
    'roybatty': 'TNI1625!AIJqYj%2FL0aqPn%2BSLyxQOVoabyDlaozZf0dJqT9l%2FiRL%2FYDZqYXBYtKukWi28rLEmdgYe1IRkNbkAqdy3ozXz1HlwYBc4UzFS5h0gvjW79g6Ub8L4vozQTFs%2FFxMHAfat7YvOYUYKdhPlZB7uhVTkM9tmhBNxNlm5lrj2VNDmNN3i%2C1',
    'OptanonAlertBoxClosed': '2023-02-07T22:54:56.716Z',
    'OTAdditionalConsentString': '1~',
    'EVT': 'gac.STANDARD_PAGINATION*gaa.page*gal.2*gav.0*gani.false*gass.Restaurants*gasl.293740*gads.Restaurants*gadl.293740*gapu.5037d516-ad3e-46bc-b5fd-20a7e2eb0fa7*gams.0',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.tripadvisor.com/Restaurants-g293740-South_Africa.html',
    # 'Cookie': 'TADCID=NBhu4oNS3xCkI9trABQCXdElnkGETRW-Svh01l3nWnNoC8IxhwO6irNdZoSoTh-lPPVIa10SityJwGyQYLXYNoY7b5ZavizVIxs; TAUnique=%1%enc%3A5yOE8aPgi9NB3fKM3sTaWqkK%2Fet2DEnbsRARHfmCuuc2jHwltRJPGQ%3D%3D; TASSK=enc%3AAECgI8PSQJs9iND5PgYkYymvQlShc0dIfNTfUmzojkWTYXMk5RrrlxJ81oJOgtgchSYtneriVohf8L%2FeOwqncipB8BlHKSgC4itfgBaZDyEgIK%2FUJTFceAVFbl4OyN%2Br4A%3D%3D; PAC=AKdZak-Cf7or72qdcgybl5A5y85I7nMtvCq26nELli48XsrxiePLc_A2dvPWbqV24Zn0SM_Qa2N7ZnJTtzyivqy_LJUIRoql8m70pJmvIQxS_EfPTV2VZQr8zVwqsLSJb5ZJRJ94wxlYmSCnv_8hbn4gvb3Bf8rm9B8B6bneo6kM; PMC=V2*MS.2*MD.20230131*LD.20230207; TATravelInfo=V2*A.2*MG.-1*HP.2*FL.3*RS.1; TAUD=LA-1675192623282-1*RDD-1-2023_01_31*ARC-160187284*LG-617873566-2.1.F.*LD-617873567-.....; datadome=7frqzavIEHRCDTtp2PcpUasoKAHdf_yRTz29zr8nyKVxh5UF5Or0RCc4aJ7zFlozamTsxbBR6i4~qkaoAo5HxH35G9ZE_-66SlXStGhjhRjvHjUvPXgWj_h6McuXNRzD; _abck=EE956BD611E3EA00C406D426CF999771~-1~YAAQXdMRApBwKB+GAQAA8e8WLgnFlHOSJC6Js77rqMmrbxrVEbwhdIUDXytJR4g7qj88bChbi7W0+NIdYEVs1KOQ7kclr7IHFzY1TLVAguhrqb9X4SNmQgQdDuZIXWpboRUK3X8Hy7fZgcM4O+199pjVFAE/7lIlrIctFD/rNwdDdI/J5q55kWVDcuPFkO2yfMxdAvGW7wEWieVKVofBjYs+jIHO4lh+OfzldEb6QMy3RYpjYZooXEqF5va+P6gT6nVgvzxd6QCDyGnkYd0LXX2qimQAgeEtBdv7azedro3jJzcx2ZXkMAG+JA9fPV+Kj5mccfVw4uA6jbSnd2oi5h2WsqJcYUoTe4F2Z3LXmLqbl4hGuGSg4nTIlQ8NvZz1Mh/C18AQvWOLTr+IHX/Dcq3qgnHw/EIDwQbYn9kd~-1~-1~-1; OptanonConsent=isGpcEnabled=1&datestamp=Tue+Feb+07+2023+23%3A54%3A56+GMT%2B0100+(hora+est%C3%A1ndar+de+Europa+central)&version=202209.1.0&hosts=&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0%2CSTACK42%3A0&consentId=fe19a896-a1ec-4af2-9c94-3d54ca4cc06b&landingPath=NotLandingPage&isIABGlobal=false&geolocation=ES%3BMD&interactionCount=1&AwaitingReconsent=false; eupubconsent-v2=CPmz0BgPmz0BgAcABBENC2CgAAAAAAAAACiQAAAAAAFAgBgAOgAuADZAHgARAAwgCdAFyAM4AbYA7QCBwQAQADoAVwBEADCAJ0AZwA7QCBwYAMADoAiABhAGcAO0AgcIADgA6AIgAYQBOgDOAHaAQOFAAwDCAM4AgcMACgCuAMIAzgBtgEDhwAgAHQArgCIAGEAToAzgB2gEDiAAMAwgDOAIHEgAYBEADCAQOKACAAdACuAIgAYQBOgDOAHaAQOA.YAAAAAAAAAAA; TATrkConsent=eyJvdXQiOiJBRFYsQU5BLEZVTkNUSU9OQUwsU09DSUFMX01FRElBIiwiaW4iOiIifQ==; TART=%1%enc%3AQd3yjN7E2lqeLbkL%2BtNPn0kcO4B21p5X4ulaKXbRmzK6vl8KBsRprd9q1fOTvjh6CeyaC1LeImE%3D; __vt=F0BIc1ufGddY5vczABQCwDrKuA05TCmUEEd0_4-PPCNFq7Q7E44-g3kocK6GFhX8rHbwXIHjxiSjW1kvv8AiZKPhzYcrcERdKDEWMQ2v6G8R6LoCrEaqi67VzdtvDAIYIdpoUuFtdrcZKJg6VIQJClzrEVA; TASession=V2ID.A5B706E7B1414C51BEFC198206A0648D*SQ.5*LS.PageMoniker*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*TRA.true*LD.293740*EAU._; SRT=TART_SYNC; ServerPool=C; TASID=A5B706E7B1414C51BEFC198206A0648D; ak_bmsc=13CB305B30E12C0AA232156EE34B9DA6~000000000000000000000000000000~YAAQXdMRAvhsKB+GAQAAGsMWLhL1lYSKDBFF27FE5/EAe/a4KLIuzXatIGq+lc9J2WeKz9X8jLp98QdoXdFDhnbGbm9bTlxazomLZ6zlIvHVgunuNZDC3fGalDKb4CJ4u/z6sBAt4Zo7uEWalROBUlcSrfY0uw1VN+hFjtRqmDsFLH1Hn0ocZyJbCIwvQhO6zRzEHWur+vet0m+hLABBguBITDlYeGYB4R4KlVyq86Z4+6QNlKuTTmhLTxU871YlpwK6NqTJJ3ZhhlI+Iw3xS58A23LGUKrp3L1Nt6izLn4m7XzQZ59KIdlRlud4p3kzg13tCgmaOA0NgYXmsMpINE7pfYz7dHbfE7wN9SPxSfGWheqGfjgUrud/py+M1H64knLlEtlhO6c7UekpuA==; bm_sz=C42DB26D2CD71A6F00474A92C57B5064~YAAQXdMRAvlsKB+GAQAAGsMWLhIQpc+1yAE1o6t880JqyW6V3orNytNjAT2dnUIKOB0/es65gVdaQPOcBvnM+DiuR1jyZbcJdT1jc2Ms4eQcXXQEes4ANgKyqwE/TI8fuJbKw/mvR5BQhMY8SWcb8iy0u/VnrxpIP9SFNN87X9f3zcXWh8wF2ynyXNIQRs3YYYG/tbVqObhOXdn1rumB3NMy8I5Fk9Rihf4lUvER6aRHgm4XgktQytyB9UnrRWAtOG45NPPz/SG0s9WwEL35g041QrZJLooD/ZWOlXlCb8d5WF4aku1L1A==~3163703~3553605; TAReturnTo=%1%%2FRestaurants-g293740-South_Africa.html; bm_sv=27377C177122407E5D7262DCE240664A~YAAQXdMRAjJxKB+GAQAA3vQWLhKeMbIIzs2Cum+yeQTCQZdK9foC8ggGEUawK6GknIWSW9TqN5/onohQE6c2+4sP8eFpvlQ5PH9TxVmvsY/6Qgi0V+DM2PvXW/Eo/vamR9OV994yEDdx6haXbZ48Hw9vYjQ9DYbdNh2VnaYZa67zQgQYKQIYvYxPKhO/9FYfXWngrHsCuPw2w4CfZpGCYYYyRRVLU8AQJhh3lSI6c0v3gA/Pz1UjYlbvWmdXXuoNUWEVCuU=~1; roybatty=TNI1625!AIJqYj%2FL0aqPn%2BSLyxQOVoabyDlaozZf0dJqT9l%2FiRL%2FYDZqYXBYtKukWi28rLEmdgYe1IRkNbkAqdy3ozXz1HlwYBc4UzFS5h0gvjW79g6Ub8L4vozQTFs%2FFxMHAfat7YvOYUYKdhPlZB7uhVTkM9tmhBNxNlm5lrj2VNDmNN3i%2C1; OptanonAlertBoxClosed=2023-02-07T22:54:56.716Z; OTAdditionalConsentString=1~; EVT=gac.STANDARD_PAGINATION*gaa.page*gal.2*gav.0*gani.false*gass.Restaurants*gasl.293740*gads.Restaurants*gadl.293740*gapu.5037d516-ad3e-46bc-b5fd-20a7e2eb0fa7*gams.0',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
    # Requests doesn't support trailers
    # 'TE': 'trailers',
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