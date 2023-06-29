import beautifulsoup4 as soup
import requests
import re
import pandas as pd
from datetime import date

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Exporting dataframe to the google sheet
scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file('spheric-verve-377117-9d16b7e5bc6b.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_key('1zGVtWuySj8QWFZV19UHchAafwlH4dbyeTd4P4WgzL4Y')
# select a work sheet from its name
worksheet1 = gs.worksheet('Sheet1')
worksheet2 = gs.worksheet('Sheet2')
worksheet3 = gs.worksheet('Sheet3')


# give acess to trend-747@spheric-verve-377117.iam.gserviceaccount.com

def news_scraping_diepresse():
    diepresse_url = 'https://www.diepresse.com/'
    html = requests.get(diepresse_url)
    bsobj = soup(html.content, 'lxml')
    links = []

    for link in bsobj.find_all('a', attrs={'href': re.compile("^https://www.diepresse.com/6")}):
        link = link.get('href')
        links.append(link)

    lst = []
    lst2 = []
    lst3 = []
    for link in links:
        page = requests.get(link)
        b = soup(page.content)

        for i in b.findAll('div', {'class': 'article__body js-fontsizer'}):
            lst.append(i.text.strip())

        for j in b.findAll('h1', {'class': 'article__title'}):
            lst2.append(j.text.strip())

        for k in b.findAll('div', {'class': 'meta__date'}):
            lst3.append(k.text.strip())

    # print(len(lst))
    # print(len(lst2))
    # print(len(lst3))
    df = pd.DataFrame(
        {'Date': lst3,
         'Title': lst2,
         'News': lst,
         'Link': links
         })
    df["Date"] = df["Date"].apply(lambda x: x.replace("um ", ""))
    df["Date"] = df["Date"].apply(lambda x: x.replace(".", "-"))
    df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)
    df.sort_values(by='Date', ascending=False, inplace=True)

    worksheet1.clear()
    set_with_dataframe(worksheet=worksheet1, dataframe=df.head(20), include_index=False, include_column_header=True,
                       resize=True)
    return df.head(20)


def news_scraping_kurier():
    kurier_url = 'https://kurier.at/neues'
    html = requests.get(kurier_url)
    bsobj_2 = soup(html.content, 'html.parser')

    import re
    links = []
    for link in bsobj_2.find_all('a', {'target': '_self', 'href': re.compile('\d$')}):
        # display the actual urls
        link = link.get('href')
        links.append(link)

    lst = []
    for link in links:
        try:
            if link[0:3].startswith("po") or link[0:3].startswith("/ch") or link[0:4].startswith("wis") or link[
                                                                                                           0:3].startswith(
                    "/sp") or link[0:4].startswith("/wir") or link[0:3].startswith("/li") or link[0:3].startswith(
                    "/ku") or link[0:3].startswith("/st") or link[0:3].startswith("/me"):
                lst.append("https://kurier.at" + link)
        except IndexError:
            pass

    lst1 = []
    lst3 = []
    lst4 = []
    lst5 = []
    lst6 = []
    lst7 = []

    for l in lst:
        page = requests.get(l)
        b = soup(page.content)

        for k in b.findAll('div', {'class': 'article-paragraphs'}):
            lst1.append(k.text.strip())

        for m in b.findAll('h1', {'class': 'article-header-title'}):
            lst3.append(m.text.strip())

        for n in b.findAll('span', {'class': 'article-meta-date ng-star-inserted'}):
            lst4.append(n.text.strip())
            lst5.append(l)
            lst6.append(m.text.strip())
            lst7.append(k.text.strip())

    df2 = pd.DataFrame(
        {'Date': lst4,
         'Title': lst6,
         'News': lst7,
         'Link': lst5
         })

    m = df2['Date'].str.startswith('Heute')
    df2['Date'] = df2['Date'].str[:10]
    df2.loc[m, 'Date'] = date.today()

    try:
        df2['Date'] = pd.to_datetime(df2['Date'])

    except:
        pass

    df2.sort_values(by='Date', ascending=False, inplace=True)

    worksheet2.clear()

    set_with_dataframe(worksheet=worksheet2, dataframe=df2.head(20), include_index=False, include_column_header=True,
                       resize=True)
    return df2.head(20)


def news_scraping_wienerzeitung():
    wienerzeitung_url = 'https://www.wienerzeitung.at/'
    html = requests.get(wienerzeitung_url)
    bsobj_3 = soup(html.content, 'html.parser')

    links = []
    for link in bsobj_3.find_all('a', attrs={'href': re.compile("^https://www.wienerzeitung.at/nachrichten")}):
        link = link['href']
        links.append(link)

    lst1 = []
    lst2 = []
    lst3 = []
    lst4 = []

    for l in links:
        page = requests.get(l)
        b = soup(page.content)

        for m in b.findAll('h1', {'class': 'article-title d-inline'}):
            lst1.append(m.text.strip())

        for n in b.findAll('span', {'class': 'article-subtitle'}):
            lst2.append(n.text.strip())

        for k in b.findAll('span', {'class': 'article-published'}):
            lst3.append(k.text.strip())

        for r in b.findAll('link', {'rel': 'amphtml'}):
            r = r['href']
            lst4.append(r)

    df3 = pd.DataFrame(
        {'Date': lst3,
         'Title': lst1,
         'News': lst2,
         'Link': lst4
         })

    df3["Date"] = df3["Date"].apply(lambda x: x.replace("vom ", ""))
    df3["Date"] = df3["Date"].apply(lambda x: x.replace("Uhr", ""))

    df3["Date"] = df3["Date"].apply(lambda x: x.replace(".", "-"))
    df3['Date'] = pd.to_datetime(df3['Date'], infer_datetime_format=True)

    df3.sort_values(by='Date', ascending=False, inplace=True)

    worksheet3.clear()

    set_with_dataframe(worksheet=worksheet3, dataframe=df3.head(20), include_index=False, include_column_header=True,
                       resize=True)

    return df3.head(20)


news_scraping_diepresse()

news_scraping_kurier()

news_scraping_wienerzeitung()