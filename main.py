from urllib import response
from bs4 import BeautifulSoup
import time
import certifi
import urllib3
import requests
from urllib3 import ProxyManager, make_headers
from urllib.request import Request, urlopen
import mysql.connector
from urllib.parse import urlparse
from fake_useragent import UserAgent
import random
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

from deep_translator import (GoogleTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)
ua = UserAgent()
chrome_ua = ua.chrome

# MYSQL CONNECTION PARAMS
cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='flatfoxdb')
cursor = cnx.cursor(buffered=True)
start = time.time()


session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=8))
session.mount("http://", HTTPAdapter(max_retries=8))
count = 0
def status(str):
    print(str)

def inc(): 
    global count 
    count += 1

pcount = 0

good_proxies = []

def clear_txt():
    f = open('good.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('good2.txt', 'r+')
    f.truncate(0) # need '0' when using r+

def clear_links_txt():
    f = open('links.txt', 'r+')
    f.truncate(0) # need '0' when using r+
   

def proxies_list():
    headers={'User-Agent': chrome_ua}
    response = requests.get('https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt', headers=headers)
    with open("response.txt", "w") as f:
        f.write(response.text)
        f.close()

def proxies_arr():
    proxies_arr = []
    with open('response.txt', 'r') as reader:
        for line in reader.readlines():
            # print(line, end='')
            proxies_arr.append(line.strip())
    return proxies_arr


def extract(proxy):
    global pcount
    headers={'User-Agent': ua.chrome}
    r = requests.get('https://flatfox.ch/api/v1/pin/?east=10.492340&max_count=400&north=48.514639&offer_type=SALE&south=45.075166&west=5.956080', headers=headers, proxies={'http' :proxy,'https': proxy},timeout=2)
    if(r.status_code == 200):
        pcount = pcount + 1
        print(pcount, " ", proxy, " is working ", r.status_code)
        with open("good2.txt", "a") as myfile:
            myfile.write(proxy)
            myfile.write('\n')
            myfile.close()
        good_proxies.append(proxy)
    return proxy

def getAllBuyProperties(proxy):
    status("GETTING BUY PROPERTIES....")
    ids = []
    time.sleep(3)
    proxies = {
                'http' :proxy,
                'https':proxy
                }
    headers={'User-Agent': chrome_ua}
    
    session.proxies.update(proxies)
    session.headers.update(headers)
    while True:
        try:
            response = session.get('https://flatfox.ch/api/v1/pin/?east=10.492340&max_count=400&north=48.514639&offer_type=SALE&south=45.075166&west=5.956080')
            break
        except requests.exceptions.ProxyError:
            print("Proxy Error Encountered: Reloading")
    j = json.loads(response.text)
    for x in j:
        link = x["pk"]
        inc()
        status("gotten list " + str(count) + ": " + str(link))
        ids.append(link)

    with open('links.txt', "w") as  f:
        for line in ids:
            f.write(str(line) + "\n") 
    print("successful written to the file ")
    f.close()



def getTimeRange():
    arr = []
    timestamp = time.strftime('%H');
    hour = int(timestamp)
    arr = [1 + (hour - 9) * 48, 1 + (hour - 8) * 48]
    return arr

def readFile():
    with open('links.txt', 'r') as f:
        arr = f.readlines()
        lines = len(arr)
        lines_range = getTimeRange()
        print(lines_range)
        data = arr[lines_range[0]:lines_range[1]]
       
    f.close()
    return data

def getData(proxy):
    data = readFile()
    newdata = []
    for link in data:
        x = "&pk=" + link.strip()
        newdata.append(x)
    status("GETTING ALL DATA USING THEIR UNIQUE IDS....")
    string = ''.join(str(x) for x in newdata)
    proxies = {
                'http' :proxy,
                'https':proxy,
                }
    headers={'User-Agent': chrome_ua}
    session.proxies.update(proxies)
    session.headers.update(headers)
    while True:
        try:
            response = session.get('https://flatfox.ch/api/v1/public-listing/?expand=cover_image&include=is_liked&include=is_disliked&include=is_subscribed&limit=0' + string)
            break
        except requests.exceptions.ProxyError:
            print("Proxy Error Encountered: Reloading")

    j = json.loads(response.text)    
    for row in j:
        typeprop = row["offer_type"]
        price = row["price_display"]
        description = row["description_title"]
        propertylink = row["pk"]
        numRooms = row["number_of_rooms"]
        street = row["street"]
        city = row["city"]
        livingSpace = row["livingspace"]
        category = row["object_category"]
        constructionDate = row["year_built"]
        print(propertylink)
        vals = (propertylink,)
        cursor.execute('SELECT propertylink FROM properties WHERE propertylink = %s', vals)
        cnx.commit()
        newcount = cursor.rowcount
        if(newcount == 0):
            sql = 'INSERT INTO properties(typeprop, price, description, propertylink, numRooms, street, city, livingSpace, category, constructionDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            sql_vals =  (typeprop, price, description, propertylink, numRooms, street, city, livingSpace, category, constructionDate)

            cursor.execute(sql, sql_vals)
            cnx.commit()
            print("affected rows = " + str(cursor.rowcount))
        else:
            print("Already in Database")

       

                


# print(getTimeRange())
# print(save_proxies)
start = time.time()

clear_txt()

proxies_list()
proxylist = proxies_arr()
with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract, proxylist)
proxies = [*set(good_proxies)]
print(len(proxies), " are working well")
if(time.strftime('%H') == '9'):
    clear_links_txt()
    getAllBuyProperties(random.choice(proxies))

getData(random.choice(proxies))
cursor.close()

end = time.time()

print(end - start)