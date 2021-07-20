import random
import time
import requests
import pandas as pd
import re
import datetime
from datetime import date
today = date.today()
from datetime import datetime as dt
import os
import socket
from selenium import webdriver
from dateutil import parser
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from pymongo import MongoClient
#關閉認證警告
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()
import logging
from lib.crawl_logger import logger
requests.packages.urllib3.disable_warnings()
logging.getLogger().setLevel(logging.DEBUG)


# global variable
user_agents = [
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9'
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
]

# add proxy
# proxy = 'T3-MIS-BI.ad.garmin.com:8888'
# chrome_options.add_argument('--proxy-server=http://' + proxy)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--log-level=3')
chrome_options.add_argument('--no-sandbox') #bypass the OS security model
chrome_options.add_experimental_option(
    'excludeSwitches', ['enable-automation'])
chrome_options.add_argument('user-agents=' + random.choice(user_agents))

# data
# df_test = pd.read_csv(r'D:/Amazon_products/product_list_jp_official.csv')
df_test = pd.read_csv('../amazon/product_list_jp_official_server.csv')


def get_all_commodities():

    '''
    進入商品主頁面，主要為了得到商品名稱&href
    '''

    temp_list = []
    browser = webdriver.Chrome(chrome_options=chrome_options, executable_path = "../chromedriver")
    for u in range(len(df_test)):
        try:
            url = 'https://www.amazon.co.jp/s?k=' + df_test['Product'][u]
            browser.get(url)
            element = browser.find_element_by_xpath('//*[@id="search"]/div[1]/div/div[1]/div/span[3]/div[2]/div[2]/div')
            temp = {}
            keyword = df_test['Product'][u]
            temp['asin'] = keyword
            locator = (By.XPATH, '//*[@id="search"]/div[1]/div[2]/div/span[3]/div[2]/div[2]/div/span/div/div/div[2]/h2/a/span')
            try:
                title = element.find_element_by_css_selector('span.a-size-base-plus.a-color-base.a-text-normal').text.upper().replace('Í', 'I')
                temp['title'] = title
            except NoSuchElementException:
                temp['title'] = df_test['Item'][u].upper().replace('Í', 'I')
            href = 'https://www.amazon.co.jp/dp/' + keyword
            temp['href'] = href
            temp_list.append(temp)
        except:
            continue
            browser.quit()
    browser.quit()
    temp_df = pd.DataFrame(temp_list)
    return temp_list, temp_df
    

# positive
def get_pos_count(keyword, review_count, browser):

    '''
    正面留言跟反面流言有不同的URL規則，這個function進入正面留言的頁面爬正面流言數
    '''

    url = "https://www.amazon.co.jp/product-reviews/" + keyword + "/ref=cm_cr_arp_d_viewpnt_lft?ie=UTF8&reviewerType=all_reviews&filterByStar=positive&pageNumber="
    browser.get(url)
    try:
        locator = (By.XPATH, '//*[@id="customer_review-RTB3PT1N1NOSC"]/div[4]/span/span')
        WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
        time.sleep(5)
    except TimeoutException:
        time.sleep(1.5)
        pass
    pos = browser.find_elements_by_xpath(
        '//*[@id="filter-info-section"]/div[2]/span')
    if len(pos) != 0:
        pos_count = int(pos[0].text[-13:-1].replace('グローバルレビュ', '').replace(' ','').replace('|',''))
    else:
        pos_count = 0
    return pos_count
    

# negative
def get_neg_count(keyword, review_count, browser):

    '''
    正面留言跟反面流言有不同的URL規則，這個function進入負面留言的頁面爬負面流言數
    '''

    url = "https://www.amazon.co.jp/product-reviews/" + keyword + "/ref=cm_cr_arp_d_viewpnt_rgt?ie=UTF8&reviewerType=all_reviews&filterByStar=critical&pageNumber="
    browser.get(url)
    try:
        locator = (By.XPATH, '//*[@id="customer_review-RTB3PT1N1NOSC"]/div[4]/span/span')
        WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
        time.sleep(5)
    except TimeoutException:
        time.sleep(1.5)
        pass
    neg = browser.find_elements_by_xpath(
        '//*[@id="filter-info-section"]/div[2]/span')
    if len(neg) != 0:
        neg_count = int(neg[0].text[-13:-1].replace('グローバルレビュ', '').replace(' ','').replace('|',''))
    else:
        neg_count = 0
    return neg_count
    

def get_all_attribute(temp_df, crawled_item):

    '''
    利用在get_all_commodities得到的href，爬更細項的商品資訊，也會再append get_pos_count得到的正面留言數，get_neg_count得到的負面流言數
    '''

    browser = webdriver.Chrome(chrome_options=chrome_options, executable_path = "/home/oracle/chromedriver")
    commodities = []
    commodity_index = 0
    for index, page in temp_df.iterrows():
        commodity_index += 1
        commodity = {}
        url = page['href']
        keyword = page['asin']
        browser.get(url)
        commodity['item_url'] = page['href']
        try:
            commodity['pid'] = page['asin']
        except:
            commodity['pid'] = keyword
        commodity['title'] = page['title']
        commodity['platform'] = 'Amazon'
        today_str = str(today)
        today_datetime = dt.strptime(today_str, "%Y-%m-%d")
        commodity['creatDate'] = today_datetime
        
        # country
        if 'co.uk' in page['href']:
            commodity['country'] = 'UK'
        elif 'amazon.com' in page['href']:
            commodity['country'] = 'USA'
        elif 'co.jp' in page['href']:
            commodity['country'] = 'JP'
        else:
            commodity['country'] = 'Others'
            
        # brand
        try:
            locator = (By.XPATH, '//*[@id="bylineInfo"]')
            WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
            time.sleep(5)
        except TimeoutException:
            time.sleep(1.5)
            continue
        try:
            brand = browser.find_element_by_css_selector(
                'div#bylineInfo_feature_div div.a-section.a-spacing-none a#bylineInfo')
            commodity['brand'] = brand.text
        except:
            commodity['brand'] = 'NONE'
            
        # tags
        tags = browser.find_elements_by_css_selector("span.cr-lighthouse-term")
        commodity["tags"] = []
        for tag in tags:
            x = tag.text.replace(" ", "").replace("\n", "").replace("\b", "")
            if len(x) != 0:
                commodity["tags"].append(x)
                
        # average_star
        try:
            locator = (By.XPATH, '//*[@id="reviewsMedley"]/div/div[1]/div[2]/div[1]/div/div[2]/div/span/span')
            WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
            time.sleep(5)
        except TimeoutException:
            time.sleep(1.5)
            continue
        average_star = browser.find_elements_by_xpath(
            '//*[@id="reviewsMedley"]/div/div[1]/div[2]/div[1]/div/div[2]/div/span/span')
        if len(average_star) != 0:
            commodity['average_star'] = float(average_star[0].text.replace('星5つ中の', '').replace('5つ星のうち',''))
        else:
            commodity['average_star'] = 0.0
            
        # every star
        try:
            locator = (By.XPATH, '//*[@id="reviewsMedley"]/div/div[1]/span[1]/div[1]/div')
            WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
            time.sleep(5)
        except TimeoutException:
            time.sleep(1.5)
            continue
        every_star = browser.find_elements_by_css_selector(
            'td.a-text-right.a-nowrap span.a-size-base a.a-link-normal')
        index = 5
        for i in range(0, len(every_star)):
            commodity['star' + str(index)
                      ] = float(every_star[i].text.replace('%', '')) / 100
            index -= 1
            
        # review_count
        try:
            locator = (By.XPATH, '//*[@id="acrCustomerReviewText"]')
            WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
            time.sleep(5)
        except TimeoutException:
            time.sleep(1.5)
            continue
        review_count = browser.find_elements_by_css_selector('span#acrCustomerReviewText')
        if len(review_count) > 0:
            commodity['review_count'] = int(
                review_count[0].text.replace('個の評価', '').replace(',', ''))
        else:
            commodity['review_count'] = 0
            
        # review_url
        try:
            commodity["review_url"] = "https://www.amazon.co.jp/product-reviews/" + commodity["pid"] + "/"
        except TypeError:
            commodity["review_url"] = "https://www.amazon.co.jp/product-reviews/" + keyword + "/"
            
        # get_all_pos_reviews
        commodity['pos_count'] = get_pos_count(
            keyword, review_count, browser)
            
        # get_all_neg_reviews
        commodity['neg_count'] = get_neg_count(
            commodity['pid'], commodity['review_count'], browser)
            
        # append to commodities
        commodities.append(commodity)
        # print('\npos_count:', commodity['pos_count'],
              # '\nneg_count:', commodity['neg_count'])
    browser.quit()
    return commodities


def clean_title(test_AmazonJP_new):

    '''
    整理title，去除多餘的文字
    '''
    
    test_AmazonJP_new = test_AmazonJP_new.rename(columns={'title': 'ori_title'})
    test_AmazonJP_new['title'] = ''
    title_list = []
    for row in test_AmazonJP_new['ori_title']:
        fix_title = row.upper().replace('Í', 'I').replace(
                                'GARMIN', '').replace('GPS', '').replace('CITY','').replace('USB','').replace(
                                'SLATE','').replace('5日', '').replace('13日', '').replace('Ē','E').replace(
                                '010','').replace('02247','').replace('40','').replace('02256','').replace(
                                '08','').replace('02258','').replace('2B','').replace('02260','').replace(
                                '10','').replace('BLUE', '').replace('BLACK','').replace('MUSIC','').replace(
                                'VO2MAX','').replace('S/M','').replace('01789','').replace('70','').replace(
                                'PIPELINE','').replace('CLOUDBREAK','').replace('CERAMIC','').replace(
                                '01702','').replace('22','').replace('3個','').replace('14個','').replace(
                                'CT 3センサー','').replace('1901','').replace('TRUSWING','').replace(
                                'TRAN','').replace('フォアアスリート745','').replace('745用','').replace(
                                '(R)','').replace('MINNIE','').replace('MOUSE','').replace('MARVEL','').replace(
                                'DISNEY','').replace('STAR','').replace('WARS','')
                                
        # 再用正則整理一次讓title只包含英文跟數字
        title = re.findall('[a-zA-Z0-9]+',fix_title)
        title = " ".join(title).strip().upper()
        title_list.append(title)
    test_AmazonJP_new['title'] = title_list
    return test_AmazonJP_new


def add_series(test_AmazonJP_new):

    '''
    加上商品是屬於哪個系列
    '''
    
    test_AmazonJP_new['series'] = ''
    for i in range(len(test_AmazonJP_new)):
        if 'SWIM' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'others'
        elif 'FENIX' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'fenix Series'
        elif 'VIVO' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'vivo Series'
        elif 'LEGACY' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'vivo Series'
        elif 'VENU' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'vivo Series'
        elif 'INSTINCT' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'instinct Series'
        elif 'GPS' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'Handy GPS products'
        elif 'MAP' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'Handy GPS products'
        elif 'APPROACH' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'Approach Series'
        elif 'FOREATHLETE' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'ForeAthlete Series'
        elif 'MARQ' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'MARQ Series'
        elif 'ETREX' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'Handy GPS products'
        elif 'VARIA' in test_AmazonJP_new['title'][i]:
            test_AmazonJP_new['series'][i] = 'Cycling Products'
    return test_AmazonJP_new
    

def update(test_AmazonJP_new):

    '''
    update商品資訊的規則
    '''
    
    uri = "mongouri"  #production
    client = MongoClient(uri)
    db = client['DBAA']  #'DBAA'資料庫
    collection_name='ECOMMERCE_MAIN'  #'test table' 
    collect = db[collection_name]
    for i in range(test_AmazonJP_new.shape[0]):
        collect.update_one({
            'platform': test_AmazonJP_new['platform'][i],
            'item_url': test_AmazonJP_new['item_url'][i]},
            {
                '$set': {
                        'title' : test_AmazonJP_new['title'][i],
                        'ori_title': test_AmazonJP_new['ori_title'][i],
                        'pid' : test_AmazonJP_new['pid'][i],
                        'item_url' : test_AmazonJP_new['item_url'][i],
                        'platform' : test_AmazonJP_new['platform'][i],
                        'creatDate' : test_AmazonJP_new['creatDate'][i],
                        'tags': test_AmazonJP_new['tags'][i],
                        'country' : test_AmazonJP_new['country'][i],
                        'average_star': float(test_AmazonJP_new['average_star'][i]),
                        'star1': float(test_AmazonJP_new['star1'][i]),
                        'star2': float(test_AmazonJP_new['star2'][i]),
                        'star3': float(test_AmazonJP_new['star3'][i]),
                        'star4': float(test_AmazonJP_new['star4'][i]),
                        'star5': float(test_AmazonJP_new['star5'][i]),
                        'review_url': test_AmazonJP_new['review_url'][i],
                        'review_count': int(test_AmazonJP_new['review_count'][i]),
                        'pos_count': int(test_AmazonJP_new['pos_count'][i]),
                        'neg_count': int(test_AmazonJP_new['neg_count'][i]),
                        'brand': test_AmazonJP_new['brand'][i],
                        'series': test_AmazonJP_new['series'][i]
                        }
            },upsert=True)
    client.close()
    
def main():

    global keyword
    keyword = ''

    global price_limit
    price_limit = 0

    test_AmazonJP = pd.DataFrame(columns=['title', 'pid', 'item_url', 'platform',
                                          'creatDate',
                                          'tags', 'seller', 'country',
                                          'average_star',
                                          'star1', 'star2', 'star3', 'star4',
                                          'star5', 'review_url', 'review_count',
                                          'pos_count', 'neg_count'])


            
    # 主要執行
    for i in range(len(df_test)):
        crawled_item = df_test['Item'][i]
        keyword = df_test['Product'][i]
        url = 'https://www.amazon.co.jp/s?k=' + keyword
        
    temp_list, temp_df = get_all_commodities()
    commodities = get_all_attribute(temp_df, crawled_item)
    AmazonJP_df = pd.DataFrame(commodities)
    # 刪除重複pid，以避免爬到相同商品
    test_AmazonJP_new = AmazonJP_df.drop_duplicates(subset='pid', keep='first')
    test_AmazonJP_new = clean_title(test_AmazonJP_new)
    test_AmazonJP_new = test_AmazonJP_new.reset_index(drop=True)
    test_AmazonJP_new = add_series(test_AmazonJP_new)
    update(test_AmazonJP_new)
    

if __name__ == "__main__":
    main()
