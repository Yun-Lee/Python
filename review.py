from selenium import webdriver
from dateutil import parser
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd
import time
import random
from datetime import date
import datetime
today = date.today()
from datetime import datetime as dt
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()
import logging
#from crawl_logger import logger
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
logging.getLogger().setLevel(logging.DEBUG)

# GLOBAL VARIABLES
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

# PROD
browser = webdriver.Chrome(chrome_options=chrome_options, executable_path = "../chromedriver")

# 在本地端跑
#path = 'D:\Rakuten_products\chromedriver.exe'
#browser = webdriver.Chrome(executable_path = path, chrome_options=chrome_options)


def get_soup(url, time_):
    time.sleep(random.uniform(0.3, time_))
    html = requests.get(url, verify = False, headers={'User-Agent': random.choice(user_agents)})
    return BeautifulSoup(html.text, 'lxml')


def connect_to_mongo():

    '''
    抓MONGO裡面既有的商品出來
    '''
    
    #正式環境
    uri = "mongouri"
    client = MongoClient(uri, authsource='', username = '', password = '')
    client = MongoClient()
    db = client['DBAA']
    ECOMMERCE_MAIN = db.ECOMMERCE_MAIN
    result = pd.DataFrame(list(ECOMMERCE_MAIN.find()))
    
    # 整理
    test = result[result['platform']=='Amazon']
    test = test.reset_index(drop=True)
    
    #因為platform collection包含了其他電商網站的資料，以下column對於Amazon來說不適用
    test = test.drop(['_id', 'man_10', 'man_20', 'man_30', 'man_40', 'man_50',
                      'point', 'woman_10', 'woman_20', 'woman_30', 'woman_40', 'woman_50',
                      'thread_url', 'thread_count', 'ori_title', 'crtdate','crawled_date',
                      'category_avg_size','category_avg_operability','category_avg_functionality',
                      'category_avg_design','category_avg_battery','category_average_rating','avg_size',
                      'avg_operability', 'avg_functionality', 'avg_design', 'avg_battery',
                      'average_review_rating', 'thread_last_update', 'interested_ppl'], axis=1)
    test.sort_values("review_count", inplace=True)
    test = test.drop_duplicates(subset=['pid'], keep='first').reset_index(drop=True)
    
    return test



def pos_get_all_reviews(keyword, browser):

    '''
    進入正面留言爬留言資訊
    '''
    
    url = "https://www.amazon.co.jp/product-reviews/" + keyword + "/ref=cm_cr_arp_d_viewpnt_lft?ie=UTF8&reviewerType=all_reviews&filterByStar=positive&pageNumber="
    browser = webdriver.Chrome(chrome_options=chrome_options, executable_path = "../chromedriver")
    browser.get(url)
    
    try:
        locator = (By.XPATH, '//*[@id="filter-info-section"]/div[2]/span')
        WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
        time.sleep(5)
    except TimeoutException:
        time.sleep(1.5)
        
    pos_reviews = []
    pos = browser.find_elements_by_xpath('//*[@id="filter-info-section"]/div[2]/span')
    
    if len(pos) != 0:
        pos_count = int(pos[0].text[-13:-1].replace('グローバルレビュ', '').replace(' ','').replace('|',''))
    else:
        pos_count = 0
    count = pos_count // 10
    
    if pos_count % 10 != 0:
        count += 1
    for i in range(1, count + 1):
        soup_temp = []
        try_count = 0
        while len(soup_temp) == 0 and try_count < 8:
            try_count += 1
            soup_temp = get_soup(
                url + str(i), 20.5).select("div#cm_cr-review_list")
        if len(soup_temp) == 0:
            break
        else:
            soup = soup_temp[0]
            
        model_type_temp = soup.select('div.a-row.a-spacing-mini.review-data.review-format-strip')
        id_temp = soup.select("div.a-section.review.aok-relative")
        name_temp = soup.select("span.a-profile-name")
        star_temp = soup.select("i.review-rating span")
        title_temp = soup.select("a.review-title-content span")
        date_temp = soup.select("span.review-date")
        comment_temp = soup.select("div.review-data span.review-text-content span")
        column_temp = soup.select('div.a-section.review.aok-relative')
            
        reviews_temp = []
        for j in range(len(name_temp)):
            temp = {}
            temp['model'] = model_type_temp[j].text.replace('色', '').replace(':','').replace(' ', '').replace('Amazonで購入','')
            temp["id"] = id_temp[j].get("id")
            temp["name"] = name_temp[j].text
            temp["star"] = float(star_temp[j].text.replace("5つ星のうち", ""))
            temp["title"] = title_temp[j].text
            try:
                temp["date"] = datetime.datetime.strptime(
                    date_temp[j].text.replace(" ", "").replace("年", "/").replace("月", "/").replace("日", "").replace('アメリカ合衆国','').replace(
                        "\n", "").replace('に', '').replace('日', '').replace('本', '').replace('カナダ','').replace('でレビュー', '').replace(
                        '済み',''),"%Y/%m/%d")
            except:
                temp['date'] = parser.parse(date_temp[j].text)
            temp["comment"] = comment_temp[j].text.replace("<br>", " ")
            temp['reply_url'] = 'https://www.amazon.co.jp/gp/customer-reviews/' + id_temp[j].get("id")
            
            # image
            img_list = []
            img_temp = column_temp[j].select('div.review-image-tile-section img')
            img_list.append(img_temp)
            temp['image'] = img_list
            temp['image_count'] = len(column_temp[j].select('div.review-image-tile-section img'))
            
            # video
            video_list = []
            video_temp = column_temp[j].select('input.video-url')
            video_list.append(video_temp)
            temp['video'] = video_list
                
            temp['review_type'] = 'positive'
            temp['pid'] = keyword
            reviews_temp.append(temp)
            
        pos_reviews += reviews_temp
        if len(reviews_temp) == 0:
            break
        print(".page " + str(i) + "-->" + str(len(reviews_temp)), end="  ")
    return pos_reviews
    

def neg_get_all_reviews(keyword, browser):

    '''
    進入負面留言爬留言資訊
    '''

    url = "https://www.amazon.co.jp/product-reviews/" + keyword + "/ref=cm_cr_arp_d_viewpnt_rgt?ie=UTF8&reviewerType=all_reviews&filterByStar=critical&pageNumber="
    browser = webdriver.Chrome(chrome_options=chrome_options, executable_path = "/home/oracle/chromedriver")
    browser.get(url)
    try:
        locator = (By.XPATH, '//*[@id="filter-info-section"]/div[2]/span')
        WebDriverWait(browser, 20).until(EC.presence_of_element_located(locator))
        time.sleep(5)
    except TimeoutException:
        time.sleep(1.5)
    neg_reviews = []
    neg = browser.find_elements_by_xpath('//*[@id="filter-info-section"]/div[2]/span')
    if len(neg) != 0:
        neg_count = int(neg[0].text[-13:-1].replace('グローバルレビュ', '').replace(' ','').replace('|',''))
    else:
        neg_count = 0

    count = neg_count // 10
    if neg_count % 10 != 0:
        count += 1
    for i in range(1, count + 1):
        soup_temp = []
        try_count = 0
        while len(soup_temp) == 0 and try_count < 8:
            try_count += 1
            soup_temp = get_soup(
                url + str(i), 20.5).select("div#cm_cr-review_list")
        if len(soup_temp) == 0:
            break
        else:
            soup = soup_temp[0]
        model_type_temp = soup.select('div.a-row.a-spacing-mini.review-data.review-format-strip')
        id_temp = soup.select("div.a-section.review.aok-relative")
        name_temp = soup.select("span.a-profile-name")
        star_temp = soup.select("i.review-rating span")
        title_temp = soup.select("a.review-title-content span")
        date_temp = soup.select("span.review-date")
        comment_temp = soup.select("div.review-data span.review-text-content span")
        column_temp = soup.select('div.a-section.review.aok-relative')
            
        reviews_temp = []
        for j in range(len(name_temp)):
            temp = {}
            temp['model'] = model_type_temp[j].text.replace('色', '').replace(':','').replace(' ', '').replace('Amazonで購入','')
            temp["id"] = id_temp[j].get("id")
            temp["name"] = name_temp[j].text
            temp["star"] = float(star_temp[j].text.replace("5つ星のうち", ""))
            try:
                temp["title"] = title_temp[j].text
            except IndexError:
                temp["title"] = 'NO TITLE'
            try:
                temp["date"] = datetime.datetime.strptime(
                    date_temp[j].text.replace(" ", "").replace("年", "/").replace("月", "/").replace("日", "").replace('アメリカ合衆国','').replace(
                        "\n", "").replace('に', '').replace('日', '').replace('本','').replace('カナダ','').replace('でレビュー', '').replace(
                        '済み',''),"%Y/%m/%d")
            except:
                temp['date'] = parser.parse(date_temp[j].text)
            temp["comment"] = comment_temp[j].text.replace("<br>", " ")

            temp['reply_url'] = 'https://www.amazon.co.jp/gp/customer-reviews/' + id_temp[j].get("id")
            
            # image
            img_list = []
            img_temp = column_temp[j].select('div.review-image-tile-section img')
            img_list.append(img_temp)
            temp['image'] = img_list
            temp['image_count'] = len(column_temp[j].select('div.review-image-tile-section img'))
            
            # video
            video_list = []
            video_temp = column_temp[j].select('input.video-url')
            video_list.append(video_temp)
            temp['video'] = video_list
            
            temp['review_type'] = 'negative'
            temp['pid'] = keyword
            reviews_temp.append(temp)
        neg_reviews += reviews_temp
        if len(reviews_temp) == 0:
            break
        print(".page " + str(i) + "-->" + str(len(reviews_temp)), end="  ")
        
    return neg_reviews
    
    
def clean_data(append_neg_reviews, append_pos_reviews):

    '''
    對正負留言裡抓到的照片/影片作資料整理
    '''

    # negative image
    append_neg_reviews['src'] = ""
    for z in range(len(append_neg_reviews)):
        src_list = []
        if len(append_neg_reviews) > 0:
            for i in range(len(append_neg_reviews.iloc[z]['image'][0])):
                kk = append_neg_reviews.iloc[z]['image'][0][i].get('src')
                # 換成高解析度的圖的url
                kkk = kk[:-6]+'1600.jpg'
                src_list.append(kkk)
                append_neg_reviews['src'][z] = src_list
    
    # positive image
    append_pos_reviews['src'] = ""
    for z in range(len(append_pos_reviews)):
        src_list = []
        if len(append_pos_reviews) > 0:
            for i in range(len(append_pos_reviews.iloc[z]['image'][0])):
                kk = append_pos_reviews.iloc[z]['image'][0][i].get('src')
                # 換成高解析度的圖的url
                kkk = kk[:-6]+'1600.jpg'
                src_list.append(kkk)
                append_pos_reviews['src'][z] = src_list
                
    # positive video
    append_pos_reviews['mp4'] = ""
    for z in range(len(append_pos_reviews)):
        v_list = []
        if len(append_pos_reviews) > 0:
            for i in range(len(append_pos_reviews.iloc[z]['video'][0])):
                vv = append_pos_reviews.iloc[z]['video'][0][i].get('value')
                v_list.append(vv)
                append_pos_reviews['mp4'][z] = v_list
                
    # negative video
    append_neg_reviews['mp4'] = ""
    for z in range(len(append_neg_reviews)):
        v_list = []
        if len(append_neg_reviews) > 0:
            for i in range(len(append_neg_reviews.iloc[z]['video'][0])):
                vv = append_neg_reviews.iloc[z]['video'][0][i].get('value')
                v_list.append(vv)
                append_neg_reviews['mp4'][z] = v_list
    
    # 資料整理
    if len(append_neg_reviews) > 0:
        del append_neg_reviews['image']   
        del append_neg_reviews['video']
    if len(append_pos_reviews) > 0:
        del append_pos_reviews['image']
        del append_pos_reviews['video']

    return append_neg_reviews, append_pos_reviews


def update_negative(append_neg_reviews):

    '''
    update負面流言的規則
    '''

    uri = "mongouri"  #production
    client = MongoClient(uri)
    db = client['DBAA']  #'DBAA'資料庫
    collection_name='ECOMMERCE_REPLY'  #'test table' 
    collect = db[collection_name]
    
    for i in range(append_neg_reviews.shape[0]):
        collect.update_one({
            'id': append_neg_reviews['id'][i],
            'comment': append_neg_reviews['comment'][i],
            'pid': append_neg_reviews['pid'][i]},
            {
                '$set': {
                        'id' : append_neg_reviews['id'][i],
                        'name' : append_neg_reviews['name'][i],
                        'star' : append_neg_reviews['star'][i],
                        'title' : append_neg_reviews['title'][i],
                        'date' : append_neg_reviews['date'][i],
                        'comment' : append_neg_reviews['comment'][i],
                        'review_type' : append_neg_reviews['review_type'][i],
                        'pid' : append_neg_reviews['pid'][i],
                        'image_count': append_neg_reviews['image_count'][i],
                        'src': append_neg_reviews['src'][i],
                        'model': append_neg_reviews['model'][i],
                        'mp4': append_neg_reviews['mp4'][i],
                        'platform': append_neg_reviews['platform'][i],
                        'reply_url': append_neg_reviews['reply_url'][i]
                        }
            },upsert=True)
    client.close()


def update_positive(append_pos_reviews):

    '''
    update負面流言的規則
    '''
    
    uri = "mongouri"  #production
    client = MongoClient(uri)
    db = client['DBAA']  #'DBAA'資料庫
    collection_name='ECOMMERCE_REPLY'  #'test table' 
    collect = db[collection_name]
    
    for i in range(append_pos_reviews.shape[0]):
        collect.update_one({
            'pid': append_pos_reviews['pid'][i],
            'comment': append_pos_reviews['comment'][i],
            'id': append_pos_reviews['id'][i]},
            {
                '$set': {
                        'id' : append_pos_reviews['id'][i],
                        'name' : append_pos_reviews['name'][i],
                        'star' : append_pos_reviews['star'][i],
                        'title' : append_pos_reviews['title'][i],
                        'date' : append_pos_reviews['date'][i],
                        'comment' : append_pos_reviews['comment'][i],
                        'review_type' : append_pos_reviews['review_type'][i],
                        'pid' : append_pos_reviews['pid'][i],
                        'image_count': append_pos_reviews['image_count'][i],
                        'src': append_pos_reviews['src'][i],
                        'model': append_pos_reviews['model'][i],
                        'mp4': append_pos_reviews['mp4'][i],
                        'platform':append_pos_reviews['platform'][i],
                        'reply_url': append_pos_reviews['reply_url'][i]
                        }
            },upsert=True)
    client.close()

def main():

    append_neg_reviews = pd.DataFrame(columns=['id', 'name', 'star', 'title', 'date', 'comment', 'image', 'model'
                                               'review_type', 'pid', 'image_count', 'src', 'mp4', 'reply_url'])
    append_pos_reviews = pd.DataFrame(columns=['id', 'name', 'star', 'title', 'date', 'comment', 'image', 'model'
                                               'review_type', 'pid', 'image_count', 'src', 'mp4', 'reply_url'])   
    test = connect_to_mongo()
    index=0
    
    # 主要執行
    for i in range(len(test)):
        try:
            index += 1
            crawled_item = test['title'][i]
            keyword = test['pid'][i]
            neg_reviews = neg_get_all_reviews(keyword, browser)
            pos_reviews = pos_get_all_reviews(keyword, browser)
            neg_reviews = pd.DataFrame(neg_reviews)
            pos_reviews = pd.DataFrame(pos_reviews)
            append_neg_reviews = append_neg_reviews.append(neg_reviews)
            append_pos_reviews = append_pos_reviews.append(pos_reviews)
        except:
            browser.quit()
            continue
            print('browser closed due to except')
        finally:
            browser.quit()
            print('browser closed due to finally')
            
    # reset index
    append_neg_reviews = append_neg_reviews.reset_index(drop=True)
    append_pos_reviews = append_pos_reviews.reset_index(drop=True)

    # get src
    append_neg_reviews, append_pos_reviews = clean_data(append_neg_reviews, append_pos_reviews)
    append_neg_reviews['platform'] = 'Amazon'
    append_pos_reviews['platform'] = 'Amazon'

    # 正向負向的評論各自update到DB
    update_positive(append_pos_reviews)
    update_negative(append_neg_reviews)
    

if __name__ == "__main__":
    main()

