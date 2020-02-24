# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 23:49:02 2019

@author: Administrator
"""

import requests
from concurrent.futures import ThreadPoolExecutor
from pyquery import PyQuery as pq
import json
import threading
import time
import pymssql


def get_list_page_url():

    start = "http://zf.szhome.com"
    start_url = "http://zf.szhome.com/Search.html?sor=1&aom=1&kwd=&xzq=0&pq=0&price=0&prif=0&prit=0&barea=0&baf=0&bat=0&hx=0&ord=0&dtyx=0&dtst=0&scat=0&sx=0&schid=0&page="
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    }
    try:
        response = requests.get(start, headers=headers)
        doc = pq(response.text)
        total_num =  int(doc("h2 span em").text())
        total_page = total_num // 20 + 1

        page_url_list = list()

        for i in range(total_page):
            url = start_url + str(i + 1)
            page_url_list.append(url)
        return page_url_list

    except:
        print("获取总套数出错,请确认起始URL是否正确")
        return None


detail_list = list()

# 需要先在本地开启代理池
def get_valid_ip():
    try:
        ip = requests.get("http://127.0.0.1:5010/get/").json()
        return ip
    except:
        print("请先运行代理池")


def get_detail_page_url(page_url):
    pre_url = "http://zf.szhome.com"
    global detail_list
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'http://bol.szhome.com/'
    }

    try:
        response = requests.get(page_url,headers=headers,timeout=3)
        doc = pq(response.text)
        i = 0
        detail_urls = list()
        
        for item in doc(".searchcont .lpinfo").items():
            i += 1
            if i == 21:
                break
            child_item = item(".mianbox a")
            if child_item == None:
                i -= 1
            detail_url = child_item.attr("href")
            detail_urls.append(pre_url + detail_url)
        return detail_urls
    except:
        print("获取列表页" + page_url + "出错")

lock = threading.Lock()

def detail_page_parser(res):
    global detail_list
    detail_urls = res.result()
    if not detail_urls:
        print("detail url 为空")
        return None
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'http://bol.szhome.com/'
    }
    for detail_url in detail_urls:
        try:
            response = requests.get(url=detail_url, headers=headers,timeout=3)
            detail_dict = dict()
            doc = pq(response.text)
            unit_price = doc(".two .f18").text()
            unit_price = unit_price[0:unit_price.index("元")]#去掉单位
            title = doc("h1").text()
            area1 = doc(".wrap.path.fix .left a").eq(2).text().strip()
            area1 = area1[:-3]
            area2 = doc(".wrap.path.fix .left a").eq(3).text().strip()
            area2 = area2[:-3]
            area = area1 + area2
            floor = doc(".one p").text()
            url = detail_url
            
            detail_dict["title"] = title
            detail_dict["area"] = area
            detail_dict["price"] = unit_price
            detail_dict["floor"] = floor
            detail_dict["url"] = url

            detail_list.append(detail_dict)

            print(unit_price, title, area, floor)

        except:
            print("获取详情页出错,换ip重试")
            proxy = get_valid_ip().get("proxy")
            #proxies = {
            #    "http": "http://" + get_valid_ip(),
            #}
            try:
                response = requests.get(url=detail_url, headers=headers, proxies={"http": "http://{}".format(proxy)})
                detail_dict = dict()
                doc = pq(response.text)
                unit_price = doc(".two .f18").text()
                unit_price = unit_price[0:unit_price.index("元")]
                title = doc("h1").text()
                area1 = doc(".wrap.path.fix .left a").eq(2).text().strip()
                area1 = area1[:-3]
                area2 = doc(".wrap.path.fix .left a").eq(3).text().strip()
                area2 = area2[:-3]
                area = area1 + area2                   
                floor = doc(".one p").text()
                url = detail_url 
                
                if len(unit_price)>0:
                    detail_dict["title"] = title
                    detail_dict["area"] = area
                    detail_dict["price"] = unit_price
                    detail_dict["floor"] = floor
                    detail_dict["url"] = url

                    detail_list.append(detail_dict)

                    print(unit_price, title, area, floor)
            except:
                print("重试失败...")


def save_data(data,filename):
    with open("Lj_"+filename+"_ershou.json", 'w', encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))

def savetoSQL(detail_list, city):
    #sql 
    city = city + "_ershoufang"
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    if not cursor:
        raise(NameError,"连接数据库失败")
    else:
        print('OK')

    sql = """CREATE TABLE """ + city + """(
    title VARCHAR(200),
    area VARCHAR(200),
    price VARCHAR(200),
    floor VARCHAR(200),               
    url VARCHAR(200))"""
    try:
        cursor.execute("select * from " + city)
    except:
        cursor.execute(sql)
    for data in detail_list:
        find="select * from " + city + " where title='" + data["title"] + "' and " + "price='" +data["price"] +"' and area='"+data["area"]+"' and floor='"+data["floor"]+"'"
        cursor.execute(find)
        row = cursor.fetchone()
        if row:
            continue
        else:
            value = tuple(data.values())           
            inesrt_re = "INSERT INTO " + city + " VALUES (%s, %s, %s, %s, %s)"            
            cursor.executemany(inesrt_re, [value])
            db.commit()
            
    print('存入数据库成功！...')
    db.close()   
    
def main():
    
    city = 'sz'
    page_url_list = get_list_page_url()

    p = ThreadPoolExecutor(30)

    for page_url in page_url_list:
        p.submit(get_detail_page_url, page_url).add_done_callback(detail_page_parser)

    p.shutdown()

    save_data(detail_list, city)
    savetoSQL(detail_list, city)
    detail_list.clear()

if __name__ == '__main__':
    old = time.time()
    main()
    new  = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))
