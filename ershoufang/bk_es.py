# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 23:52:26 2019

@author: Administrator
"""

import requests
from concurrent.futures import ThreadPoolExecutor
from pyquery import PyQuery as pq
import json
import threading
import time
import pymssql

def get_list_page_url(city):

    start_url = "https://{}.ke.com/ershoufang/".format(city)
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    }
    try:
        response = requests.get(start_url, headers=headers)
        doc = pq(response.text)
        total_num =  int(doc(".resultDes .total span").text())
        total_page = total_num // 30 + 1
        if total_page > 100:
            total_page = 100

        page_url_list = list()

        for i in range(total_page):
            url = start_url + "pg" + str(i + 1) + "/"
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
    global detail_list
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'https://zh.ke.com/ershoufang/'
    }

    try:
        response = requests.get(page_url,headers=headers,timeout=3)
        doc = pq(response.text)
        i = 0
        detail_urls = list()
        for item in doc(".sellListContent li").items():
            i += 1
            if i == 31:
                break
            child_item = item(".VIEWDATA")
            if child_item == None:
                i -= 1
            detail_url = child_item.attr("href")
            detail_urls.append(detail_url)
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
        'Referer': 'https://zh.ke.com/ershoufang/'
    }
    for detail_url in detail_urls:
        try:
            response = requests.get(url=detail_url, headers=headers,timeout=3)
            detail_dict = dict()
            doc = pq(response.text)
            city = doc(".container .fl a").eq(0).text().strip()
            city = city[:-2]            
            unit_price = doc(".unitPriceValue").text()
            #unit_price = unit_price[0:unit_price.index("元")]#去掉单位
            title = doc("h1").text()
            communityName = doc(".communityName .info.no_resblock_a").text()
            area = doc(".areaName .info a").text().replace(" ", "")
            floor = doc(".room .subInfo").text()
            direction = doc(".type .mainInfo").text()
            ReleaseTime = doc(".baseattribute.clear .transaction .content li").eq(0).text().replace('\n','').strip()
            ReleaseTime = ReleaseTime[4:]
            support = doc(".introContent.showbasemore .baseattribute.clear").text().strip()
            url = detail_url

            detail_dict["city"] = city
            detail_dict["title"] = title
            detail_dict["communityName"] = communityName
            detail_dict["area"] = area
            detail_dict["price"] = unit_price
            detail_dict["floor"] = floor
            detail_dict["direction"] = direction
            detail_dict["ReleaseTime"] = ReleaseTime
            detail_dict["support"] = support
            detail_dict["url"] = url

            detail_list.append(detail_dict)

            print(city, unit_price, title, communityName, area, floor, direction, ReleaseTime)

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
                city = doc(".container .fl a").eq(0).text().strip()
                city = city[:-2]  
                unit_price = doc(".unitPriceValue").text()
                #unit_price = unit_price[0:unit_price.index("元")]
                title = doc("h1").text()
                communityName = doc(".communityName .info.no_resblock_a").text()
                area = doc(".areaName .info a").text().replace(" ", "")
                floor = doc(".room .subInfo").text()
                direction = doc(".type .mainInfo").text()
                ReleaseTime = doc(".baseattribute.clear .transaction .content li").eq(0).text().replace('\n','').strip()
                ReleaseTime = ReleaseTime[4:]
                support = doc(".introContent.showbasemore .baseattribute.clear").text().strip()
                url = detail_url    
                
                if len(unit_price)>0:
                    detail_dict["city"] = city
                    detail_dict["title"] = title
                    detail_dict["communityName"] = communityName
                    detail_dict["area"] = area
                    detail_dict["price"] = unit_price
                    detail_dict["floor"] = floor
                    detail_dict["direction"] = direction
                    detail_dict["ReleaseTime"] = ReleaseTime
                    detail_dict["support"] = support
                    detail_dict["url"] = url

                    detail_list.append(detail_dict)

                    print(city, unit_price, title, communityName, area, floor, direction, ReleaseTime)
            except:
                print("重试失败...")


def save_data(data,filename):
    with open("Bk_"+filename+"_ershou.json", 'w', encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))

def savetoSQL(detail_list):
    #sql 

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

    sql = """CREATE TABLE ershoufang(
    city VARCHAR(200),
    title VARCHAR(200),
    communityName VARCHAR(200),
    area VARCHAR(200),
    price VARCHAR(200),
    floor VARCHAR(200),  
    direction VARCHAR(200), 
    time VARCHAR(200), 
    support VARCHAR(8000),             
    url VARCHAR(200))"""
    try:
        cursor.execute("select * from ershoufang")
    except:
        cursor.execute(sql)
    for data in detail_list:
        find="select * from ershoufang where title='" + data["title"] + "' and " + "price='" +data["price"] +"' and area='"+data["area"]+"' and floor='"+data["floor"]+"'"
        cursor.execute(find)
        row = cursor.fetchone()
        if row:
            continue
        else:
            value = tuple(data.values())           
            inesrt_re = "INSERT INTO ershoufang VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"            
            cursor.executemany(inesrt_re, [value])
            db.commit()
            
    print('存入数据库成功！...')
    db.close() 
    
def main():
    city_list =['dg']#['fs','zh','gz','sz','dg','zs','hui','jiangmen']
    for city in city_list:
        page_url_list = get_list_page_url(city)
        p = ThreadPoolExecutor(30)

        for page_url in page_url_list:
            p.submit(get_detail_page_url, page_url).add_done_callback(detail_page_parser)

        p.shutdown()

        #save_data(detail_list, city)
        savetoSQL(detail_list)
        detail_list.clear()

if __name__ == '__main__':
    old = time.time()
    main()
    new  = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))