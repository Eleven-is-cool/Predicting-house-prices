# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 15:04:16 2019

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
    start_url = "http://{}.esf.loupan.com".format(city)
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    }
    try:
        response = requests.get(start_url, headers=headers)
        doc = pq(response.text)
        total_num =  int(doc(".num span").text())
        total_page = total_num // 25 + 1
        if total_page > 100:
            total_page = 100
        page_url_list = list()

        for i in range(total_page):
            url = start_url + "/p" + str(i + 1) + "/"
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


def get_detail_page_url(page_url, city):
    pre_url = "http://{}.esf.loupan.com".format(city)
    global detail_list
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'http://zh.esf.loupan.com'
    }
    
    try:
        response = requests.get(page_url,headers=headers,timeout=3)
        doc = pq(response.text)
  
        i = 0
        detail_urls = list()
        for item in doc(".main .list li").items():
            
            i += 1
            if i == 26:
                break
            child_item = item("a")
            if child_item == None:
                i -= 1
            detail_url = child_item.attr("href")
            detail_urls.append(pre_url + detail_url)
        #print(detail_urls)
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
        'Referer': 'http://zh.esf.loupan.com'
    }
    for detail_url in detail_urls:
        try:
            response = requests.get(url=detail_url, headers=headers,timeout=3)
            detail_dict = dict()
            doc = pq(response.text)
            city = doc(".currentPage .m_box .pos a").eq(0).text().strip()
            city = city[:-3]            
            unit_price = doc(".price span i").text()
            title = doc("h1").text()
            communityName = doc(".ps .xq p").text()
            area1 = doc(".wz p").eq(0).text().strip()
            if area1=='蓬江':
                area1 ='蓬江区'
            elif area1=='江海':
                area1 ='江海区'
            elif area1=='新会':
                area1 ='新会区'
            elif area1=='台山':
                area1 ='台山市'
            elif area1=='鹤山':
                area1 ='鹤山市'
            elif area1=='开平':
                area1 ='开平市'
            elif area1=='恩平':
                area1 ='恩平市'
            else:
                if len(area1) > 0:
                    area1 += '区'
            area2 = doc(".wz p").eq(1).text().strip()
            area = area1 + area2
            floor = doc(".imgText .info li p").eq(3).text().strip()
            direction = doc(".imgText .info li p").eq(2).text().strip()
            ReleaseTime = doc(".describe.posFloatMenu .time span").text()
            ReleaseTime = ReleaseTime[:10]
            support = doc(".describe.posFloatMenu .text").text().replace('\n',' ').strip()  
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
            try:
                response = requests.get(url=detail_url, headers=headers, proxies={"http": "http://{}".format(proxy)})
                detail_dict = dict()
                
                doc = pq(response.text)
                city = doc(".currentPage .m_box .pos a").eq(0).text().strip()
                city = city[:-3]  
                unit_price = doc(".price span i").text()
                title = doc("h1").text()
                communityName = doc(".ps .xq p").text()
                area1 = doc(".wz p").eq(0).text().strip()
                if area1 == '蓬江':
                    area1 = '蓬江区'
                elif area1 == '江海':
                    area1 = '江海区'
                elif area1 == '新会':
                    area1 = '新会区'
                elif area1 == '台山':
                    area1 = '台山市'
                elif area1 == '鹤山':
                    area1 = '鹤山市'
                elif area1 == '开平':
                    area1 = '开平市'
                elif area1 == '恩平':
                    area1 = '恩平市'
                else:
                    if len(area1) > 0:
                        area1 += '区'
                area2 = doc(".wz p").eq(1).text().strip()
                area = area1 + area2
                floor = doc(".imgText .info li p").eq(3).text().strip()
                direction = doc(".imgText .info li p").eq(2).text().strip()
                ReleaseTime = doc(".describe.posFloatMenu .time span").text()
                ReleaseTime = ReleaseTime[:10]
                support = doc(".describe.posFloatMenu .text").text().replace('\n',' ').strip()  
                url = detail_url               
                if len(unit_price)>0:
                    detail_dict["city"] = city
                    detail_dict["title"] = title
                    detail_dict["communityName"] = communityName
                    detail_dict["area"] = area
                    detail_dict["price"] = unit_price
                    detail_dict["floor"] = floor
                    detail_dict["direction"] = direction
                    detail_dict["time"] = time
                    detail_dict["support"] = support
                    detail_dict["url"] = url

                    detail_list.append(detail_dict)

                    print(city, unit_price, title, communityName, area, floor, direction, ReleaseTime)
            except:
                print("重试失败")


def save_data(data,filename):
    with open("Lpw_"+filename+"_ershou.json", 'w', encoding="utf-8") as f:
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
            try:
                cursor.executemany(inesrt_re, [value])
                db.commit()
            except:
                print("插入失败")
                continue
                            
    print('存入数据库成功！...')
    db.close() 

def main():
    city_list = ['dg']#['zh','gz','sz','fs','dg','zs','huizhou','jm','zhaoqing']
    for city in city_list:
        page_url_list = get_list_page_url(city)

        p = ThreadPoolExecutor(30)

        for page_url in page_url_list:
            p.submit(get_detail_page_url, page_url, city).add_done_callback(detail_page_parser)

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