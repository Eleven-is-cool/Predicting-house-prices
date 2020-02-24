# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 20:22:07 2019

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
    start_url = "https://{}.qfang.com/sale".format(city)
    
    session = requests.Session()
    #以下所有的cookie每次程序运行前都得更新
    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
         'cookie':'cookieId=1195501d-5c79-4c68-9818-a9a936993abb; qchatid=fb271883-0734-4ed0-adb9-900a4de834b6; CITY_NAME=DONGGUAN; Hm_lvt_de678bd934b065f76f05705d4e7b662c=1574069730,1574071562,1574071598,1574088619; _jzqa=1.3708410177663412700.1568432715.1574074069.1574088640.16; _jzqy=1.1568432715.1568630555.2.jzqsr=baidu|jzqct=%E5%A6%82%E4%BD%95%E6%9F%A5%E7%9C%8B%E7%8F%A0%E6%B5%B7%E7%9A%84%E6%88%BF%E6%BA%90.jzqsr=baidu|jzqct=%E7%8F%A0%E6%B5%B7%E5%B8%82%E6%88%BF%E5%9C%B0%E4%BA%A7%E4%BA%A4%E6%98%93%E4%B8%AD%E5%BF%83; _jzqx=1.1570441200.1574088640.5.jzqsr=zhuhai%2Eqfang%2Ecom|jzqct=/.jzqsr=dongguan%2Eqfang%2Ecom|jzqct=/; sid=f01810b9-2f9b-403a-84b9-4804adeef314; _jzqckmp=1; _ga=GA1.2.1962689464.1574069730; _gid=GA1.2.1206227590.1574069730; language=SIMPLIFIED; _dc_gtm_UA-47416713-1=1; Hm_lpvt_de678bd934b065f76f05705d4e7b662c=1574088648; LXB_REFER=sp0.baidu.com; sec_tc=AQAAAA5MsC2WqgIADnaKdEKHQxdKY+vg; acw_tc=0e1d391615740886271748061ee11aa51467b3c12c74c0f1c4dca52ca0; JSESSIONID=aaaQF0mnuX7ZtcdHujQ5w; Hm_lvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088628; Hm_lpvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088647; WINDOW_DEVICE_PIXEL_RATIO=1.25; _ga=GA1.3.1962689464.1574069730; _gid=GA1.3.1206227590.1574069730; acw_sc__v2=5dd2afb6c15f647b8e8ad8bd82d015d23e8209e4; _jzqb=1.2.10.1574088640.1; _jzqc=1; _qzja=1.1625897367.1574088640122.1574088640122.1574088640123.1574088640123.1574088648822.0.0.0.2.1; _qzjb=1.1574088640122.2.0.0.0; _qzjc=1; _qzjto=2.1.0' ,
        'Referer': 'https://www.qfang.com/index.html#BDPZ'
    }
    try:
        
        response = session.get(start_url, headers=headers)
        #time.sleep(10)
        doc = pq(response.text)
        #print(doc)        
        total_num =  int(doc(".count").text())
        total_page = total_num // 30 + 1
        if total_page > 100:
            total_page = 100
        page_url_list = list()
        #print(total_page)
        for i in range(total_page):
            url = start_url + "/f" + str(i + 1) #+ "/"
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
    pre_url = "https://{}.qfang.com".format(city)
    global detail_list

    headers =  {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'https://www.qfang.com/index.html#BDPZ',
        'cookie':'cookieId=1195501d-5c79-4c68-9818-a9a936993abb; qchatid=fb271883-0734-4ed0-adb9-900a4de834b6; CITY_NAME=DONGGUAN; Hm_lvt_de678bd934b065f76f05705d4e7b662c=1574069730,1574071562,1574071598,1574088619; _jzqa=1.3708410177663412700.1568432715.1574074069.1574088640.16; _jzqy=1.1568432715.1568630555.2.jzqsr=baidu|jzqct=%E5%A6%82%E4%BD%95%E6%9F%A5%E7%9C%8B%E7%8F%A0%E6%B5%B7%E7%9A%84%E6%88%BF%E6%BA%90.jzqsr=baidu|jzqct=%E7%8F%A0%E6%B5%B7%E5%B8%82%E6%88%BF%E5%9C%B0%E4%BA%A7%E4%BA%A4%E6%98%93%E4%B8%AD%E5%BF%83; _jzqx=1.1570441200.1574088640.5.jzqsr=zhuhai%2Eqfang%2Ecom|jzqct=/.jzqsr=dongguan%2Eqfang%2Ecom|jzqct=/; sid=f01810b9-2f9b-403a-84b9-4804adeef314; _jzqckmp=1; _ga=GA1.2.1962689464.1574069730; _gid=GA1.2.1206227590.1574069730; language=SIMPLIFIED; Hm_lpvt_de678bd934b065f76f05705d4e7b662c=1574088700; LXB_REFER=sp0.baidu.com; sec_tc=AQAAAA5MsC2WqgIADnaKdEKHQxdKY+vg; acw_tc=0e1d391615740886271748061ee11aa51467b3c12c74c0f1c4dca52ca0; JSESSIONID=aaaQF0mnuX7ZtcdHujQ5w; Hm_lvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088628; Hm_lpvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088699; WINDOW_DEVICE_PIXEL_RATIO=1.25; _ga=GA1.3.1962689464.1574069730; _gid=GA1.3.1206227590.1574069730; acw_sc__v2=5dd2afb6c15f647b8e8ad8bd82d015d23e8209e4; _jzqb=1.4.10.1574088640.1; _jzqc=1; _qzja=1.1625897367.1574088640122.1574088640122.1574088640123.1574088661428.1574088700315.0.0.0.4.1; _qzjb=1.1574088640122.4.0.0.0; _qzjc=1; _qzjto=4.1.0; SALEROOMREADRECORDCOOKIE=100840040; looks=SALE%2C100840040%2C3512603; _dc_gtm_UA-47416713-1=1; _gat_UA-47416713-1=1'
    }
    session = requests.Session()
    try:
        response = session.get(page_url,headers=headers,timeout=8)
        #time.sleep(10)
        doc = pq(response.text)  
        i = 0
        detail_urls = list()
        for item in doc(".main-left .list-result li").items():
            
            i += 1
            if i == 31:
                break
            child_item = item(".photo-wrap a")
            if child_item == None:
                i -= 1
            detail_url = child_item.attr("href")
            detail_urls.append(pre_url + detail_url)
            #print(pre_url + detail_url)
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
        'Referer': 'https://guangzhou.qfang.com',
        'cookie':'cookieId=1195501d-5c79-4c68-9818-a9a936993abb; qchatid=fb271883-0734-4ed0-adb9-900a4de834b6; CITY_NAME=DONGGUAN; Hm_lvt_de678bd934b065f76f05705d4e7b662c=1574069730,1574071562,1574071598,1574088619; _jzqa=1.3708410177663412700.1568432715.1574074069.1574088640.16; _jzqy=1.1568432715.1568630555.2.jzqsr=baidu|jzqct=%E5%A6%82%E4%BD%95%E6%9F%A5%E7%9C%8B%E7%8F%A0%E6%B5%B7%E7%9A%84%E6%88%BF%E6%BA%90.jzqsr=baidu|jzqct=%E7%8F%A0%E6%B5%B7%E5%B8%82%E6%88%BF%E5%9C%B0%E4%BA%A7%E4%BA%A4%E6%98%93%E4%B8%AD%E5%BF%83; _jzqx=1.1570441200.1574088640.5.jzqsr=zhuhai%2Eqfang%2Ecom|jzqct=/.jzqsr=dongguan%2Eqfang%2Ecom|jzqct=/; sid=f01810b9-2f9b-403a-84b9-4804adeef314; _jzqckmp=1; _ga=GA1.2.1962689464.1574069730; _gid=GA1.2.1206227590.1574069730; language=SIMPLIFIED; Hm_lpvt_de678bd934b065f76f05705d4e7b662c=1574088700; LXB_REFER=sp0.baidu.com; sec_tc=AQAAAA5MsC2WqgIADnaKdEKHQxdKY+vg; acw_tc=0e1d391615740886271748061ee11aa51467b3c12c74c0f1c4dca52ca0; JSESSIONID=aaaQF0mnuX7ZtcdHujQ5w; Hm_lvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088628; Hm_lpvt_5c4e7d90aac984cc764dc11f5762cb9d=1574088699; WINDOW_DEVICE_PIXEL_RATIO=1.25; _ga=GA1.3.1962689464.1574069730; _gid=GA1.3.1206227590.1574069730; acw_sc__v2=5dd2afb6c15f647b8e8ad8bd82d015d23e8209e4; _jzqb=1.4.10.1574088640.1; _jzqc=1; _qzja=1.1625897367.1574088640122.1574088640122.1574088640123.1574088661428.1574088700315.0.0.0.4.1; _qzjb=1.1574088640122.4.0.0.0; _qzjc=1; _qzjto=4.1.0; SALEROOMREADRECORDCOOKIE=100840040; looks=SALE%2C100840040%2C3512603; _dc_gtm_UA-47416713-1=1; _gat_UA-47416713-1=1'
    }
    session = requests.Session()
    for detail_url in detail_urls:
        try:            
            #print(detail_url)
            response = session.get(url=detail_url, headers=headers,timeout=8)
            #time.sleep(10)
            detail_dict = dict()
            doc = pq(response.text)
            #print(doc)
            city = doc(".crumbs-link.fl .crumbs-link-inner.clearfix a").eq(0).text().strip()
            city = city[:-3]            
            unit_price = doc(".prices .average-price").text()
            unit_price = unit_price[0:unit_price.index("元")]
            title = doc("h2").text()
            communityName = doc(".head-info-list p").eq(0).text()
            area1 = doc(".crumbs-link-inner .crumbs-drop-menu .alink").eq(0).text().strip()
            area1 = area1[:-3]
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
            
            area2 = doc(".crumbs-link-inner .crumbs-drop-menu .alink").eq(1).text().strip()
            area = area1 + area2[:-3]
            
            floor = doc(".house-model .clearfix span").eq(0).text().replace('\n','').strip()  
            direction = doc(".house-model .clearfix p").eq(2).text().replace('\n','').strip()  
            ReleaseTime = doc(".housing-info-con.outer.fl .housing-info-con.inner.fl").eq(7).text().strip() 
            support = doc(".hs-evaluation.hs-evaluation-list .json .clearfix").text().replace('\n','').replace("'","''").strip()                                      
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
                response = session.get(url=detail_url, headers=headers, proxies={"http": "http://{}".format(proxy)})
                #time.sleep(10)
                detail_dict = dict()
                
                doc = pq(response.text)
                city = doc(".crumbs-link.fl .crumbs-link-inner.clearfix a").eq(0).text().strip()
                city = city[:-3]            
                unit_price = doc(".prices .average-price").text()
                unit_price = unit_price[0:unit_price.index("元")]
                title = doc("h2").text()
                communityName = doc(".head-info-list p").eq(0).text().strip()
                area1 = doc(".crumbs-link-inner .crumbs-drop-menu .alink").eq(0).text()
                area1 = area1[:-3]
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
                
                area2 = doc(".crumbs-link-inner .crumbs-drop-menu .alink").eq(1).text()
                area = area1 + area2[:-3]
                
                floor = doc(".house-model .clearfix span").eq(0).text().replace('\n','').strip()  
                direction = doc(".house-model .clearfix p").eq(2).text().replace('\n','').strip()  
                ReleaseTime = doc(".housing-info-con.outer.fl .housing-info-con.inner.fl").eq(7).text().strip()
                support = doc(".hs-evaluation.hs-evaluation-list .json .clearfix").text().replace('\n','').replace("'","''").strip()                                 
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
                print("重试失败")


def save_data(data,filename):
    with open("Qf_"+filename+"_ershou.json", 'w', encoding="utf-8") as f:
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
    city_list = ['dongguan']#['zhuhai','guangzhou','shenzhen','foshan','dongguan','zhongshan','huizhou','jiangmen']
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