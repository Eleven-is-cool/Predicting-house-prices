# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 09:47:33 2019

@author: Administrator
"""

import requests
from concurrent.futures import ThreadPoolExecutor
from pyquery import PyQuery as pq
import json
import threading
import time
import pymssql
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
def get_list_page_url(city):
    start_url = "https://{}.lianjia.com/ershoufang".format(city)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    }
    try:
        response = requests.get(start_url, headers=headers)
        doc = pq(response.text)
        total_num = int(doc(".resultDes .total span").text())
        total_page = total_num // 30 + 1
        if total_page > 100:
            total_page = 100
        page_url_list = list()
        for i in range(total_page):
            url = start_url + "/pg" + str(i + 1) + "/"
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        'Referer': 'https://bj.lianjia.com/ershoufang'
    }
    try:
        response = requests.get(page_url, headers=headers, timeout=3)
        doc = pq(response.text)
        i = 0
        detail_urls = list()
        for item in doc(".sellListContent li").items():
            i += 1
            if i == 31:
                break
            child_item = item(".noresultRecommend")
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
    for detail_url in detail_urls:
        try:
            chrome_options = Options()
            chrome_options.add_argument('window-size=1920x3000') #指定浏览器分辨率
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu') #谷歌文档提到需要加上这个属性来规避bug
            chrome_options.add_argument('blink-settings=imagesEnabled=false') #不加载图片, 提升速度
            driver = webdriver.Chrome(chrome_options=chrome_options)
            #driver.set_page_load_timeout(15)
            #old = time.time()
            try:
                driver.get(detail_url)
            except TimeoutException:
                driver.execute_script('window.stop()')
            response = driver.page_source
            doc = pq(response)
            #new = time.time()
            #print(new-old)
            # response = requests.get(url=detail_url, headers=headers, timeout=3)
            detail_dict = dict()
            # doc = pq(response.text)
            city = doc(".container .fl a").eq(0).text().strip()
            city = city[:-3]
            unit_price = doc(".unitPriceValue").text()
            unit_price = unit_price[0:unit_price.index("元")]  # 去掉单位
            title = doc("h1").text()
            communityName = doc(".communityName .info").text()
            area1 = doc(".areaName .info a").eq(0).text().strip()
            area2 = doc(".areaName .info a").eq(1).text().strip()
            area = area1 + area2
            floor = doc(".room .subInfo").text()
            direction = doc(".type .mainInfo").text()
            ReleaseTime = doc(".introContent .transaction .content li").eq(0).text().replace('\n', '').strip()
            ReleaseTime = ReleaseTime[4:]
            # support = doc(".introContent.showbasemore .baseattribute.clear").text().strip().replace('\n', ' ')
            target = driver.find_element_by_xpath('//*[@id="around"]/div/div[2]/ul/li[6]')
            driver.execute_script("return arguments[0].scrollIntoView();", target)
            first_rows = driver.find_elements_by_css_selector(".aroundType li")
            #print(res)
            all_nums = []  # 存储每一个分类的数量
            old = time.time()
            for first_row in first_rows:#遍历最上面的分类
                # time.sleep(5)
                #print(first_row)
                first_row.click()
                #ActionChains(driver).move_to_element(first_row).click().perform()
                time.sleep(1)
                second_rows = driver.find_elements_by_css_selector(".tagStyle.LOGCLICK")
                no_click = 0#设置一个变量去跳过点击第一个分类，因为点击第一个分类的话会加载更多时间，导致页面来不及刷新
                for second_row in second_rows:#遍历下面的分类
                    no_click += 1
                    num = 0
                    if no_click == 1:
                        pass
                    else:
                        #ActionChains(driver).move_to_element(second_row).click().perform()
                        second_row.click()
                    time.sleep(1)
                    html = driver.page_source
                    h = pq(html)
                    #print(h(".aroundContainer .aroundList").text())
                    k = h(".aroundContainer .aroundList .itemContent .itemText.itemTitle").text()
                    if k[:3] == '很抱歉':#无信息
                        num = 0
                    else:
                        results = driver.find_elements_by_css_selector(".aroundContainer .aroundList .itemContent .itemText.itemTitle")
                        for res in results:#遍历每一条信息，每次加一
                            num += 1
                    all_nums.append(num)
            new = time.time()
            driver.quit()

            url = detail_url
            detail_dict["city"] = city
            detail_dict["title"] = title
            detail_dict["communityName"] = communityName
            detail_dict["area"] = area
            detail_dict["price"] = unit_price
            detail_dict["floor"] = floor
            detail_dict["direction"] = direction
            detail_dict["ReleaseTime"] = ReleaseTime
            detail_dict["subway_station"] = all_nums[0]
            detail_dict["bus_station"] = all_nums[1]
            detail_dict["kindergarten"] = all_nums[2]
            detail_dict["primary_school"] = all_nums[3]
            detail_dict["middle_school"] = all_nums[4]
            detail_dict["the_University"] = all_nums[5]
            detail_dict["hospital"] = all_nums[6]
            detail_dict["pharmacy"] = all_nums[7]
            detail_dict["the_mall"] = all_nums[8]
            detail_dict["Supermarket"] = all_nums[9]
            detail_dict["market"] = all_nums[10]
            detail_dict["bank"] = all_nums[11]
            detail_dict["ATM"] = all_nums[12]
            detail_dict["restaurant"] = all_nums[13]
            detail_dict["coffee_shop"] = all_nums[14]
            detail_dict["park"] = all_nums[15]
            detail_dict["cinema"] = all_nums[16]
            detail_dict["Gym"] = all_nums[17]
            detail_dict["stadium"] = all_nums[18]
            #detail_dict["support"] = support
            detail_dict["url"] = url
            detail_list.append(detail_dict)
            print(city, unit_price, title, communityName, area, floor, direction, ReleaseTime, url, all_nums,new-old)

        except:
            print("获取详情页出错,换ip重试")
            proxy = get_valid_ip().get("proxy")
            # proxies = {
            #    "http": "http://" + get_valid_ip(),
            # }
            try:
                #response = requests.get(url=detail_url, headers=headers, proxies={"http": "http://{}".format(proxy)})
                chrome_options = Options()
                chrome_options.add_argument("--proxy-server=http://{}".format(proxy))#代理
                chrome_options.add_argument('window-size=1920x3000')  # 指定浏览器分辨率
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
                chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片, 提升速度
                driver = webdriver.Chrome(chrome_options=chrome_options)
                #driver.set_page_load_timeout(15)
                try:
                    driver.get(detail_url)
                except TimeoutException:
                    driver.execute_script('window.stop()')
                response = driver.page_source
                doc = pq(response)
                # response = requests.get(url=detail_url, headers=headers, timeout=3)
                detail_dict = dict()
                city = doc(".container .fl a").eq(0).text().strip()
                city = city[:-3]
                unit_price = doc(".unitPriceValue").text()
                unit_price = unit_price[0:unit_price.index("元")]  # 去掉单位
                title = doc("h1").text()
                communityName = doc(".communityName .info").text()
                area1 = doc(".areaName .info a").eq(0).text().strip()
                area2 = doc(".areaName .info a").eq(1).text().strip()
                area = area1 + area2
                floor = doc(".room .subInfo").text()
                direction = doc(".type .mainInfo").text()
                ReleaseTime = doc(".introContent .transaction .content li").eq(0).text().replace('\n', '').strip()
                ReleaseTime = ReleaseTime[4:]
                # support = doc(".introContent.showbasemore .baseattribute.clear").text().strip().replace('\n', ' ')
                target = driver.find_element_by_xpath('//*[@id="around"]/div/div[2]/ul/li[6]')
                driver.execute_script("return arguments[0].scrollIntoView();", target)
                first_rows = driver.find_elements_by_css_selector(".aroundType li")
                # print(res)
                all_nums = []  # 存储每一个分类的数量
                old = time.time()
                for first_row in first_rows:  # 遍历最上面的分类
                    #print(first_row)
                    ActionChains(driver).move_to_element(first_row).click().perform()
                    time.sleep(1)
                    second_rows = driver.find_elements_by_css_selector(".tagStyle.LOGCLICK")
                    no_click = 0  # 设置一个变量去跳过点击第一个分类，因为点击第一个分类的话会加载更多时间，导致页面来不及刷新
                    for second_row in second_rows:  # 遍历下面的分类
                        no_click += 1
                        num = 0
                        if no_click == 1:
                            pass
                        else:
                            ActionChains(driver).move_to_element(second_row).click().perform()
                        time.sleep(1)
                        html = driver.page_source
                        h = pq(html)
                        #print(h(".aroundContainer .aroundList").text())
                        k = h(".aroundContainer .aroundList .itemContent .itemText.itemTitle").text()
                        if k[:3] == '很抱歉':  # 无信息
                            num = 0
                        else:
                            results = driver.find_elements_by_css_selector(
                                ".aroundContainer .aroundList .itemContent .itemText.itemTitle")
                            for res in results:  # 遍历每一条信息，每次加一
                                num += 1
                        all_nums.append(num)
                new = time.time()
                driver.quit()

                url = detail_url
                detail_dict["city"] = city
                detail_dict["title"] = title
                detail_dict["communityName"] = communityName
                detail_dict["area"] = area
                detail_dict["price"] = unit_price
                detail_dict["floor"] = floor
                detail_dict["direction"] = direction
                detail_dict["ReleaseTime"] = ReleaseTime
                detail_dict["subway_station"] = all_nums[0]
                detail_dict["bus_station"] = all_nums[1]
                detail_dict["kindergarten"] = all_nums[2]
                detail_dict["primary_school"] = all_nums[3]
                detail_dict["middle_school"] = all_nums[4]
                detail_dict["the_University"] = all_nums[5]
                detail_dict["hospital"] = all_nums[6]
                detail_dict["pharmacy"] = all_nums[7]
                detail_dict["the_mall"] = all_nums[8]
                detail_dict["Supermarket"] = all_nums[9]
                detail_dict["market"] = all_nums[10]
                detail_dict["bank"] = all_nums[11]
                detail_dict["ATM"] = all_nums[12]
                detail_dict["restaurant"] = all_nums[13]
                detail_dict["coffee_shop"] = all_nums[14]
                detail_dict["park"] = all_nums[15]
                detail_dict["cinema"] = all_nums[16]
                detail_dict["Gym"] = all_nums[17]
                detail_dict["stadium"] = all_nums[18]
                # detail_dict["support"] = support
                detail_dict["url"] = url
                detail_list.append(detail_dict)
                print(city, unit_price, title, communityName, area, floor, direction, ReleaseTime, url, all_nums, new-old)
            except:
                print("重试失败...")


def save_data(data, filename):
    with open("Lj_" + filename + "_ershou.json", 'w', encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))


def savetoSQL(detail_list):
    # sql
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    if not cursor:
        raise (NameError, "连接数据库失败")
    else:
        print('OK')

    sql = """CREATE TABLE SecondHandHouse(
    city VARCHAR(200),
    title VARCHAR(200),
    communityName VARCHAR(200),
    area VARCHAR(200),
    price VARCHAR(200),
    floor VARCHAR(200),  
    direction VARCHAR(200), 
    time VARCHAR(200), 
    subway_station VARCHAR(200),  
    bus_station VARCHAR(200), 
    kindergarten VARCHAR(200), 
    primary_school VARCHAR(200), 
    middle_school VARCHAR(200), 
    the_University VARCHAR(200), 
    hospital VARCHAR(200), 
    pharmacy VARCHAR(200), 
    the_mall VARCHAR(200), 
    Supermarket VARCHAR(200), 
    market VARCHAR(200), 
    bank VARCHAR(200), 
    ATM VARCHAR(200), 
    restaurant VARCHAR(200), 
    coffee_shop VARCHAR(200), 
    park VARCHAR(200), 
    cinema VARCHAR(200), 
    Gym VARCHAR(200), 
    stadium VARCHAR(200),              
    url VARCHAR(200))"""
    try:
        cursor.execute("select * from SecondHandHouse")
    except:
        cursor.execute(sql)
    for data in detail_list:
        find = "select * from SecondHandHouse where title='" + data["title"] + "' and " + "price='" + data[
            "price"] + "' and area='" + data["area"] + "' and floor='" + data["floor"] + "'"
        cursor.execute(find)
        row = cursor.fetchone()
        if row:
            continue
        else:
            value = tuple(data.values())
            inesrt_re = "INSERT INTO SecondHandHouse VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(inesrt_re, [value])
            db.commit()

    print('存入数据库成功！...')
    db.close()


def main():
    city_list = ['fs','zh','gz','sz','dg','zs','hui','jiangmen']
    for city in city_list:
        page_url_list = get_list_page_url(city)

        p = ThreadPoolExecutor()

        for page_url in page_url_list:
            p.submit(get_detail_page_url, page_url).add_done_callback(detail_page_parser)

        p.shutdown()

        # save_data(detail_list, city)
        savetoSQL(detail_list)
        detail_list.clear()


if __name__ == '__main__':
    old = time.time()
    main()
    new = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))
