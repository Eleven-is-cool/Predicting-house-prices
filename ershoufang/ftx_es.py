# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 18:04:48 2019

@author: Administrator
"""
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from pyquery import PyQuery as pq
import json
import threading
import time
import re


def get_list_page_url(city):
    start_url = "https://{}.esf.fang.com".format(city)
    Agent_all = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.  9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]

    headers =  {
        'User-Agent': random.choice(Agent_all),
#        'Referer': 'https://zh.esf.fang.com/house/i32/?_rfss=bb&rfss=1-c00f4fc0cc2987b8a2-bb'
        
        }
    try:
        response = requests.get(start_url, headers=headers)        
        doc = pq(response.text)
        #print(doc(".shop_list"))
        total_num =  int(doc(".col14 b").eq(1).text())
        
        total_page = total_num // 90 + 1
        if total_page > 100:
            total_page = 100
        page_url_list = list()
        for i in range(total_page):
            url = start_url + "/house/i3" + str(i + 1) + "/"
            
            page_url_list.append(url)
            #print(url)
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
        
def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))
    
def get_detail_page_url(page_url, city):
    pre_url = "https://{}.esf.fang.com".format(city)
    global detail_list
    
    Agent_all = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]
    
    headers =  {
        'Host': 'zh.esf.fang.com',
        'User-Agent':random.choice(Agent_all),
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://zh.esf.fang.com/',
        'Cookie': 'city=zh; global_cookie=snvrecyzp5mxngm7jz9u1d1xk17k0m9qfz3; Integrateactivity=notincludemc; __utma=147393320.1203728920.1570024002.1570024002.1570598164.2; __utmz=147393320.1570598164.2.2.utmcsr=zh.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/; integratecover=1; new_search_uid=23cc5e2c41288094da159c3aca6b32a1; searchConN=1_1570024108_1247%5B%3A%7C%40%7C%3A%5D837d472b02a6031eb708c3fe6ab19387; unique_cookie=U_eew0xqjqgdjyi07eih91ddpnk13k1g6hgdu*5; logGuid=594f26a2-0183-42b2-8030-4d5e3a392bbb; g_sourcepage=ehlist; __utmc=147393320; Captcha=354C7243725932774365356C34457A782F616A533135446369564D483142796E596C52764F596F7737696279347233586766576F7237417838354230414375362F487554444E3935512B593D',
        'Upgrade-Insecure-Requests': '1',
          }
    
    #print(page_url)
    retry_count = 5   
    j = 0
    while retry_count > 0:
        proxy = get_valid_ip().get("proxy")            
        try:           
            response = requests.get(url=page_url, headers=headers,proxies={"http": "http://{}".format(proxy)})
            #print(proxy)
            
            doc = pq(response.text)
            #print(doc)
            i = 0
            detail_urls = list()        
            #print(doc(".main945"))
            for item in doc(".main945 .shop_list .floatl").items():           
                #print('a')
                #print(item)
                #print('b')                   
                i += 1
                if i%20 == 0:
                    time.sleep(60)                  
                if i == 91:
                    break
                child_item = item("a")
                #print(child_item)
                if child_item == None:
                    i -= 1
                detail_url = child_item.attr("href")  
                print(pre_url + detail_url)

                detail_urls.append(pre_url + detail_url)
            return detail_urls
        except:
            j += 1
            print("获取列表页" + page_url + "出错,进行第"+str(j)+"重试")     
            retry_count -= 1
            delete_proxy(proxy)           
                           
            # 出错5次, 删除代理池中代理
    print("获取列表页" + page_url + "出错")  
    
'''
    try:
        response = requests.get(url=page_url,headers=headers)
        
        doc = pq(response.text) 
        #print(doc)
        i = 0
        detail_urls = list()        
        #print(doc(".main945"))
        for item in doc(".main945 .shop_list .floatl").items():           
            #print('a')
            #print(item)
            #print('b')
            i += 1
            if i == 91:
                break
            child_item = item("a")
            #print(child_item)
            if child_item == None:
                i -= 1
            detail_url = child_item.attr("href")  
            #print(pre_url + detail_url)
            detail_urls.append(pre_url + detail_url)
        time.sleep(8)    
        return detail_urls
    except:
        print("获取列表页" + page_url + "出错")
   
'''
lock = threading.Lock()

def detail_page_parser(res):
    global detail_list
    detail_urls = res.result()
    if not detail_urls:
        print("detail url 为空")
        return None
    Agent_all = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]

    headers1 =  {
        #'User-Agent': random.choice(Agent_all),
        #'Referer': 'https://qz.fang.com/',
        #'Host': 'zh.esf.fang.com',
        'User-Agent': random.choice(Agent_all)#,
        #'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        #'Accept-Encoding': 'gzip, deflate, br',
        #'Connection': 'keep-alive',
        #'Referer': 'http://search.fang.com/captcha-8f3e819b3b32843f61/redirect?h=https://zh.esf.fang.com/chushou/3_384911387.htm',
        #'Cookie': 'city=zh; global_cookie=snvrecyzp5mxngm7jz9u1d1xk17k0m9qfz3; Integrateactivity=notincludemc; __utma=147393320.1203728920.1570024002.1570598164.1570670530.3; __utmz=147393320.1570598164.2.2.utmcsr=zh.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/; integratecover=1; new_search_uid=23cc5e2c41288094da159c3aca6b32a1; searchConN=1_1570024108_1247%5B%3A%7C%40%7C%3A%5D837d472b02a6031eb708c3fe6ab19387; unique_cookie=U_eew0xqjqgdjyi07eih91ddpnk13k1g6hgdu*18; logGuid=594f26a2-0183-42b2-8030-4d5e3a392bbb; g_sourcepage=esf_fy%5Exq_pc; __utmc=147393320; Captcha=354C7243725932774365356C34457A782F616A533135446369564D483142796E596C52764F596F7737696279347233586766576F7237417838354230414375362F487554444E3935512B593D; __utmb=147393320.39.10.1570670530; lastscanpage=0; __utmt_t0=1; __utmt_t1=1; __utmt_t2=1',
        #'Upgrade-Insecure-Requests': '1' 
        }
    headers2 = {
            #'Host': 'zh.esf.fang.com',
            'User-Agent': random.choice(Agent_all),
            #'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            #'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            #'Accept-Encoding': 'gzip, deflate, br',
            #'Referer': 'https://zh.esf.fang.com/chushou/3_384182510.htm?channel=1,2&psid=2_4_60&_rfss=5e&rfss=1-078a101141428fb87d-5e',
            #'Connection': 'keep-alive',
            #'Cookie': 'city=zh; global_cookie=snvrecyzp5mxngm7jz9u1d1xk17k0m9qfz3; Integrateactivity=notincludemc; __utma=147393320.1203728920.1570024002.1570670530.1570805773.4; __utmz=147393320.1570598164.2.2.utmcsr=zh.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/; integratecover=1; new_search_uid=23cc5e2c41288094da159c3aca6b32a1; searchConN=1_1570024108_1247%5B%3A%7C%40%7C%3A%5D837d472b02a6031eb708c3fe6ab19387; lastscanpage=0; csrfToken=dyfW6xw-K16mvUN5LWYxsK_-; unique_cookie=U_dkj8urqxh1y7aa2joq2t3pead2jk1m975z8*5; g_sourcepage=esf_fy%5Exq_pc; __utmb=147393320.15.10.1570805773; __utmc=147393320; logGuid=b285ca2c-60d5-462f-86ce-c111c1b5f9e7; __utmt_t0=1; __utmt_t1=1; __utmt_t2=1',
            #'Upgrade-Insecure-Requests': '1',
            #'Cache-Control': 'max-age=0',
            #'TE': 'Trailers'
            }
    i = 0
    for detail_url in detail_urls:
        i += 1
        if i % 10 == 0:
            time.sleep(90)            
        #print(detail_url)
        
        try:
            #print(detail_url)
            #response = requests.get(url=detail_url, headers=headers1, timeout=5)
            #print(response.text)
            #new_url = response.url
            proxy = get_valid_ip().get("proxy")
            #print(proxy)
            doc = requests.get(url=detail_url,headers=headers1,proxies={"http": "http://{}".format(proxy)}).text 
            #print(doc)
            
            time.sleep(8)
            
            detail_dict = dict()
            #doc = pq(response.text)
            #print(doc)
            #print(doc('.redict'))
            #new_url = doc('.redict .btn-redir').attr("href") 
            new_url = detail_url + '?' + re.findall(r't3=\'(.*?)\'', doc)[0] 
            print(new_url)
            #response = requests.get(url=new_url, headers=headers2, timeout=5)
            #doc = requests.get(new_url).text
            response = requests.get(url=new_url, headers=headers2, timeout=5,proxies={"http": "http://{}".format(proxy)})
            #print(response.text)
            
            time.sleep(10)
            
            #doc = pq(response.text)
            #print(doc(".tr-line"))
            doc = pq(response.text)   
                
            unit_price = doc(".tt").eq(2).text().strip()
            unit_price = unit_price[0:unit_price.index("元")]#去掉单位
            title = doc("h1").text()
            area = doc(".bread a").eq(2).text().strip()
            area = area[:-3]
            floor = doc(".tt").eq(4).text().strip()
            
            
            url = detail_url                
            if len(unit_price)>0:
                detail_dict["title"] = title
                detail_dict["area"] = area
                detail_dict["price"] = unit_price
                detail_dict["floor"] = floor
                detail_dict["url"] = url
    
                detail_list.append(detail_dict)
            #time.sleep(30)
                
            print(unit_price, title, area, floor) 
        except:
            print("换ip重试...") 
            try:
                #print(detail_url)
                #response = requests.get(url=detail_url, headers=headers1, timeout=5)
                #print(response.url)
                proxy = get_valid_ip().get("proxy")
                doc = requests.get(url=detail_url, headers=headers1,proxies={"http": "http://{}".format(proxy)}).text 
                #print(proxy)
                time.sleep(10)
                
                detail_dict = dict()
                #doc = pq(response.text)
                new_url = detail_url + '?' + re.findall(r't3=\'(.*?)\'', doc)[0] 
                print(new_url)
                response = requests.get(url=new_url, headers=headers2, timeout=5,proxies={"http": "http://{}".format(proxy)})
                
                time.sleep(10)
                
                doc = pq(response.text) 
                #print(doc)
                #doc = pq(response.text)
                #print(doc)
                    
                unit_price = doc(".tt").eq(2).text().strip()
                unit_price = unit_price[0:unit_price.index("元")]#去掉单位
                title = doc("h1").text()
                area = doc(".bread a").eq(2).text().strip()
                area = area[:-3]
                floor = doc(".tt").eq(4).text().strip()
                url = detail_url                
                if len(unit_price)>0:
                    detail_dict["title"] = title
                    detail_dict["area"] = area
                    detail_dict["price"] = unit_price
                    detail_dict["floor"] = floor
                    detail_dict["url"] = url
        
                    detail_list.append(detail_dict)
                #time.sleep(30)
        
                print(unit_price, title, area, floor)
            except:
                
                delete_proxy(proxy)
                print("换ip重试失败...") 
   
'''        
        try:
            
            response = requests.get(url=detail_url, headers=headers,timeout=10) 
            #response = requests.get(url=response.url, headers=headers,timeout=3)
            #print(detail_url)
            detail_dict = dict()
            doc = pq(response.text)
            new_url = doc('.btn-redir').attr("href") 
            response = requests.get(url=new_url, headers=headers,timeout=10) 
            doc = pq(response.text)
            #print(doc)
            unit_price = doc(".tt").eq(2).text().strip()
            unit_price = unit_price[0:unit_price.index("元")]#去掉单位
            title = doc("h1").text()
            area = doc(".bread a").eq(2).text().strip()
            area = area[:-3]
            url = detail_url
            detail_dict["title"] = title
            detail_dict["area"] = area
            detail_dict["price"] = unit_price
            detail_dict["url"] = url

            detail_list.append(detail_dict)

            print(unit_price, title, area)

        except:
            print("获取详情页出错,换ip重试")
            #print(detail_url)
            proxy = get_valid_ip().get("proxy")
            
            try:
                #print(detail_url)
                response = requests.get(url=detail_url, headers=headers, proxies={"http": "http://{}".format(proxy)})
                detail_dict = dict()
                doc = pq(response.text)
                new_url = doc('.btn-redir').attr("href") 
                response = requests.get(url=new_url, headers=headers,proxies={"http": "http://{}".format(proxy)})
                doc = pq(response.text)
                
                
                unit_price = doc(".tt").eq(2).text().strip()
                unit_price = unit_price[0:unit_price.index("元")]#去掉单位
                title = doc("h1").text()
                area = doc(".bread a").eq(2).text().strip()
                area = area[:-3]
                url = detail_url                
                if len(unit_price)>0:
                    detail_dict["title"] = title
                    detail_dict["area"] = area
                    detail_dict["price"] = unit_price
                    detail_dict["url"] = url
    
                    detail_list.append(detail_dict)
    
                    print(unit_price, title, area)
            except:
                print("重试失败...")              

'''

def save_data(data,filename):
    with open("Ftx_"+filename+"_ershou.json", 'w', encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))

def main():
    city_list =  ['zh']#['zh','gz','sz','fs','dg','zs','huizhou','jm','zhaoqing']
    for city in city_list:
        page_url_list = get_list_page_url(city)



        p = ThreadPoolExecutor(10)

        for page_url in page_url_list:
            p.submit(get_detail_page_url, page_url, city).add_done_callback(detail_page_parser)

        p.shutdown()

        save_data(detail_list, city)

        detail_list.clear()

if __name__ == '__main__':
    old = time.time()
    main()
    new  = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))