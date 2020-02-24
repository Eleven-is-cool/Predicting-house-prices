# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 16:00:14 2019

@author: Administrator
"""

import pymssql
import time
import matplotlib.pyplot as plt

def SQL(city):
    #sql 
    global dic_data
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    if not cursor:
        raise(NameError,"连接数据库失败")
    #else:
    #    print('OK')
       
    #price = []
    #region = []
    dic_data = dict()
    find="select price,area from ershoufang where city='" + city + "'" 
    cursor.execute(find)
    row = cursor.fetchone()
    if row:
        while(row):  
            #print(row[0])
            #price.append(int(float(row[0])))
            #region.append(row[1])
            #print(row[0])
            if row[1]==None:
                continue
            if row[1] != '' and row[1][1] == '区':
                try:
                    dic_data[row[1][:2]].append(int(float(row[0])))
                except:
                    dic_data[row[1][:2]] = [int(float(row[0]))]
            else:
                try:
                    dic_data[row[1][:3]].append(int(float(row[0])))
                except:
                    dic_data[row[1][:3]] = [int(float(row[0]))]

            row = cursor.fetchone()         
    #print(price)
    db.close()
    
def split_data():
    global region_data
    region_data = dict()
    for region in dic_data.keys():
        if len(region)==0:
            continue
        # 最大值、最小值、平均值
        region_data[region] = {"max":dic_data[region][0],"min":dic_data[region][0],"average":0}
        for per_price in dic_data[region]:
            if per_price > region_data[region]["max"]:
                region_data[region]["max"] = per_price
            if per_price < region_data[region]["min"]:
                region_data[region]["min"] = per_price
            region_data[region]["average"] += per_price
        region_data[region]["average"] /= len(dic_data[region])
        # 保留两位小数
        region_data[region]["average"] = int(region_data[region]["average"])
    print(region_data)
    
def data_viewer():
    label_list = region_data.keys()  # 横坐标刻度显示值
    max = []
    min = []
    average = []
    for label in label_list:
        max.append(region_data[label].get("max"))
        min.append(region_data[label].get("min"))
        average.append(region_data[label].get("average"))
    x = range(0,len(max)*5,5)
    
    """
    绘制条形图
    left: 长条形中点横坐标
    height: 长条形高度
    width: 长条形宽度，默认值0.8
    label: 为后面设置legend准备
    """
    plt.rcParams['font.sans-serif']=['SimHei']#显示中文
    rects1 = plt.bar(x=x, height=max, width=1.5, alpha=0.8, color='red', label='最大值')
    rects2 = plt.bar(x=[i + 1.5 for i in x], height=average, width=1.5, color='green', label='平均值')
    rects3 = plt.bar(x=[i + 3.0 for i in x], height=min, width=1.5, color='blue', label='最小值')
    #plt.ylim(0, 50) # y轴取值范围
    plt.ylabel('元/平米')
    """
    设置x轴刻度显示值
    参数一：中点坐标
    参数二：显示值
    """
    plt.xticks([index + 1.5 for index in x], label_list)
    plt.xlabel("地区")
    plt.legend()
    for rect in rects1:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2, height+1, str(height), ha="center", va="bottom",fontsize=8)
    for rect in rects2:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2, height + 1, str(height), ha="center", va="bottom",fontsize=8)
    for rect in rects3:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2, height + 1, str(height), ha="center", va="bottom",fontsize=8)

    plt.savefig('b.png')
    plt.show()


def main():
    cities = ['珠海','广州','深圳','佛山','东莞','中山','惠州','江门']#,'肇庆'
    for city in cities:
        SQL(city)
        split_data()
        data_viewer()
    
if __name__ == '__main__':
    old = time.time()
    main()
    new  = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))