import requests
import json
import pymssql
import time
import threading
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()

def get_LatLng(address, city):

    url = "http://api.map.baidu.com/geocoding/v3/?address=" + address + "&city=" + city + "&output=json&ak=2GRLlL3GmBC1UroaS6Y6AelW4gEG9piQ"
    res = requests.get(url, timeout=15)
    json_data = json.loads(res.text)
    print(json_data)
    if json_data['status'] == 0:
        la = json_data['result']['location']['lat']  # 纬度
        ln = json_data['result']['location']['lng']  # 经度
    else:
        print("Error output!")
        la = 0
        ln = 0
    return la, ln


def SQL(city):
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    cursor_insert = db.cursor()
    #cursor_create = db.cursor()
    if not cursor:
        raise (NameError, "连接数据库失败")
    lats = {}
    lngs = {}
    '''
    create1="alter table ershoufang add longitude nvarchar(200)"
    create2="alter table ershoufang add latitude nvarchar(200)"
    cursor_create.execute(create1)
    cursor_create.execute(create2)
    db.commit()
    '''
    find_area = "select communityName from ershoufang where city='" + city + "'"
    cursor.execute(find_area)
    row = cursor.fetchone()
    if row:
        while(row):
            lat, lng = get_LatLng(row[0], city)
            if lat != 0 and lng != 0:
                #print(lat, lng)
                if row[0] in lats:
                    pass
                else:
                    lats[row[0]] = lat
                    lngs[row[0]] = lng
            row = cursor.fetchone()
    for name in lats:
        inesrt_re = "update ershoufang set longitude='" + str(lngs[name]) + "',latitude='" + str(lats[name]) + "' where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    print(lats)
    print(lngs)
    db.commit()
    db.close()
    print(city+" is updated successing")


def main():
    cities =['深圳','佛山','东莞','中山','惠州','江门','肇庆']#['珠海','广州','深圳','佛山','东莞','中山','惠州','江门','肇庆']
    p = ThreadPoolExecutor(30)
    for city in cities:
        p.submit(SQL(city))
    p.shutdown()


if __name__ == '__main__':
    old = time.time()
    main()
    new = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))