import requests
import json
import pymssql
import time
import threading
from concurrent.futures import ThreadPoolExecutor

'''
地铁站 公交站
幼儿园 小学 中学 大学
医院 药店
商场 超市 市场
银行 ATM 餐厅  咖啡馆
公园 电影院 健身房 体育馆
'''
lock = threading.Lock()
def get_sum(ca, latitude, longitude):
    url = "http://api.map.baidu.com/place/v2/search?query={c}&radius_limit=true&page_size=20&location={la},{lo}&radius=1500&output=json&ak=dASz7ubuSpHidP1oQWKuAK3q".format(c=ca, la=latitude, lo=longitude)
    #url = "http://api.map.baidu.com/place/v2/search?query={c}&radius_limit=true&page_size=20&location={la},{lo}&radius=1500&output=json&ak=2GRLlL3GmBC1UroaS6Y6AelW4gEG9piQ".format(c=ca, la=latitude, lo=longitude)
    try:
        res = requests.get(url, timeout=15)
    except:
        return -1
    json_data = json.loads(res.text, strict=False)
    #print(json_data)
    sum = 0
    if json_data['status'] == 0:
        sum = json_data['total']
        '''
        for re in json_data['results']:
            print(re)
            sum += 1
        '''
    else:
        print("Error output!")
        sum = -1
    return sum

def SQL(city):
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    cursor_insert = db.cursor()
    if not cursor:
        raise (NameError, "连接数据库失败")
    subway_stations={}
    bus_stations={}
    kindergartens={}
    primary_schools={}
    middle_schools={}
    the_Universitys={}
    hospitals={}
    pharmacys={}
    the_malls={}
    Supermarkets={}
    markets={}
    banks={}
    ATMs={}
    restaurants={}
    coffee_shops={}
    parks={}
    cinemas={}
    Gyms={}
    stadiums={}

    find_area = "select latitude,longitude,communityName from ershoufang where city='" + city + "'"
    cursor.execute(find_area)
    row = cursor.fetchone()
    category = ['地铁站', '公交站', '幼儿园', '小学', '中学', '大学', '医院', '药店', '商场', '超市', '市场', '银行', 'ATM', '餐厅',  '咖啡馆', '公园', '电影院', '健身房', '体育馆']

    if row:
        while(row):
            if row[2] not in subway_stations:
                for ca in category:
                    print(ca, row[0], row[1], row[2])
                    sum = get_sum(ca, row[0], row[1])
                    print(sum)
                    if sum != -1:
                        if ca=='地铁站':
                            if row[2] in subway_stations:
                                pass
                            else:
                                subway_stations[row[2]] = sum
                        elif ca=='公交站':
                            if row[2] in bus_stations:
                                pass
                            else:
                                bus_stations[row[2]] = sum
                        elif ca=='幼儿园':
                            if row[2] in kindergartens:
                                pass
                            else:
                                kindergartens[row[2]] = sum
                        elif ca=='小学':
                            if row[2] in primary_schools:
                                pass
                            else:
                                primary_schools[row[2]] = sum
                        elif ca=='中学':
                            if row[2] in middle_schools:
                                pass
                            else:
                                middle_schools[row[2]] = sum
                        elif ca=='大学':
                            if row[2] in the_Universitys:
                                pass
                            else:
                                the_Universitys[row[2]] = sum
                        elif ca=='医院':
                            if row[2] in hospitals:
                                pass
                            else:
                                hospitals[row[2]] = sum
                        elif ca=='药店':
                            if row[2] in pharmacys:
                                pass
                            else:
                                pharmacys[row[2]] = sum
                        elif ca=='商场':
                            if row[2] in the_malls:
                                pass
                            else:
                                the_malls[row[2]] = sum
                        elif ca=='超市':
                            if row[2] in Supermarkets:
                                pass
                            else:
                                Supermarkets[row[2]] = sum
                        elif ca=='市场':
                            if row[2] in markets:
                                pass
                            else:
                                markets[row[2]] = sum
                        elif ca=='银行':
                            if row[2] in banks:
                                pass
                            else:
                                banks[row[2]] = sum
                        elif ca=='ATM':
                            if row[2] in ATMs:
                                pass
                            else:
                                ATMs[row[2]] = sum
                        elif ca=='餐厅':
                            if row[2] in restaurants:
                                pass
                            else:
                                restaurants[row[2]] = sum
                        elif ca=='咖啡馆':
                            if row[2] in coffee_shops:
                                pass
                            else:
                                coffee_shops[row[2]] = sum
                        elif ca=='公园':
                            if row[2] in parks:
                                pass
                            else:
                                parks[row[2]] = sum
                        elif ca=='电影院':
                            if row[2] in cinemas:
                                pass
                            else:
                                cinemas[row[2]] = sum
                        elif ca=='健身房':
                            if row[2] in Gyms:
                                pass
                            else:
                                Gyms[row[2]] = sum
                        elif ca=='体育馆':
                            if row[2] in stadiums:
                                pass
                            else:
                                stadiums[row[2]] = sum
            row = cursor.fetchone()
    for name in cinemas:
        inesrt_re = "update ershoufang set cinema_1500=" + str(cinemas[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in subway_stations:
        inesrt_re = "update ershoufang set subway_station_1500=" + str(subway_stations[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in bus_stations:
        inesrt_re = "update ershoufang set bus_station_1500=" + str(bus_stations[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in kindergartens:
        inesrt_re = "update ershoufang set kindergarten_1500=" + str(kindergartens[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in primary_schools:
        inesrt_re = "update ershoufang set primary_school_1500=" + str(primary_schools[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in middle_schools:
        inesrt_re = "update ershoufang set middle_school_1500=" + str(middle_schools[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in the_Universitys:
        inesrt_re = "update ershoufang set the_University_1500=" + str(the_Universitys[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in hospitals:
        inesrt_re = "update ershoufang set hospital_1500=" + str(hospitals[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in pharmacys:
        inesrt_re = "update ershoufang set pharmacy_1500=" + str(pharmacys[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in the_malls:
        inesrt_re = "update ershoufang set the_mall_1500=" + str(the_malls[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in Supermarkets:
        inesrt_re = "update ershoufang set Supermarket_1500=" + str(Supermarkets[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in markets:
        inesrt_re = "update ershoufang set market_1500=" + str(markets[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in banks:
        inesrt_re = "update ershoufang set bank_1500=" + str(banks[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in ATMs:
        inesrt_re = "update ershoufang set ATM_1500=" + str(ATMs[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in restaurants:
        inesrt_re = "update ershoufang set restaurant_1500=" + str(restaurants[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in coffee_shops:
        inesrt_re = "update ershoufang set coffee_shop_1500=" + str(coffee_shops[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in parks:
        inesrt_re = "update ershoufang set park_1500=" + str(parks[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in Gyms:
        inesrt_re = "update ershoufang set Gym_1500=" + str(Gyms[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)
    for name in stadiums:
        inesrt_re = "update ershoufang set stadium_1500=" + str(stadiums[name]) + "  where city='" + city + "' and communityName='" + name + "'"
        print(inesrt_re)
        cursor_insert.execute(inesrt_re)

    db.commit()
    db.close()
    print(city+" is updated successing")


def main():
    cities =['惠州','江门','肇庆']#['广州','深圳','佛山','东莞','中山','惠州','江门','肇庆']#'东莞','珠海',
    p = ThreadPoolExecutor(150)
    for city in cities:
        #SQL(city)
        p.submit(SQL(city))
    p.shutdown()


if __name__ == '__main__':
    old = time.time()
    main()
    new = time.time()
    delta_time = new - old
    print("程序共运行{}s".format(delta_time))
