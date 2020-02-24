import pymssql
import time
import threading
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()

def SQL(city):
    # sql
    global dic_data
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    if not cursor:
        raise (NameError, "连接数据库失败")
    if city == '珠海':
        update1 = "update ershoufang set area='金湾区' where city = '珠海' and area like '高栏港经济区%'"
        cursor.execute(update1)
        update2 = "update ershoufang set area='香洲区横琴' where city = '珠海' and area like '横琴%'"
        cursor.execute(update2)
        delete1 = "delete from ershoufang where city='珠海' and area like '坦洲%'"
        cursor.execute(delete1)
        db.commit()
    if city == '广州' or city == '深圳' or city == '佛山':
        find_area = "select area from ershoufang where city='"+ city +"'"
        cursor.execute(find_area)
        row = cursor.fetchone()
        areas=[]
        if row:
            while(row):
                # print(row[0])
                # price.append(int(float(row[0])))
                # region.append(row[1])
                #print(row[0]
                if row[0] != '':
                    if row[0][2] != '区':
                        areas.append(row[0])
                row = cursor.fetchone()

        #print(areas)
        for a in areas:
            new_area = a[:2] + '区' + a[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city  + "'" +" and area='" + a + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
    if city == '佛山':
        delete2 = "delete from ershoufang where city='佛山' and area like '白云%'"
        cursor.execute(delete2)
        delete2 = "delete from ershoufang where city='佛山' and area like '番禺%'"
        cursor.execute(delete2)
        db.commit()
    if city == '中山':
        delete = "delete from ershoufang where city='中山' and area like '香洲%'"
        cursor.execute(delete)
        db.commit()
        find_area = "select area from ershoufang where city='" + city + "'"
        cursor.execute(find_area)
        row = cursor.fetchone()
        areas_zheng = []
        areas_qu_1 = []
        areas_qu_2 = []

        if row:
            while (row):
                # print(row[0])
                # price.append(int(float(row[0])))
                # region.append(row[1])
                # print(row[0]
                if row[0] != '':
                    if row[0][2] == '镇' or row[0][1] == '区' or row[0][2] == '区':
                        print('pass')
                    elif row[0][:2] == '板芙' or row[0][:2] == '大涌' or row[0][:2] == '东凤' or row[0][:2] == '东升' or row[0][:2] == '阜沙' or row[0][:2] == '港口' or row[0][:2] == '古镇' or row[0][:2] == '横栏' or row[0][:2] == '黄圃' or row[0][:2] == '民众' or row[0][:2] == '南朗' or row[0][:2] == '南头' or row[0][:2] == '三角' or row[0][:2] == '三乡' or row[0][:2] == '沙溪' or row[0][:2] == '神湾' or row[0][:2] == '坦洲' or row[0][:2] == '小榄':
                        areas_zheng.append(row[0])
                    elif row[0][0] == '西' or row[0][0] == '东' or row[0][0] == '南':
                        areas_qu_1.append(row[0])
                    elif row[0][:2] == '火炬' or row[0][:2] == '石岐':
                        areas_qu_2.append(row[0])
                row = cursor.fetchone()
        for a1 in areas_zheng:
            new_area = a1[:2] + '镇' + a1[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a1 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
        for a2 in areas_qu_1:
            new_area = a2[:1] + '区' + a2[1:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a2 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
        for a3 in areas_qu_2:
            new_area = a3[:2] + '区' + a3[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a3 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
    if city == '东莞':
        delete = "delete ershoufang where city = '东莞'and url like '%loupan%'"
        cursor.execute(delete)
        db.commit()
        find_area = "select area from ershoufang where city='" + city + "'"
        cursor.execute(find_area)
        row = cursor.fetchone()
        areas_zheng_2 = []
        areas_zheng_3 = []
        areas_qu = []

        if row:
            while (row):
                if row[0] != '':
                    if row[0][3] != '镇' and (row[0][:3]=='樟木头' or row[0][:3]=='大岭山' or row[0][:3]=='望牛墩'):
                        areas_zheng_3.append(row[0])
                    elif row[0][2] !='区' and (row[0][:2]=='莞城' or row[0][:2]=='南城' or row[0][:2]=='万江' or row[0][:2]=='东城'):
                        areas_qu.append(row[0])
                    elif row[0][2] != '镇' and (row[0][:2]=='石碣' or row[0][:2]=='石龙' or row[0][:2]=='茶山' or row[0][:2]=='石排' or row[0][:2]=='企石' or row[0][:2]=='横沥' or row[0][:2]=='桥头' or row[0][:2]=='谢岗' or row[0][:2]=='东坑' or row[0][:2]=='常平' or row[0][:2]=='寮步' or row[0][:2]=='大朗' or row[0][:2]=='黄江' or row[0][:2]=='清溪' or row[0][:2]=='塘厦' or row[0][:2]=='凤岗' or row[0][:2]=='长安' or row[0][:2]=='虎门' or row[0][:2]=='厚街' or row[0][:2]=='沙田' or row[0][:2]=='道窖' or row[0][:2]=='洪梅' or row[0][:2]=='麻涌' or row[0][:2]=='中堂' or row[0][:2]=='高步'):
                        areas_zheng_2.append(row[0])

                row = cursor.fetchone()
        for a1 in areas_zheng_2:
            new_area = a1[:2] + '镇' + a1[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a1 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()

        for a2 in areas_zheng_3:
            new_area = a2[:3] + '镇' + a2[3:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a2 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit() 

        for a3 in areas_qu:
            new_area = a3[:2] + '区' + a3[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city + "'" + " and area='" + a3 + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()

        print(city + 'clean successfully')
        db.close()
    if city == '惠州':
        delete = "delete ershoufang where city = '惠州'and url like '%loupan%'"
        cursor.execute(delete)
        db.commit()
        find_area = "select area from ershoufang where city='"+ city +"'"
        cursor.execute(find_area)
        row = cursor.fetchone()
        areas_qu=[]
        areas_xian = []
        if row:
            while(row):
                # print(row[0])
                # price.append(int(float(row[0])))
                # region.append(row[1])
                #print(row[0]
                if row[0] != '':
                    if len(row[0]) == 2 and (row[0][:2] == '惠阳' or row[0][:2] == '惠城' or row[0][:2] == '惠东'):
                        areas_qu.append(row[0])
                    elif len(row[0]) == 2 and (row[0][:2] == '博罗' or row[0][:2] == '龙门'):
                        areas_xian.append(row[0])
                    elif (row[0][2] != '区') and (row[0][:2] == '惠阳' or row[0][:2] == '惠城' or row[0][:2] == '惠东'):
                        areas_qu.append(row[0])
                    elif (row[0][2] != '县') and (row[0][:2] == '博罗' or row[0][:2] == '龙门'):
                        areas_xian.append(row[0])
                row = cursor.fetchone()
        for a in areas_qu:
            new_area = a[:2] + '区' + a[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city  + "'" +" and area='" + a + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
        for b in areas_xian:
            new_area = b[:2] + '县' + b[2:]
            update_area = "update ershoufang set area='" + new_area + "' where city ='" + city  + "'" +" and area='" + b + "'"
            print(update_area)
            cursor.execute(update_area)
            db.commit()
        print(city + 'clean successfully')
        db.close()

def main():
    cities = ['惠州']#['珠海','广州','深圳','佛山','中山','东莞']  # ['珠海','广州','深圳','佛山','东莞','中山','惠州','江门']#,'肇庆']
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