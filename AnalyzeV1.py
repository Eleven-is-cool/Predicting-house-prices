import pymssql
import pandas as pd
import time
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
import seaborn as sns
import numpy
from sklearn.preprocessing import StandardScaler
#使用python连接数据库
def get_message():
    server = "127.0.0.1"
    user = "sa"
    password = "123123"
    database = "house"
    db = pymssql.connect(server, user, password, database)
    cursor = db.cursor()
    if not cursor:
        raise (NameError, "连接数据库失败")

    #sql = "select distinct communityName,cinema,subway_station,bus_station,kindergarten,primary_school,middle_school,the_University,hospital,pharmacy,the_mall,Supermarket,market,bank,ATM,restaurant,coffee_shop,park,Gym,stadium from ershoufang where city='中山'"
    #sql = "select distinct communityName,cinema_500,subway_station_500,bus_station_500,kindergarten_500,primary_school_500,middle_school_500,the_University_500,hospital_500,pharmacy_500,the_mall_500,Supermarket_500,market_500,bank_500,ATM_500,restaurant_500,coffee_shop_500,park_500,Gym_500,stadium_500 from ershoufang where city='江门'"
    sql = "select distinct communityName,cinema_1500,subway_station_1500,bus_station_1500,kindergarten_1500,primary_school_1500,middle_school_1500,the_University_1500,hospital_1500,pharmacy_1500,the_mall_1500,Supermarket_1500,market_1500,bank_1500,ATM_1500,restaurant_1500,coffee_shop_1500,park_1500,Gym_1500,stadium_1500 from ershoufang where city='广州'"
    data = pd.read_sql(sql, db)
    global communityName,cinema,subway_station,bus_station,kindergarten,primary_school,middle_school,the_University, hospital, pharmacy, the_mall, Supermarket, market, bank, ATM, restaurant, coffee_shop, park, Gym, stadium, prices
    communityName = list(data.communityName)

    # cinema = list(map(int, list(data.cinema)))
    # subway_station = list(map(int, list(data.subway_station)))
    # bus_station = list(map(int, list(data.bus_station)))
    # kindergarten = list(map(int, list(data.kindergarten)))
    # primary_school = list(map(int, list(data.primary_school)))
    # middle_school = list(map(int, list(data.middle_school)))
    # the_University = list(map(int, list(data.the_University)))
    # hospital = list(map(int, list(data.hospital)))
    # pharmacy = list(map(int, list(data.pharmacy)))
    # the_mall = list(map(int, list(data.the_mall)))
    # Supermarket = list(map(int, list(data.Supermarket)))
    # market = list(map(int, list(data.market)))
    # bank = list(map(int, list(data.bank)))
    # ATM = list(map(int, list(data.ATM)))
    # restaurant = list(map(int, list(data.restaurant)))
    # coffee_shop = list(map(int, list(data.coffee_shop)))
    # park = list(map(int, list(data.park)))
    # Gym = list(map(int, list(data.Gym)))
    # stadium = list(map(int, list(data.stadium)))

    # cinema = list(map(int, list(data.cinema_500)))
    # subway_station = list(map(int, list(data.subway_station_500)))
    # bus_station = list(map(int, list(data.bus_station_500)))
    # kindergarten = list(map(int, list(data.kindergarten_500)))
    # primary_school = list(map(int, list(data.primary_school_500)))
    # middle_school = list(map(int, list(data.middle_school_500)))
    # the_University = list(map(int, list(data.the_University_500)))
    # hospital = list(map(int, list(data.hospital_500)))
    # pharmacy = list(map(int, list(data.pharmacy_500)))
    # the_mall = list(map(int, list(data.the_mall_500)))
    # Supermarket = list(map(int, list(data.Supermarket_500)))
    # market = list(map(int, list(data.market_500)))
    # bank = list(map(int, list(data.bank_500)))
    # ATM = list(map(int, list(data.ATM_500)))
    # restaurant = list(map(int, list(data.restaurant_500)))
    # coffee_shop = list(map(int, list(data.coffee_shop_500)))
    # park = list(map(int, list(data.park_500)))
    # Gym = list(map(int, list(data.Gym_500)))
    # stadium = list(map(int, list(data.stadium_500)))

    cinema = list(map(int, list(data.cinema_1500)))
    subway_station = list(map(int, list(data.subway_station_1500)))
    bus_station = list(map(int, list(data.bus_station_1500)))
    kindergarten = list(map(int, list(data.kindergarten_1500)))
    primary_school = list(map(int, list(data.primary_school_1500)))
    middle_school = list(map(int, list(data.middle_school_1500)))
    the_University = list(map(int, list(data.the_University_1500)))
    hospital = list(map(int, list(data.hospital_1500)))
    pharmacy = list(map(int, list(data.pharmacy_1500)))
    the_mall = list(map(int, list(data.the_mall_1500)))
    Supermarket = list(map(int, list(data.Supermarket_1500)))
    market = list(map(int, list(data.market_1500)))
    bank = list(map(int, list(data.bank_1500)))
    ATM = list(map(int, list(data.ATM_1500)))
    restaurant = list(map(int, list(data.restaurant_1500)))
    coffee_shop = list(map(int, list(data.coffee_shop_1500)))
    park = list(map(int, list(data.park_1500)))
    Gym = list(map(int, list(data.Gym_1500)))
    stadium = list(map(int, list(data.stadium_1500)))
    #求小区的平均价格
    prices = []
    for each in communityName:
        cursor = db.cursor()
        sql_price = "select AVG(convert(float,price)) as price from ershoufang where communityName='" + each +"'";
        cursor.execute(sql_price)
        row = cursor.fetchone()
        if row:
            prices.append(int(row[0]))
    print(communityName)
    print(prices)

    #for i in range(0, len(communityName)):
    #    print(communityName[i], prices[i], cinema[i], subway_station[i], bus_station[i], kindergarten[i], primary_school[i], middle_school[i], the_University[i], hospital[i], pharmacy[i], the_mall[i], Supermarket[i], market[i], bank[i], ATM[i], restaurant[i], coffee_shop[i], park[i], Gym[i], stadium[i])

    prices = numpy.array(prices)
    cinema = numpy.array(cinema)
    db.close()


def get_DataFrame():
    global examDict
    global examDf
    examDict = {'prices': prices,
                'cinema': cinema,
               'subway_station': subway_station,
                'bus_station': bus_station,
                'kindergarten': kindergarten,
                'primary_school': primary_school,
                'middle_school': middle_school,
                'the_University': the_University,
                'hospital': hospital,
                'pharmacy': pharmacy,
                'the_mall': the_mall,
                'Supermarket': Supermarket,
                'market': market,
                'bank': bank,
                'ATM': ATM,
                'restaurant': restaurant,
                'coffee_shop': coffee_shop,
                'park': park,
                'Gym': Gym,
                'stadium': stadium
    }
    examDf = pd.DataFrame(examDict)
    print(examDf)


def find_relation():
    #plt.plot(cinema, prices, 'ro')
    sns.set(style="ticks", color_codes=True)
    sns.pairplot(examDf)
    #plt.show()


    cor = numpy.corrcoef(examDf, rowvar=0)[:, 0]
    ######输出相关矩阵的第一列
    print(cor)

    # 3.模型预测
    #pre = model2.predict(cinema)

    # 4.绘制结果
    #plt.scatter(cinema, prices, color='red')
    #plt.plot(cinema, pre)
    #plt.show()
    #iris_dataframe = pd.DataFrame(cinema, columns=prices)
    # 利用dataframe创建散点图矩阵, 按y_train进行着色
    #pd.plotting.scatter_matrix(iris_dataframe, c=prices, figsize=(15, 15),
    #                           marker='o', hist_kwds={'bins': 20}, s=60,
    #                           alpha=.8)

    # 使用matplotlib.pyplot来显示图像


def Linear():
    exam_X = examDf.iloc[:, 1:]
    #print(exam_X)
    exam_Y = examDf.iloc[:, 0:1]
    #print(exam_Y)
    # exam_X = exam_X.values.reshape(-1, 1)

    StandardScaler_X = StandardScaler()
    StandardScaler_y = StandardScaler()
    X_Standard = StandardScaler_X.fit_transform(exam_X)
    y_Standard = StandardScaler_y.fit_transform(exam_Y)
    #print(X_Standard)
    #print(y_Standard)

    #model2 = LinearRegression()

    model2 = LogisticRegression()
    model2.fit(X_Standard, y_Standard.astype('int'))
    #a = model2.intercept_  # 截距
    #b = model2.coef_  # 回归系数
    #print("最佳拟合线：截距", a, ",回归系数：", b)
    score = model2.score(X_Standard, y_Standard.astype('int'))
    print(score)


def main():
    get_message()
    get_DataFrame()
    find_relation()
    Linear()
if __name__ == '__main__':
    #old = time.time()
    main()
    #new = time.time()
    #delta_time = new - old
    #print("程序共运行{}s".format(delta_time))
