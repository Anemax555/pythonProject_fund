import pymysql
import pandas as pd


def input_fund_mysql(id, prize):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = "update fund_name_info set f_prize='{}' where f_id='{}'".format(prize, id)
    cur.execute(sql)
    con.commit()
    con.close()

def input_manager_mysql(name,prize):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = "update fund_manager_info set f_prize='{}' where f_name='{}'".format(prize, name)
    cur.execute(sql)
    con.commit()
    con.close()

def input_company_mysql(name,prize):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = "update fund_company_info set f_prize='{}' where f_name LIKE '%%{}%%'".format(prize, name)
    cur.execute(sql)
    con.commit()
    con.close()

def get_data_mysql(f_id):
    db = pymysql.connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = db.cursor()  # 使用cursor方法，创建一个游标对象cur
    sql = cur.execute("SELECT f_manager FROM fund_name_info WHERE f_id='{}'".format(f_id))  # 使用 execute()  方法执行 SQL 查询,注意有引号
    data = cur.fetchone()  # 使用 fetchone() 方法获取单条数据.
    db.close()  # 关闭数据库
    return data

def fund_prize():
    f = open('prize.txt', mode='r')
    fund_p = f.readline()
    while fund_p:
        fund_v = fund_p.split('	')
        fund_id = fund_v[0].strip('.OF')
        print(fund_id)
        fund_prize = '第十八届金牛奖'
        name =  get_data_mysql(fund_id)
        name = str(name).split('、')[0]
        name = name.replace('(\'','')
        name = name.replace('\'','')
        name = name.replace(',)','')
        print(name)
        # print(fund_id, fund_prize)
        input_fund_mysql(id=fund_id, prize=fund_prize)
        input_manager_mysql(name=name,prize=fund_prize)
        fund_p = f.readline()
    f.close()

def company_prize():
    f = open('company_prize.txt',mode='r')
    fund_company = f.readline()
    while fund_company:
        company_name = fund_company.split('	')[0]
        fund_prize = '第十八届金牛奖'
        print(company_name)
        input_company_mysql(name=company_name,prize=fund_prize)
        fund_company = f.readline()
    f.close()

def get_art_mysql(f_id):
    db = pymysql.connect(host='47.96.18.55', user='crawler', password='123456', database='cnstock_db', port=3306)
    cur = db.cursor()  # 使用cursor方法，创建一个游标对象cur
    sql = cur.execute("SELECT f_source,f_fromurl,f_sourceAddress,f_inputTime,f_context FROM f_article WHERE f_uid='{}'".format(f_id))  # 使用 execute()  方法执行 SQL 查询,注意有引号
    data = cur.fetchall()  # 使用 fetchone() 方法获取单条数据.
    db.close()  # 关闭数据库
    return data

data = get_art_mysql('20220714_4737962')
print(data)
# pd_list = pd.DataFrame(data)
# print(pd_list)
# pd_list.to_excel('21.xls')