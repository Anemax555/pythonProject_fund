# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pymysql

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

def get_data_mysql(f_title):
    db = pymysql.connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = db.cursor()  # 使用cursor方法，创建一个游标对象cur
    sql = cur.execute("SELECT * FROM fund_name_info WHERE f_ID LIKE '%%{}%%'".format(f_title))  # 使用 execute()  方法执行 SQL 查询,注意有引号
    data = cur.fetchone()  # 使用 fetchone() 方法获取单条数据.
    print(data)
    db.close()  # 关闭数据库
    return data

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    get_data_mysql("159710")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
