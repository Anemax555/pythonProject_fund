import asyncio
import time
import pandas as pd
import aiohttp
import pymysql
import found_aum_em
from tqdm import tqdm
from lxml import etree

f_reg_capital = ''


def push_mysql(params):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = 'replace into fund_company_info (f_id,f_name,f_date,f_glgm,f_num,f_mag_num,f_reg_capital) values (%s,%s,%s,%s,%s,%s,%s)'
    cur.execute(sql, params)
    con.commit()
    con.close()


async def get_requests(url, id):
    async with aiohttp.ClientSession() as sess:
        async with await sess.get(url=url) as resp:
            if (resp.status != 200):
                print("Erro: ", resp.status, url)
            page_text = await resp.text()
            page = {"url": url, "page": page_text}
            return id, page


def table_get(t):
    fund_company, page = t.result()
    page_text = page["page"]
    url = page["url"]
    temp_df = pd.read_html(page_text)[1]

    # =====================================ID
    f_ID = fund_company['ID']
    # =====================================公司名称
    f_company = fund_company['company']
    # =====================================注册时间
    f_inport_date = fund_company['incorporation']
    # =====================================管理基金规模
    f_money = fund_company['money']
    # =====================================基金数量
    f_fund_num = fund_company['fund_num']
    # =====================================基金经理数量
    f_manager_num = fund_company['manager_num']
    # =====================================注册资本
    f_reg_capital = temp_df[3][3]
    f_reg_capital = str(f_reg_capital).split('（')[0]
    f_reg_capital = float(f_reg_capital)
    # =====================================
    params = (f_ID, f_company, f_inport_date, f_money, f_fund_num, f_manager_num, f_reg_capital)
    push_mysql(params)

    # f_name = temp_df[1][0]
    # f_Eng_name = temp_df[1][1]
    # f_property = temp_df[1][2]
    # f_date_cl = temp_df[1][3]

    # f_frdb = temp_df[1][4]
    # f_zjl = temp_df[3][4]
    # f_zcdz = temp_df[1][5]
    # f_yzbm = temp_df[1][8]
    # f_manager_num = temp_df[3][12]
    # f_gm = temp_df[1][12]
    #
    # params = (f_ID, f_name, f_Eng_name, f_property, f_date_cl, f_gm, f_manager_num)
    # print(params)


def page_index_get(urls, ids):
    tasks = []
    for i in range(len(urls)):
        c = get_requests(urls[i], ids[i])
        task = asyncio.ensure_future(c)
        task.add_done_callback(table_get)
        tasks.append(task)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))


def main_index():
    found_aum_em_fd = found_aum_em.fund_aum_em()
    urls = []
    n = 0
    ids = []
    for i in tqdm(range(len(found_aum_em_fd))):
        f_ID = str(found_aum_em_fd['相关链接'][i]).split('/')[-1].strip('.html')
        url = f'http://fund.eastmoney.com/Company/f10/jbgk_{f_ID}.html'
        f_money = found_aum_em_fd['全部管理规模'][i]
        if pd.isna(f_money):
            f_money = 0
        params = {
            'ID': f_ID,
            'company': found_aum_em_fd['基金公司'][i],
            'incorporation': found_aum_em_fd['成立时间'][i],
            'money': f_money,
            'fund_num': found_aum_em_fd['全部基金数'][i],
            'manager_num': found_aum_em_fd['全部经理数'][i]
        }
        urls.append(url)
        ids.append(params)
        n = n + 1
        if n == 30:
            page_index_get(urls, ids)
            ids = []
            urls = []
            n = 0
            time.sleep(1)
    if len(urls) > 0:
        page_index_get(urls, ids)
    print("over")

    # f_money = found_aum_em_fd['全部管理规模'][i]
    # if pd.isna(f_money):
    #     f_money = 0
    # params = (f_ID, found_aum_em_fd['基金公司'][i], found_aum_em_fd['成立时间'][i], f_money, found_aum_em_fd['全部基金数'][i],
    #           found_aum_em_fd['全部经理数'][i])
    # print(params, (f_money))

    # push_mysql(params)


if __name__ == '__main__':
    main_index()
