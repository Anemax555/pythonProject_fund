import asyncio
import time
import fund_manager
import aiohttp
import pymysql
import pandas as pd
import fund_name_em
from lxml import etree
from tqdm import tqdm


def push_mysql(params):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = 'replace into fund_manager_info (f_id,f_name,f_company,f_company_id,f_scale,f_response_rate,f_introduction) values (%s,%s,%s,%s,%s,%s,%s)'
    cur.execute(sql, params)
    con.commit()
    con.close()


async def get_requests(url, info):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'}
    async with aiohttp.ClientSession() as sess:
        async with await sess.get(url=url, headers=headers) as resp:
            page_text = await resp.text()
            page = {"url": url, "page": page_text, "fund_manager": info, "status": resp.status}
            return page


def table_get(t):
    page = t.result()
    page_text = page["page"]
    url = page["url"]
    fund_manager = page["fund_manager"]
    if (page["status"] != 200):
        print("Erro:", page["status"], url)
        return
    tree = etree.HTML(page_text)
    fm_jj = tree.xpath(
        "//div[@class='content_out']/div[@class='content_in ']/div[@class='jlinfo clearfix']/div[@class='right ms']/p/text()")

    fm_info = ''.join(fm_jj).strip()
    if pd.isna(fund_manager[8]):
        f_scale = float(0)
    else:
        f_scale = fund_manager[8]
    if pd.isna(fund_manager[-1]):

        f_resp_rate = float(0)
    else:
        f_resp_rate = fund_manager[-1]

    params = (fund_manager[1], fund_manager[2], fund_manager[4], fund_manager[3], f_scale, f_resp_rate, fm_info)
    # print(params)

    push_mysql(params)


def page_index_get(urls, infos):
    tasks = []
    for i in range(len(urls)):
        c = get_requests(urls[i], infos[i])
        task = asyncio.ensure_future(c)
        task.add_done_callback(table_get)
        tasks.append(task)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))


def main_index():
    fund_manager_info = fund_manager.fund_manager(adjust='0')
    n = 0
    urls = []
    fund_managers_info = []
    time.sleep(5)
    print("go")
    for i in tqdm(range(len(fund_manager_info))):
        url = f'http://fund.eastmoney.com/manager/{fund_manager_info["ID"][i]}.html'
        urls.append(url)
        fund_managers_info.append(fund_manager_info.iloc[i].values.tolist())
        n = n + 1
        if n == 30:
            page_index_get(urls, fund_managers_info)
            urls = []
            fund_managers_info = []
            n = 0
            # print(i / len(fund_manager_info))
            time.sleep(1)
    if len(urls) > 0:
        page_index_get(urls, fund_managers_info)
    print("over")


if __name__ == '__main__':
    main_index()
