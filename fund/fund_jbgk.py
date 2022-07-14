import asyncio
import time
from tqdm import tqdm
import aiohttp
import pymysql
import fund_name_em
from lxml import etree


def push_mysql(params):
    con = pymysql.Connect(host='47.96.18.55', user='crawler', password='123456', database='cnfund_db', port=3306)
    cur = con.cursor()
    sql = 'replace into fund_name_info (f_id,' \
          'f_name,' \
          'f_name_full,' \
          'f_type,' \
          'f_date_fx,' \
          'f_date_cl,' \
          'f_gm,' \
          'f_manager,' \
          'f_admin,' \
          'f_gm_date,' \
          'f_source,' \
          'f_hosting,' \
          'f_target) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    cur.execute(sql, params)
    con.commit()
    con.close()


async def get_requests(url, id):
    async with aiohttp.ClientSession() as sess:
        async with await sess.get(url=url) as resp:
            if (resp.status != 200):
                print("Erro: ", resp.status, url)
            page_text = await resp.text()
            page = {"url": url, "page": page_text, "fund_ID": id}
            return page


def table_get(t):
    page = t.result()
    page_text = page["page"]
    url = page["url"]
    fund_ID = page["fund_ID"]

    tree = etree.HTML(page_text)
    name_full = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[1]/td[1]/text()")
    name_jc = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[1]/td[2]/text()")
    fund_type = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[2]/td[2]/text()")
    fund_date_fx = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[3]/td[1]/text()")
    fund_date = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[3]/td[2]/text()")
    fund_gl = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[5]/td[1]//text()")
    fund_jls = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[6]/td[1]//text()")
    fund_gm = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[4]/td[1]//text()")
    fund_hosting = tree.xpath(
        "//div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][1]/table[@class='info w790']/tr[5]/td[2]//text()")
    fund_target = tree.xpath(
        "/html/body/div[@id='bodydiv']/div[@class='mainFrame'][8]/div[@class='r_cont right']/div[@class='detail']/div[@class='txt_cont']/div[@class='txt_in']/div[@class='box'][2]/div[@class='boxitem w790']/p/text()")

    f_name = ''.join(name_jc).strip()
    f_name_full = ''.join(name_full).strip()
    f_type = ''.join(fund_type).strip()
    f_date_fx = ''.join(fund_date_fx).strip()
    f_manager = ''.join(fund_jls).strip()
    f_admin = ''.join(fund_gl).strip()
    fund_gm = ''.join(fund_gm).strip()
    fund_date_cl = str(fund_date[0]).split('/')[0].strip()
    fund_hosting = ''.join(fund_hosting).strip()
    fund_target = ''.join(fund_target).strip()
    # ==================================================规模截止日期
    fund_gm_date = fund_gm.split('（')[-1].strip('）')
    fund_gm_date = fund_gm_date.replace('截止至：', '')
    # ==================================================规模
    fund_gm = fund_gm.split('（')[0]
    fund_gm = fund_gm.replace('亿元', '')
    fund_gm = fund_gm.replace(',', '')
    if '---' in fund_gm:
        fund_gm = float(0)
    else:
        fund_gm = float(fund_gm)
    params = (
        fund_ID, f_name, f_name_full, f_type, f_date_fx, fund_date_cl, fund_gm, f_manager, f_admin, fund_gm_date, '互联网',
        fund_hosting, fund_target)
    # print(params)
    push_mysql(params)


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
    # urls = ['https://fundf10.eastmoney.com/jbgk_004570.html']
    # ids = ['004570']
    # page_index_get(urls,ids)
    # ## ====================================test

    fund_id = fund_name_em.fund_name_em().get('基金代码')
    n = 0
    urls = []
    fund_ids = []
    for i in tqdm(range(len(fund_id))):
        url = f'https://fundf10.eastmoney.com/jbgk_{fund_id[i]}.html'
        urls.append(url)
        fund_ids.append(fund_id[i])
        n = n + 1
        if n == 50:
            page_index_get(urls, fund_ids)
            urls = []
            fund_ids = []
            n = 0
            # print(i / len(fund_id))
            time.sleep(1)
    if len(urls) > 0:
        page_index_get(urls, fund_ids)


if __name__ == '__main__':
    main_index()
