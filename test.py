import asyncio
import aiohttp
import sys
sys.path.append("/home/NRGLXT/pythonproject/project_art/py_projiec_358/article/shzqb_run/")
import time
from redis import StrictRedis
from hashlib import md5
from lxml import etree

news_list = []


def input_redis(url_id):
    redis = StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    if redis.sismember('urllist', url_id):
        return True
    else:
        # redis.sadd('urllist', url_id)
        return False


async def get_requests(url):
    async with aiohttp.ClientSession() as sess:
        async with await sess.get(url=url) as resp:
            page_text = await resp.text()
            if (resp.status != 200):
                print("Erro:  ",resp.status,url)
            return page_text


def news_list_get(t):
    page_text = t.result()
    tree = etree.HTML(page_text)
    news = tree.xpath("//ul[@id='j_waterfall_list']/li/h2/a/@href")
    if len(news) ==0:
        print("Erro")
    secret = md5()
    for new in news:
        secret.update(new.encode())
        newid = secret.hexdigest()
        if not input_redis(newid):
            news_list.append(new)


def page_index_get(urls):
    tasks = []
    for url in urls:
        c = get_requests(url)  # c是特殊函数返回的协程对象
        task = asyncio.ensure_future(c)  # 任务对象
        task.add_done_callback(news_list_get)
        tasks.append(task)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))

def main():
    url = ['https://ggjd.cnstock.com/company/scp_ggjd/tjd_ggkx']
    page_index_get(url)
    print(news_list)
    # f = open('/home/NRGLXT/pythonproject/project_art/py_projiec_358/article/shzqb_run/shzqb', mode='r')
    # url = f.readline()
    # urls_index = []
    # while url:
    #     urls_index.append(url.strip())
    #     url = f.readline()
    # f.close()
    # page_index_get(urls_index)
    #
    # n = 0
    # news_url = []
    # for i in range(len(news_list)):
    #     if (n == 10):
    #         shzqb_art.page_news_get(news_url)
    #         news_url=[]
    #         n = 0
    #         time.sleep(1)
    #     else:
    #         n = n+10
    #         news_url.append(news_list[i])
    # if n != 0 :
    #     shzqb_art.page_news_get(news_url)

    print("shzqb更新 ",len(news_list),"条数据")
main()