import asyncio
import aiohttp
from lxml import html
import logging
import re
import datetime, time
# from db_connector import PDBConnector
from pymongo import MongoClient


logging.basicConfig(level=logging.INFO)


def write_to_PostgreDB(web_data, db_connector):
    try:
        # create the table "post"
        db_connector.write_to_database("""
          CREATE TABLE IF NOT EXISTS post(
          id SERIAL PRIMARY KEY, 
          title TEXT NOT NULL, 
          url VARCHAR(300) NOT NULL, 
          author VARCHAR(50) NOT NULL, 
          text TEXT NOT NULL , 
          price DOUBLE PRECISION NOT NULL, 
          currency VARCHAR(3) NOT NULL
          );""")

        # insert web-data into the table "post"
        for page in web_data:
            for result in page:
                title, url, author, text, price, currency = result
                db_connector.write_to_database("INSERT INTO post(title, url, author, text, price, currency) VALUES " \
                                               "(%s, %s, %s, %s, %s, %s)", (title, url, author, text, price, currency))

        # create indexes
        db_connector.write_to_database("CREATE INDEX title ON post (title)")
        db_connector.write_to_database("CREATE INDEX author ON post (author)")

        # close database session
        db_connector.close_connect()
    except Exception as err:
        print(err)


def write_to_MongoDB(web_data, mongo_client):
    # delete database if it exists
    # mongo_client.drop_database('scrapp_database')

    # select the database "scrapp_database", creates new if not exists
    db = mongo_client['scrapp_database']

    # select the collection "post", creates new if not exists
    coll = db['post']

    # insert document with web-data to collection
    for page in web_data:
        for data in page:
            title, url, author, text, price, currency = data
            doc = {
                'title': title,
                'url': url,
                'author': author,
                'text': text,
                'price': price,
                'currency': currency
            }

            result = coll.insert_one(doc)
            print('Document insertion success: {0}\nDoc ID: {1}'.format(
                result.acknowledged, result.inserted_id if result.acknowledged else ''))

            print('-' * 100)


class Scrapper:
    def __init__(self, page_from, page_to, limit=2,):
        self.page_from = page_from
        self.page_to = page_to + 1
        self.event_loop = None
        self.limit = limit

    async def __prepare(self):
        HEADERS = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, lzma, sdch',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'forum.overclockers.ua',
            'Referer': 'http://forum.overclockers.ua',
            'Save-Data': 'on',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/54.0.2840.99 Safari/537.36 OPR/41.0.2353.69',
        }

        conn = aiohttp.TCPConnector(verify_ssl=True)
        self.session = aiohttp.ClientSession(connector=conn, headers=HEADERS)
        return

    def start(self):
        self.event_loop = asyncio.get_event_loop()
        try:
            self.event_loop.run_until_complete(self.__prepare())
            results = self.event_loop.run_until_complete(self.__run())
        finally:
            self.session.close()
            self.event_loop.close()
        return results

    async def __run(self):
        links = [self.get_link(page) for page in range(self.page_from, self.page_to)]

        semaphore = asyncio.BoundedSemaphore(self.limit)
        tasks = []
        results = []

        for link in links:
            tasks.append(self.crawl(link, semaphore))

        for task in asyncio.as_completed(tasks):
            result = await task
            results.append(result)
            task.close()

        return results

    async def crawl(self, url, semaphore):
        """
        Choose the searched data from the site.
        :param url: 
        :param semaphore: 
        :return: data list
        """
        async with semaphore:
            resp = await self.session.get(url)

            if resp.status == 200:
                page = await resp.text()
                root = html.fromstring(page)

                items = []

                topics = root.xpath('//ul[@class="topiclist topics"]/li')
                for topic in topics:
                    try:
                        # parsing external data of the post
                        title = topic.xpath('.//a[@class="topictitle"]/text()')[0]
                        url = topic.xpath('.//a/@href')[0].replace('./', 'http://forum.overclockers.ua/')
                        author = topic.xpath('.//dd[@class="author"]/a[@class="username"]/text()')[0]

                        # run the coroutine object to work with the internal data of the post
                        post_data = []
                        for task in asyncio.as_completed([self.select_post_data(url)]):
                            result = await task
                            post_data.append(result)
                            task.close()

                        text, price, currency = post_data[0]
                        price = price if price else '0.00'
                        items.append((title, url, author, text, price, currency))
                    except Exception as err:
                        # logging.error(err)
                        pass
                return items

    async def select_post_data(self, url):
        """
        Choose internal data from the post.
        :param url: 
        :return: data tuple
        """
        async with aiohttp.ClientSession(loop=self.event_loop) as session:
            html_text = await self.fetch_html(session, url)
            root = html.fromstring(html_text)

            text = price = currency = ''
            try:
                # parsing of the internal data of the post.
                text = root.xpath('//div[@class="content"]')[0].text_content()
                text = re.sub(r'(_)+', '', text)
                price = re.findall(r'\b(\d+(\.\d{2})?)\b(?:грн|уе|уо|у\.е|у\.о)', text)[0]
                currency = re.findall(r'\b(?:\d+(\.\d{2})?)\b(грн|[₴£$€]|EUR|USD|GPB|PLN|UAH)', text)[1]
            except Exception as err:
                # logging.error(err)
                pass
            finally:
                session.close()
            return text, price, currency

    @staticmethod
    async def fetch_html(session, url):
        async with session.get(url) as response:
            return await response.text()

    @staticmethod
    def get_link(page):
        if page == 1:
            link = 'http://forum.overclockers.ua/viewforum.php?f=26'
        else:
            link = 'http://forum.overclockers.ua/viewforum.php?f=26&start={}'.format((page - 1) * 40)
        return link


if __name__ == '__main__':
    t = time.time()
    scrapper = Scrapper(1, 4, limit=2)
    results = scrapper.start()

    Time = time.time() - t
    print('{} - web-data has been received'.format(Time))

    # connector = PDBConnector(user='', password='')
    # write_to_PostgreDB(results, connector)

    client = MongoClient('localhost', 27017)
    write_to_MongoDB(results, client)
