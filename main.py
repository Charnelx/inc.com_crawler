import requests
import json
import asyncio
import aiohttp
import logging
import xlsxwriter

class GetProxy:

    def __init__(self, proxies):
        self.proxies = proxies

    @property
    def get_proxy(self):
        proxy = self.proxies.pop()
        self.proxies.insert(0, proxy)
        return proxy

class Spider:

    def __init__(self, proxies=[], limit_concurrent=20, timeout=10, retry=10):
        self.proxies = proxies
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry if retry > 1 else retry + 1
        self.proxy_list = None


        self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36 OPR/40.0.2308.81",
               "Accept":"application/json, text/javascript, */*; q=0.01",
               "Accept-Encoding":"gzip, deflate, lzma",
               "Connection":"keep-alive",
               "Connection-Type":"application/x-www-form-urlencoded; charset=UTF-8",
               "Host":"www.inc.com",
               "Referer": 'http://www.inc.com/inc5000/list/2016/',
               "Pragma": "no-cache",
               "X-Requested-With":"XMLHttpRequest",
               "Accept-language":"ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4",
               }


    @asyncio.coroutine
    def __connect(self, proxy=None):
        URL = 'http://www.inc.com/inc5000/list/2016/'
        counter = 0
        while True:
            counter += 1
            if counter >= self.retry:
                break
            try:
                if self.proxies:
                    p = proxy.get_proxy
                    conn = aiohttp.ProxyConnector(proxy=p)
                else: conn = None
                session = aiohttp.ClientSession(connector=conn, headers=self.headers)

                response = yield from session.get(URL, headers=self.headers)
                body = yield from response.read()
                return session
            except Exception as err:
                logging.warning('Connection fail due to error: {0}'.format(err))
                continue
        return None

    def start(self, ids):
        self.session = asyncio.get_event_loop().run_until_complete(self.__connect())
        if not self.session:
            raise ConnectionError

        info_data = asyncio.get_event_loop().run_until_complete(self.get_info(ids))

        self.session.close()
        self.wtireXLS(info_data)

    def getJSON(self):
        url = 'http://www.inc.com/inc5000list/json/inc5000_2016.json'
        r = requests.get(url, headers=self.headers)
        data = json.loads(r.content.decode())
        ids = []
        for item in data:
            id = item['id']
            ids.append(id)
        return ids

    @asyncio.coroutine
    def get_info(self, ids):
        semaphore = asyncio.Semaphore(self.limit_concurrent)
        tasks = []
        result = []

        if len(self.proxies) == 0:
            proxy = None
        else:
            proxy = GetProxy(self.proxies)

        for id in ids:
            tasks.append(self.request_info(id, semaphore))

        for task in asyncio.as_completed(tasks):
            response = yield from task
            result.append(response)
        return result

    @asyncio.coroutine
    def request_info(self, id, semaphore):
        counter = 0
        body = ''

        url = 'http://www.inc.com/rest/inc5000company/{0}?currentinc5000year=2016'.format(id)
        with (yield from semaphore):
            while True:
                counter += 1
                if counter >= self.retry:
                    break
                with aiohttp.Timeout(self.timeout):
                    try:
                        response = yield from self.session.get(url)
                        body = yield from response.read()
                        content = body.decode()
                        break
                    except Exception as err:
                        logging.warning('Error trying to get page')
                        raise err
                        continue



        content = json.loads(content)
        company_name = content['ifc_company']
        location = '{0},{1} ({2})'.format(content['ifc_city'], content['ifc_state'], content['ifc_address'])
        revenue = content['current_ify_revenue_raw']
        founded = content['ifc_founded']
        site = content['ifc_url']
        description = content['ifc_business_description']

        if description == '' or description == None:
            description = content['ifc_business_model']
            if description == None:
                description = ''

        # normalizing links
        if site:
            if not site.startswith('http://www'):
                site = 'http://www.{0}'.format(site)
                if ' ' in site:
                    site = site.split(' ')[0]
        else:
            cite = ''

        dict = {
            'company_name': company_name,
            'location': location,
            'revenue': revenue,
            'founded': founded,
            'cite': cite,
            'description': description
        }

        return dict


    def wtireXLS(self, dict, file_name='test.xlsx'):
        workbook = xlsxwriter.Workbook(file_name)
        worksheet = workbook.add_worksheet('orgs')

        url_format = workbook.add_format({
        'font_color': 'blue',
        'shrink': True,
        })

        worksheet.set_column('A:A', 16)
        worksheet.set_column('B:B', 32)
        worksheet.set_column('C:C', 16)
        worksheet.set_column('D:D', 12)
        worksheet.set_column('E:E', 35)
        worksheet.set_column('F:F', 10)

        worksheet.write('A1', 'Company name')
        worksheet.write('B1', 'Location')
        worksheet.write('C1', '2015 Revenue')
        worksheet.write('D1', 'Founded')
        worksheet.write('E1', 'Description')
        worksheet.write('F1', 'Cite')

        index = 2
        for element in dict:
            worksheet.write('A{0}'.format(index), element['company_name'])
            worksheet.write('B{0}'.format(index), element['location'])
            worksheet.write_number('C{0}'.format(index), int(element['revenue']))
            worksheet.write('D{0}'.format(index), element['founded'])
            worksheet.write('E{0}'.format(index), element['description'])
            worksheet.write_url('F{0}'.format(index), element['cite'], url_format, 'Link')
            index += 1

        workbook.close()
        return True


spider = Spider()
links = spider.getJSON()
spider.start(links)