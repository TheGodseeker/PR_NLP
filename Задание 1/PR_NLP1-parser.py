import pandas as pd
import aiohttp
import asyncio
import copy
from bs4 import BeautifulSoup

#константы

ROWS_LIMIT = 15000
SITE_URL = 'https://lenta.ru'
PAGE_URL = lambda year, month, day : f'{SITE_URL}/news/{year}/{month}/{day}'
IGNORE_URL = [ #всякие мусорные ссылки, которые не новостями 
   "/"
]


def date_str(num):
    if num < 10:
      return "0"+str(num)
    else:
      return str(num)
    
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
}

async def get_page_urls(session, start_date, finish_date): # date = [year, month, day]
      async with session.get(SITE_URL, headers=headers) as response:
        if response.status == 200:
            result = []
            cur_date = copy.deepcopy(start_date)
            while cur_date <= finish_date:
              cur_url = PAGE_URL(cur_date[0], date_str(cur_date[1]), date_str(cur_date[2]))
              result.append(cur_url)
              if (cur_date[1] % 2 == 0):
                match cur_date[1]:
                  case 2:
                    edge_day = 28
                  case _:
                    edge_day = 30
              else:
                edge_day = 31
              if cur_date[2] == edge_day:
                match cur_date[1]:
                  case 12:
                    cur_date[0] += 1
                    cur_date[1] = 1
                    cur_date[2] = 1
                  case _:
                    cur_date[1] += 1
                    cur_date[2] = 1
              else:
                 cur_date[2] += 1
            # await asyncio.sleep(1)
            return result
        else:
            # await asyncio.sleep(1)
            print('Ошибка при запросе:', response.status)
            return []

async def get_article_urls(url, session):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            content = soup.find_all(class_='archive-page__item _news')
            # await asyncio.sleep(1)
            return [(SITE_URL + item.find('a').get('href')) for item in content]
        else:
            # await asyncio.sleep(1)
            print('Ошибка при запросе:', response.status)
            return []

async def get_article_content(url, session):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            try:
              content = await response.text()
              soup = BeautifulSoup(content, 'html.parser')
              title = soup.find_all(class_='topic-body__titles')[0].find('span').text
              category = soup.find('a', class_='topic-header__rubric').text
              date = soup.find('a', class_='topic-header__time').text
              body = ""
              #body = "\""
              for p in soup.find_all(class_='topic-body__content')[0].find_all('p'):
                body = body + p.text
              # body = [p.text for p in soup.find_all(class_='topic-body__content')[0].find_all('p')]
              #body += "\""
              # await asyncio.sleep(1)
              return title, body, category, date
            except AttributeError:
               pass
        else:
            # await asyncio.sleep(1)
            print('Ошибка при запросе:', response.status)
            return None, None, None, None 


async def main():
    #создаем таблицу, куда и будем складировать данные

    data = {"title": [],
            "content": [],
            "category": [],
            "created_date": []
            }
    df = pd.DataFrame(data)

    rows_count = 0

    async with aiohttp.ClientSession() as session:
        page_urls = await get_page_urls(session, [2020, 1, 1], [2024, 5, 27])
        for page_url in page_urls:
            article_urls = await get_article_urls(page_url, session)
            # if len(article_urls) >= ROWS_LIMIT:
            #    article_urls = article_urls[-(len(article_urls)-ROWS_LIMIT)]
            #print(len(article_urls))          
            for article_url in article_urls:
                try:
                  title, body, category, date = await get_article_content(article_url, session)
                  await asyncio.sleep(1)
                  if title and body and category and date:
                      print("> "+ title)
                      print("Cat: "+ category)
                      print("Date: "+ date)
                      print(body)
                      rows_count += 1
                      # cur_row = {"title": title,
                      #            "content": body,
                      #            "category": category,
                      #            "created_date": date
                      #           }
                      
                      # df._append(cur_row, ignore_index=True)
                      df.loc[rows_count] = [title, body, category, date]
                      if rows_count == ROWS_LIMIT:
                          break
                except TypeError:
                   continue
                        
            if rows_count == ROWS_LIMIT:
              break    
    print("Number of rows = "+ str(df.shape[0]))
    df.to_csv("articles.csv", sep='\t')

if __name__ == "__main__":
    asyncio.run(main())