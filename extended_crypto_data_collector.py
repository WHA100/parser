import aiohttp
import asyncio
import pandas as pd
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/wha10/neiro/crypto_parser.log'),
        logging.StreamHandler()
    ]
)

class CryptoNewsParser:
    def __init__(self):
        self.data_dir = "/home/wha10/neiro"
        self.create_directories()

        # API ключ для NewsData.io
        self.newsdata_api_key = "pub_58780ef55e3d7c8c0582001f7f6f7a23fd95a"

        # Расширенный список RSS-фидов
        self.rss_feeds = [
            'https://cointelegraph.com/rss',
            'https://coindesk.com/arc/outboundfeeds/rss/',
            'https://cryptonews.com/news/feed',
            'https://decrypt.co/feed',
            'https://news.bitcoin.com/feed',
            'https://www.investing.com/rss/news_25.rss',
            'https://u.today/rss',
            'https://newsbtc.com/feed',
        ]

        # Заголовки для запросов
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def create_directories(self):
        os.makedirs(self.data_dir, exist_ok=True)

    async def fetch(self, session, url):
        try:
            async with session.get(url, headers=self.headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"Ошибка {response.status} при запросе {url}")
                    return None
        except Exception as e:
            logging.error(f"Ошибка при запросе {url}: {str(e)}")
            return None

    async def fetch_rss_feed(self, session, feed_url):
        articles = []
        content = await self.fetch(session, feed_url)
        if content:
            try:
                soup = BeautifulSoup(content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title = item.find('title').text if item.find('title') else None
                    link = item.find('link').text if item.find('link') else None
                    description = item.find('description').text if item.find('description') else None
                    pub_date = item.find('pubDate').text if item.find('pubDate') else None

                    article = {
                        'title': title,
                        'link': link,
                        'description': description,
                        'published_date': pub_date,
                        'source': feed_url.split('/')[2]
                    }
                    articles.append(article)
            except Exception as e:
                logging.error(f"Ошибка при обработке RSS-фида {feed_url}: {str(e)}")

        return articles

    async def fetch_all_rss_feeds(self):
        articles = []
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_rss_feed(session, feed_url) for feed_url in self.rss_feeds]
            results = await asyncio.gather(*tasks)

            for result in results:
                if result:
                    articles.extend(result)

        return articles

    async def fetch_newsdata_articles(self, days_back=30):
        articles = []
        url = f"https://newsdata.io/api/1/news?apikey={self.newsdata_api_key}&q=cryptocurrency&language=en"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        async with aiohttp.ClientSession() as session:
            for page in range(1, 6):  # Собираем первые 5 страниц
                params = {
                    'from_date': start_date.strftime('%Y-%m-%d'),
                    'to_date': end_date.strftime('%Y-%m-%d'),
                    'page': page
                }
                content = await self.fetch(session, f"{url}&page={page}")
                if content:
                    data = pd.json.loads(content)
                    for article in data.get('results', []):
                        articles.append({
                            'title': article.get('title'),
                            'link': article.get('link'),
                            'description': article.get('content'),
                            'published_date': article.get('pubDate'),
                            'source': 'newsdata.io'
                        })

        return articles

    def save_to_csv(self, articles):
        if not articles:
            logging.warning("Нет статей для сохранения")
            return

        df = pd.DataFrame(articles)
        output_path = os.path.join(self.data_dir, f'crypto_articles_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        df.to_csv(output_path, index=False)
        logging.info(f"Сохранено {len(df)} статей в файл {output_path}")

    async def run_parser(self):
        logging.info("Запуск парсера криптовалютных новостей")

        rss_articles = await self.fetch_all_rss_feeds()
        newsdata_articles = await self.fetch_newsdata_articles()
        all_articles = rss_articles + newsdata_articles

        self.save_to_csv(all_articles)


if __name__ == "__main__":
    parser = CryptoNewsParser()
    asyncio.run(parser.run_parser())
