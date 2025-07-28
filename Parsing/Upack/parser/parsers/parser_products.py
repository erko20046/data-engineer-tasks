import asyncio
import uuid
from logging import Logger
from pathlib import Path
from typing import Optional
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from scrapyx.clients import Requests, PostgreSQL, Files
from scrapyx.utils import normalize_text
from scrapyx.base import BaseScraperSync

from models import Product, Characteristic, Picture, Source


class ParserProducts(BaseScraperSync):
    BATCH_SIZE_ASYNC = 10
    BATCH_SIZE_INSERT = 500
    URL = "https://upack.kz"
    SOURCE_NAME = 'Upack'
    TRASH_CHARACTERISTIC_VALUE = ('НЕ УКАЗАН', '0')

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL, files: Files):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.file = files
        self.unique_urls = set()

    async def parse(self, category_urls: list, category_name_id_map: dict) -> None:
        self.logger.info("Starting to extract products...")
        all_category_urls = await self.extract_all_pages(category_urls=category_urls)

        product_urls = await self.extract_product_url(category_urls=all_category_urls)

        # parsing products
        await self.extract_products(product_urls=list(set(product_urls)), category_name_id_map=category_name_id_map)

        return None

    # example for url: https://upack.kz/c/tramontina?page=1&per_page=48
    async def extract_all_pages(self, category_urls: list) -> list:
        self.logger.info("Starting to extract pages...")
        all_pages = category_urls.copy()

        for category_url in category_urls:
            async with self.request_dispatcher.get(category_url, raise_on_status=False) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {category_url}: {response.status}")
                    continue
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            last_page = soup.select_one('div.pagination > div > a:last-child')

            if not last_page or not last_page.text or not last_page.text.isdigit():
                self.logger.info(f"Failed to extract last page {category_url}")
                continue

            for index in range(2, int(last_page.text) + 1):
                page = category_url.replace('page=1', f'page={index}')
                all_pages.append(page)

        return all_pages

    async def extract_product_url(self, category_urls: list) -> list:
        product_urls = []

        for category_url in category_urls:
            async with self.request_dispatcher.get(category_url, raise_on_status=False) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {category_url}: {response.status}")
                    continue
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            cards = soup.select('div.product-wrapper div.product-wrapper-inner a.catalog-item__inner')
            if not cards:
                self.logger.info(f"Failed to extract cards for url : {category_url}")
                continue

            for card in cards:
                url = card.get('href')
                if not url:
                    self.logger.info(f"Failed to extract product url from cards : {category_url}")
                    continue
                product_urls.append(f'{self.URL}{url}')

        return product_urls

    async def extract_products(self, product_urls: list, category_name_id_map: dict) -> None:
        if not product_urls:
            raise Exception('No product urls provided')

        source_folder = await self.find_source_id()
        if not source_folder:
            raise Exception(f"Source '{self.SOURCE_NAME}' not found in the database.")

        all_products = []
        characteristics = []
        pictures = []

        for i in range(0, len(product_urls), self.BATCH_SIZE_ASYNC):
            batch_pages = product_urls[i: i + self.BATCH_SIZE_ASYNC]
            tasks = [self.parse_single_product(product_url=product_url, category_name_id_map=category_name_id_map, characteristics=characteristics, pictures=pictures, source_folder=source_folder) for product_url in batch_pages]
            products = await asyncio.gather(*tasks)

            for product in products:
                if product:
                    all_products.append(product)

            if len(all_products) >= self.BATCH_SIZE_INSERT:
                await self.db.insert_batch(data=all_products)
                all_products.clear()
                await self.db.insert_batch(data=characteristics)
                characteristics.clear()

                await self.db.insert_batch(data=pictures)
                pictures.clear()

        if all_products:
            await self.db.insert_batch(data=all_products)

        if characteristics:
            await self.db.insert_batch(data=characteristics)

        if pictures:
            await self.db.insert_batch(data=pictures)

        # Info
        self.logger.info("Products extracted and inserted into the database successfully!")
        return None

    async def parse_single_product(self, product_url: str, category_name_id_map: dict, characteristics: list, pictures: list, source_folder: str) -> Optional[Product]:
        try:
            async with self.request_dispatcher.get(product_url, raise_on_status=False) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {product_url}: {response.status}")
                    return None
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            # 1. source code
            source_code = soup.select_one('div.card-article--page')
            if not source_code or not source_code.text:
                self.logger.warning(f"Failed to extract source code for product {product_url}")
                return None
            source_code = re.sub(r'[Арт.\s]', '', source_code.get_text())

            if not source_code:
                self.logger.warning(f"Failed to extract source code for product {product_url}")
                return None

            # 2. title
            title = soup.select_one('h1.page-title')
            if not title or not title.text:
                self.logger.warning(f"Failed to extract title for product {product_url}")
                return None
            title = normalize_text(text=title.get_text(), case='u')

            # 3. category id
            category_tag = soup.select_one('ul.Breadcrumbs_breadcrumbs__0II_j li meta[content="3"]')
            if not category_tag:
                self.logger.warning(f"Failed to extract category tag for product {product_url}")
                return None
            parent_tag = category_tag.find_parent('li')
            if not parent_tag:
                self.logger.warning(
                    f"Failed to extract parent tag for product {product_url}, source code: {source_code}")
                return None
            category_link = parent_tag.select_one('[itemprop="name"]')
            if not category_link:
                self.logger.warning(
                    f"Failed to extract category link for product {product_url}, source code : {source_code}")
                return None
            category_name = category_link.get_text()
            if not category_name:
                self.logger.warning(
                    f"Failed to extract category name for product {product_url}, source code: {source_code}")
                return None

            category_name = normalize_text(text=category_name, case='u')
            category_id = category_name_id_map.get(category_name)
            if not category_id:
                self.logger.warning(f"Failed to extract category id for product {product_url}, source code: {category_name}, category name: {category_name}")
                return None

            # 4. Per Price
            per_price = soup.select_one('div.card-panel span.card-price__total > span')
            if not per_price:
                self.logger.warning(f"Failed to extract per-price tag for product {product_url}")
                return None
            per_price = float(per_price.get_text())

            # p tags
            p_tags = soup.select('div.card-panel > p')
            # берем p_tags[0], [1], [2]
            if len(p_tags) < 3:
                self.logger.warning(f"length of p_tags less than 3 for product {product_url}")
                return None

            # 5. quantity per box
            quantity_per_box = self.extract_digit_from_text(p_tags[0].get_text())
            if not quantity_per_box:
                self.logger.warning(f"Failed to extract quantity per box for product {product_url}")
                return None

            # 6. quantity per pack
            quantity_per_pack = self.extract_digit_from_text(p_tags[1].get_text())
            if not quantity_per_pack:
                self.logger.warning(f"Failed to extract quantity per pack for product {product_url}")
                return None

            # 7. minimum quantity
            min_quantity = self.extract_digit_from_text(p_tags[2].get_text())
            if not min_quantity:
                self.logger.warning(f"Failed to extract minimum quantity for product {product_url}")
                return None

            # 8. minimum batch price
            min_batch_price = per_price * min_quantity

            product = Product(
                source_code=source_code,
                title=title,
                category_id=category_id,
                product_url=product_url,
                per_price=per_price,
                quantity_per_box=quantity_per_box,
                quantity_per_pack=quantity_per_pack,
                min_quantity=min_quantity,
                min_batch_price=min_batch_price
            )

            # characteristic part
            await self.get_characteristics(source_code=source_code, soup=soup, characteristics=characteristics)

            # picture part
            await self.get_images(source_code=source_code, soup=soup, pictures=pictures, source_folder=source_folder)

            return product
        except Exception as e:
            self.logger.error(f"Error while parsing single product {product_url}: {e}")
            return None

    @staticmethod
    def extract_digit_from_text(text: Optional[str]) -> Optional[int]:
        if text:
            digit = re.sub(r'\D', '', text)
            if digit.isdigit():
                return int(digit)
        return None

    async def get_characteristics(self, source_code: str, soup: BeautifulSoup, characteristics: list) -> None:
        try:
            check_duplicates = set()
            cards = soup.select('ul.props-list li.CardPropsItem_card-props__item__s7rU2')
            if not cards:
                self.logger.warning(
                    f'Failed to extract cards in method get_characteristics! source code: {source_code}')
                return None

            for card in cards:
                key = card.select_one("span.CardPropsItem_card-props__name__rwBE3")
                value = card.select_one("span.CardPropsItem_card-props__value__1rCme")

                if not key or not value or not key.text or not value.text:
                    self.logger.warning(f"Failed to extract key or value from characteristics")
                    continue
                key = key.text.replace('\u2009', ' ')
                if not key:
                    continue

                key = normalize_text(text=key, case='u')
                value = normalize_text(text=value.text, case='u')

                if key in check_duplicates or value in self.TRASH_CHARACTERISTIC_VALUE:
                    continue
                check_duplicates.add(key)

                characteristics.append(Characteristic(
                    source_code=source_code,
                    characteristic=key,
                    value=value
                ))
        except Exception as e:
            self.logger.error(f"Error while parsing characteristic! source code:{source_code}, error: {e}")
            return None

    async def get_images(self, source_code: str, soup: BeautifulSoup, pictures: list, source_folder: str) -> None:
        try:
            cards = soup.select("div.card-images--page a[data-fancybox='gallery']")
            if not cards:
                self.logger.warning(f'Failed to extract cards in method get_images! source code: {source_code}')
                return None

            for card in cards:
                image_url = card.get('href')
                if not image_url or not image_url.startswith("http"):
                    continue

                ext = Path(urlparse(image_url).path).suffix or '.jpg'

                picture = await self.process_file(source_folder=source_folder, source_code=source_code, url=image_url, extension=ext)

                if picture:
                    pictures.append(picture)

        except Exception as e:
            self.logger.error(f"Error while parsing image! source code:{source_code}, error: {e}")
            return None

    async def find_source_id(self) -> Optional[str]:
        sources = await self.db.select_all(Source)
        for source in sources:
            if source.name.lower() == self.SOURCE_NAME.lower():
                return str(source.id)
        return None

    async def process_file(self, source_folder: str, source_code: str, url: str, extension: str) -> Optional[Picture]:
        try:
            async with self.request_dispatcher.get(url=url, raise_on_status=False) as response:
                if response.status != 200:
                    return None
                content = await response.read()

            # build the path and download the file
            path = await self.download_file(source_folder=source_folder, source_code=source_code, extension=extension,
                                            content=content)

            if not path or path in self.unique_urls:
                return None

            self.unique_urls.add(path)
            return Picture(
                source_code=source_code,
                image_url=url,
                path=path
            )
        except Exception as e:
            self.logger.exception(f"Failed to process file {url}: {e}")
            return None

    async def download_file(self, source_folder: str, source_code: str, extension: str, content: [str | bytes]) -> Optional[str]:
        folder = await self.__generate_uuid(source_code)
        filename = await self.__generate_uuid(str(content))
        path = await self.file.write_file(
            filename=str(f"{filename}{extension}"),
            path=f"{source_folder}/{folder}",
            content=content,
            return_type='r'
        )
        return path

    @staticmethod
    async def __generate_uuid(name: str) -> str:
        return str(uuid.uuid5(namespace=uuid.NAMESPACE_URL, name=name))
