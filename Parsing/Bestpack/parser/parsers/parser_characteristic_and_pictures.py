import asyncio
import hashlib
import re
import uuid
from pathlib import Path
from logging import Logger, raiseExceptions
from urllib.parse import urlparse
from uuid import NAMESPACE_URL
from typing import Optional
from scrapyx.clients import Requests, Files, PostgreSQL
from scrapyx.base import BaseScraperSync
from models import Characteristic, Picture, Source
from scrapyx.utils import normalize_text

class ParserCharacteristicAndPicture(BaseScraperSync):
    BATCH_SIZE_ASYNC = 20
    BATCH_SIZE_INSERT = 500
    SOURCE_NAME = "Bestpack"

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL, files: Files):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.file = files
        self.unique_urls = set()
        self.all_pictures = []

    async def parse(self, product_urls: list) -> None:
        if not product_urls:
            self.logger.error("No product URLs provided for characteristic and pictures extraction.")
            return None

        # characteristic
        await self.extract_characteristic(product_urls)
        self.logger.info(f"Characteristics extracted successfully.")

        # pictures
        await self.parse_pictures(self.all_pictures)
        self.all_pictures.clear()

        # Info
        self.logger.info(f"Characteristics and Pictures parsed successfully and inserted into database!")

        return None


    async def extract_characteristic(self, product_links: list) -> None:
        if not product_links:
            self.logger.error("No product links provided for characteristic and pictures extraction.")
            return None

        product_characteristics = []

        for i in range(0, len(product_links), self.BATCH_SIZE_ASYNC):
            batch = product_links[i : i + self.BATCH_SIZE_ASYNC]

            tasks = [self.parse_single_product(link) for link in batch]
            products_chars = await asyncio.gather(*tasks)

            for product_char in products_chars:
                if product_char:
                    product_characteristics.extend(product_char)

            if len(product_characteristics) >= self.BATCH_SIZE_INSERT:
                await self.db.insert_batch(product_characteristics)
                product_characteristics.clear()

        if product_characteristics:
            await self.db.insert_batch(product_characteristics)
        return None

    async def parse_single_product(self, url: str) -> list:
        try:
            async with self.request_dispatcher.get(url) as response:
                if not response.ok:
                    self.logger.warning(f"Failed to get product {url} with status {response.status}")
                    return []
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            # product_hash_id
            product_hash_pattern = re.sub(r'.*(?=/products/)', '', url)
            product_url_hash = self.hash_string(string=product_hash_pattern)
            if not product_url_hash:
                self.logger.error(f"Failed to extract product hash ID from {url}")
                return []

            # take product characteristics
            take_characteristics = soup.select("div.prod_chars_row")
            if not take_characteristics:
                self.logger.error(f"Failed to get product characteristics {url}")
                return []

            product_characteristics = []
            check_duplicate = set()

            for characteristic in take_characteristics:
                key = characteristic.select_one("div.prod_chars_name")
                value = characteristic.select_one("div.prod_chars_value")

                if not key or not value or not key.text or not value.text:
                    continue

                key_text = normalize_text(text=key.text, case='u')
                value_text = normalize_text(text=value.text, case='u')

                key_text = key_text.replace(':', '')

                if key_text in check_duplicate or value_text == '-':
                    continue

                product_characteristics.append(Characteristic(
                    product_url_hash=product_url_hash,
                    characteristic=key_text,
                    value=value_text
                ))
                check_duplicate.add(key_text)

            # take images
            image_link = soup.select("div.product_dots img")
            if not image_link:
                self.logger.error(f"Failed to get product images {url}")
                return product_characteristics

            for image in image_link:
                curr_image = image.get('src')
                if not curr_image:
                    continue

                self.all_pictures.append({
                    "product_url_hash": product_url_hash,
                    "image_url": f"https://bestpack.kz/{curr_image}"
                })

            return product_characteristics
        except Exception as e:
            self.logger.exception(f"Error while parsing product {url}: {e}")
            return []

    @staticmethod
    def hash_string(string, algorithm="sha256") -> Optional[str]:
        if not isinstance(string, str):
            return None

        hasher = hashlib.new(algorithm)
        hasher.update(string.encode('utf-8'))
        return hasher.hexdigest()

    # picture part
    async def parse_pictures(self, pictures_info: list) -> None:
        if not pictures_info:
            self.logger.exception("No pictures to process.")
            return None

        tasks = []
        pictures = []

        source_folder = await self.find_source_id()
        if not source_folder:
            raise Exception(f"Source '{self.SOURCE_NAME}' not found in the database.")

        for pic in pictures_info:
            product_url_hash = pic.get('product_url_hash')
            image_url = pic.get('image_url')
            if not product_url_hash or not image_url or not isinstance(product_url_hash, str):
                self.logger.error(f"Invalid product code or image URL or trash image: {product_url_hash}, {image_url}")
                continue

            ext = Path(urlparse(image_url).path).suffix if Path(urlparse(image_url).path).suffix else '.jpg'

            tasks.append(self.process_file(source_folder=source_folder, product_url_hash=product_url_hash, url=image_url, extension=ext))

            if len(tasks) >= self.BATCH_SIZE_ASYNC:
                pictures += await self.filter_urls(await asyncio.gather(*tasks, return_exceptions=True))
                tasks.clear()

            if len(pictures) >= self.BATCH_SIZE_INSERT:
                await self.db.insert_batch(pictures)
                pictures.clear()

        if tasks:
            pictures += await self.filter_urls(await asyncio.gather(*tasks))

        if pictures:
            await self.db.insert_batch(pictures)

        return None

    async def find_source_id(self) -> Optional[str]:
        sources = await self.db.select_all(Source)
        for source in sources:
            if source.name.lower() == self.SOURCE_NAME.lower():
                return str(source.id)
        return None

    async def process_file(self, source_folder: str,product_url_hash: str, url: str, extension: str) -> Optional[Picture]:
        try:
            async with self.request_dispatcher.get(url=url, raise_on_status=False, log_info=False) as response:
                if response.status != 200:
                    return None
                content = await response.read()

            # build the path and download the file
            path = await self.download_file(source_folder=source_folder, product_url_hash=product_url_hash, extension=extension, content=content)

            if not path or path in self.unique_urls:
                return None

            self.unique_urls.add(path)
            return Picture(
                product_url_hash=product_url_hash,
                image_url=url,
                path=path
            )
        except Exception as e:
            self.logger.exception(f"Failed to process file {url}: {e}")
            return None

    async def download_file(self, source_folder: str, product_url_hash: str, extension: str, content: [str | bytes]) -> Optional[str]:
        folder = await self.__generate_uuid(product_url_hash)
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
        return str(uuid.uuid5(namespace=NAMESPACE_URL, name=name))

    @staticmethod
    async def filter_urls(urls: list):
        return [u for u in urls if u]