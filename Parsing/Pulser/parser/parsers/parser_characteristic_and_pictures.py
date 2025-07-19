import asyncio
import re
import uuid
from pathlib import Path
from logging import Logger
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
    URL = "https://pulser.kz/"
    SOURCE_NAME = "Pulser"
    source_folder = None
    trash_image_url = ('https://pulser.kz/gallery/images/image-by-item-and-alias?item=&dirtyAlias=placeHolder.png')

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL, files: Files):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.file = files
        self.unique_urls = set()
        self.all_pictures = []

    async def parse(self, product_url) -> None:
        # characteristic
        await self.extract_characteristic(product_url)

        # pictures
        await self.parse_pictures(self.all_pictures)
        self.all_pictures.clear()

        # Info
        self.logger.info(f"Characteristics and Pictures parsed successfully and inserted into database!")

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

    async def parse_single_product(self, url) -> list:
        try:
            async with self.request_dispatcher.get(url) as response:
                if not response.ok:
                    self.logger.warning(f"Failed to get product {url} with status {response.status}")
                    return []
                response = await response.text()

            soup = self.get_bs4_object(response, 'html.parser')

            # take product code
            product_code_link = soup.select_one("body > main > div > section > div.row > div:nth-child(2) > div > div:nth-child(1) > div > div:nth-child(2) > small")
            match = re.search(r"(\d+)", product_code_link.text) if product_code_link else None

            if not match:
                self.logger.error(f"Failed to get product code {url}")
                return []

            source_id = int(match.group(1))

            product_characteristics = []
            # take product characteristics
            take_characteristics = soup.select("div.row.no-gutters")
            if not take_characteristics:
                self.logger.error(f"Failed to get product characteristics {url}")
                return []

            check_duplicate = set()
            for characteristic in take_characteristics:
                key = characteristic.select_one("div.col-sm-3")
                value = characteristic.select_one("div.col-sm-9")

                if not key or not value or not key.text or not value.text:
                    continue

                key_text = normalize_text(key.text, 'u')
                value_text = normalize_text(value.text, 'u')

                if key_text == 'НАЗВАНИЕ ПРОДУКТА' or key_text in check_duplicate:
                    continue

                product_characteristics.append(await Characteristic.fill_characteristic(
                    source_id=source_id,
                    characteristic=key_text,
                    value=value_text
                ))
                check_duplicate.add(key_text)

            # take images
            image_link = soup.select("li.img-cardpreview")
            if not image_link:
                self.logger.error(f"Failed to get product images {url}")
                return product_characteristics

            for image in image_link:
                curr_image = image.find('img').get('src')
                if not curr_image:
                    continue

                self.all_pictures.append({
                    "source_id": source_id,
                    "image_url": f"https://pulser.kz{curr_image}"
                })

            return product_characteristics
        except Exception as e:
            self.logger.exception(f"Error while parsing product {url}: {e}")
            return []

    # picture part
    async def parse_pictures(self, pictures_info: list) -> None:
        if not pictures_info:
            self.logger.exception("No pictures to process.")
            return None

        tasks = []
        pictures = []

        self.source_folder = await self.find_source_id()
        if not self.source_folder:
            self.logger.error(f"Source '{self.SOURCE_NAME}' not found in the database.")
            return None


        for pic in pictures_info:
            product_code = pic['source_id']
            image_url = pic['image_url']
            if not product_code or not image_url or not isinstance(product_code, int) or image_url in self.trash_image_url:
                self.logger.error(f"Invalid product code or image URL or trash image: {product_code}, {image_url}")
                continue

            # ext = os.path.splitext(urlparse(image_url).path)[1].lstrip('.') or 'jpg'
            ext = Path(urlparse(image_url).path).suffix.lstrip('.') or 'jpg'

            tasks.append(self.process_file(product_code=int(product_code), url=image_url, extension=ext))

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


    async def process_file(self, product_code: int, url: str, extension: str) -> Optional[Picture]:
        try:
            async with self.request_dispatcher.get(url=url, raise_on_status=False, log_info=False) as response:
                if response.status != 200:
                    return None
                content = await response.read()

            # Сформировать путь и сохранить файл
            path = await self.download_file(source_folder=self.source_folder, product_code=product_code, extension=extension, content=content)

            if not path or path in self.unique_urls:
                return None

            self.unique_urls.add(path)

            return await Picture.fill_picture(
                source_id=product_code,
                image_url=url,
                path=path
            )
        except Exception as e:
            self.logger.exception(f"Failed to process file {url}: {e}")
            return None

    async def download_file(self, source_folder: str, product_code: int, extension: str, content: [str | bytes]) -> Optional[str]:
        folder = await self.__generate_uuid(str(product_code))
        filename = await self.__generate_uuid(str(content))
        path = await self.file.write_file(
            filename=str(f"{filename}.{extension}"),
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
        return [u for u in urls if u is not None]