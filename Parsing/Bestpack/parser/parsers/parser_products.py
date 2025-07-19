import asyncio
import hashlib
import re
from logging import Logger
from typing import Optional

from scrapyx.clients import Requests, PostgreSQL
from scrapyx.utils import normalize_text
from scrapyx.base import BaseScraperSync

from models import Product


class ParserProducts(BaseScraperSync):
    BATCH_SIZE_ASYNC = 10
    BATCH_SIZE_INSERT = 500
    URL = "https://bestpack.kz"

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.check_product_url_for_duplicate = set()

    async def parse(self, category_name_id_map: dict, full_category_urls: list) -> list:
        if not full_category_urls or not category_name_id_map:
            raise Exception("No full_category_urls or category_name_id_map provided, exiting.")

        all_links = await self.extract_pages(full_category_urls=full_category_urls)

        # parsing products
        full_product_urls = []
        await self.extract_products(category_link=all_links,product_category_id=category_name_id_map, full_product_urls=full_product_urls)

        # return product_url
        return list(set(full_product_urls))

    async def extract_pages(self, full_category_urls: list) -> list:
        self.logger.info("Starting to extract pages...")
        all_pages = full_category_urls.copy()
        seen = set(all_pages)

        for category_url in full_category_urls:
            async with self.request_dispatcher.get(category_url) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {category_url}: {response.status}")
                    continue
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            pag_items = soup.select('ul.pagination li.pag a[href]')
            if not pag_items:
                continue

            for a_tag in pag_items:
                href = a_tag.get('href')
                if not href:
                    continue
                full_url = f"{self.URL}{href}"

                if full_url not in seen:
                    seen.add(full_url)
                    all_pages.append(full_url)
                    self.logger.info(f"Added new page to the list: {full_url}")

        return all_pages

    async def extract_products(self, category_link: list, product_category_id: dict, full_product_urls: list) -> None:
        self.logger.info("Starting to extract products...")
        all_products = []
        #  0 -> left_index    len(product_pages) -> size  self.BATCH_SIZE_ASYNC -> range to jump
        for i in range(0, len(category_link), self.BATCH_SIZE_ASYNC):
            batch_pages = category_link[i: i + self.BATCH_SIZE_ASYNC]
            tasks = [self.parse_single_category(page=page, product_category_id=product_category_id, full_product_urls=full_product_urls) for page in batch_pages]
            products = await asyncio.gather(*tasks)

            for product in products:
                if product:
                    all_products.extend(product)

            if len(all_products) >= self.BATCH_SIZE_INSERT:
                await self.db.insert_batch(all_products)
                all_products.clear()

        if all_products:
            await self.db.insert_batch(all_products)
        # Info
        self.logger.info("Products extracted and inserted into the database successfully!")
        return None

    async def parse_single_category(self, page: str, product_category_id: dict, full_product_urls: list) -> list:
        try :
            async with self.request_dispatcher.get(page) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {page}: {response.status}")
                    return []
                html_text = await response.text()

            soup = self.get_bs4_object(content=html_text, markup='html.parser')

            # 1. category name
            category_link = soup.select_one("body > div.section.page > div:nth-child(1) > ul > li:nth-child(3) > a")
            if not category_link or not category_link.text:
                self.logger.warning(f"No category link found for page {page}")
                return []

            category_name = normalize_text(text=category_link.text, case='u') if category_link else None

            if not category_name:
                self.logger.error(f"Failed to extract category name from page {page}")
                return []

            category_id = product_category_id.get(category_name)
            if not category_id:
                self.logger.warning(f"Category '{category_name}' not found in product_category_id")
                return []

            # 2. find cards with products
            card_blocks = soup.select("div.product_item.share_item")
            if not card_blocks:
                self.logger.warning(f"No product cards found on page {page}")
                return []

            products = []

            for card_block in card_blocks:
                # 1. title
                title_tag = card_block.select_one('a.product_name')
                if not title_tag or not title_tag.text:
                    self.logger.warning(f"No title found for source_id: {card_block}")
                    continue
                title = normalize_text(text=title_tag.text, case='u')

                # 2. overall pack price
                overall_pack_price = card_block.select_one('div.product_total_price')
                if not overall_pack_price or not overall_pack_price.text:
                    self.logger.warning(f"No overall pack price found! name product: {title}")
                    continue

                overall_pack_price = overall_pack_price.text.replace('тг', '').strip()
                if not overall_pack_price:
                    self.logger.warning(f"No overall pack price found for product: {title}")
                    continue

                # 3. overall box price
                overall_box_price = card_block.select_one('div.amount_block[data-type="box"]')
                if not overall_box_price:
                    self.logger.warning(f"No overall box price found for product: {title}")

                overall_box_price = overall_box_price.get('data-box-price') or None
                if not overall_box_price:
                    self.logger.warning(f"No overall box price found for product: {title}")
                    continue

                # 4. per price curr
                per_price = card_block.select_one('div.share_price span.share_price_current')
                if not per_price or not per_price.text:
                    self.logger.warning(f"No per price found for product : {title}")
                    continue
                per_price = per_price.text.strip()

                per_price = re.search(r'\d+', per_price).group(0)
                if not per_price:
                    self.logger.warning(f"No overall pack price found for product: {title}")
                    continue

                # 5. per discount price
                per_discount_price = card_block.select_one('div.share_price span.share_price_old')

                if not per_discount_price or not per_discount_price.text:
                    per_discount_price = None
                else:
                    per_discount_price = float(re.search(r'\d+', per_discount_price.text).group(0))
                    if per_discount_price > float(per_price):
                        # switch prices
                        temp = per_discount_price
                        per_discount_price = per_price
                        per_price = temp

                # 6. link to product
                product_url = card_block.select_one('a.share_img').get('href')
                if not product_url:
                    self.logger.warning(f"No product URL found for product: {title}")
                    continue

                # Check for duplicates in product URLs
                product_url_check = re.sub(r'.*(?=/products/)', '', product_url)
                if product_url_check in self.check_product_url_for_duplicate or not product_url_check:
                    continue
                self.check_product_url_for_duplicate.add(product_url_check)

                # Product URL hash
                product_url_hash = self.hash_string(string=product_url_check)
                if not product_url_hash:
                    self.logger.error(f"Failed to hash product URL: {product_url_check}")
                    continue

                # 7. source_code
                source_code = card_block.select_one('div.product_articul')
                if source_code and source_code.text:
                    source_code = source_code.text.replace('Арт.', '').strip() or None
                else:
                    source_code = None

                product = Product(
                    source_code=source_code,
                    title=title,
                    category_id=category_id,
                    overall_pack_price = float(overall_pack_price),
                    overall_box_price=float(overall_box_price),
                    per_price=float(per_price),
                    per_discount_price=per_discount_price,
                    product_url=f"{self.URL}{product_url}",
                    product_url_hash = product_url_hash
                )
                products.append(product)
                full_product_urls.append(f"{self.URL}{product_url}")

            return products
        except Exception as e:
            self.logger.error(f"Error while parsing single category {page}: {e}")
            return []

    @staticmethod
    def hash_string(string, algorithm="sha256") -> Optional[str]:
        if not isinstance(string, str):
            return None

        hasher = hashlib.new(algorithm)
        hasher.update(string.encode('utf-8'))
        return hasher.hexdigest()
