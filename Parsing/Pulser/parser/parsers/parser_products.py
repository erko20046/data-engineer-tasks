import asyncio
import re
from logging import Logger

from bs4 import BeautifulSoup
from scrapyx.clients import Requests, PostgreSQL
from scrapyx.utils import normalize_text
from scrapyx.base import BaseScraperSync

from models import Product


class ParserProducts(BaseScraperSync):
    BATCH_SIZE_ASYNC = 10
    BATCH_SIZE_INSERT = 500
    url = "https://pulser.kz"

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.product_category_id = dict()
        self.product_url = []

    async def parse(self, product_category_id) -> list:
        async with self.request_dispatcher.get(self.url) as response:
            if not response.ok:
                self.logger.error("Failed to get URL {}".format(self.url))
                return []
            response = await response.text()

        # link to categories
        category_links = await self.extract_category_links(response)
        if not category_links:
            self.logger.exception("No category links found, exiting.")
            return []

        self.logger.info(f"category_links : {category_links}")

        # parsing products
        await self.extract_products(product_category_page=category_links,product_category_id=product_category_id)

        # return product_url
        return self.product_url


    async def extract_category_links(self, html_text) -> list:
        soup = self.get_bs4_object(html_text, 'html.parser')

        # take category links
        category_links = soup.select("li.nav-item.dropdown div.dropdown-menu a.nav-link")
        if not category_links:
            self.logger.error(f"Failed to get categories from: {html_text}")
            return []

        all_categories = []
        for category in category_links:
            curr_cat = category.get('href')
            if not curr_cat:
                self.logger.error(f"Failed to get categories from: {category}")
                continue

            all_categories.append(f"https://pulser.kz{curr_cat}?limit=0")

        # return list
        return all_categories


    async def extract_products(self, product_category_page: list, product_category_id: dict) -> None:
        self.product_category_id = product_category_id
        self.logger.info("Starting to extract products...")
        all_products = []

        #  0 -> left_index    len(product_pages) -> size  self.BATCH_SIZE_ASYNC -> range to jump
        for left_index in range(0, len(product_category_page), self.BATCH_SIZE_ASYNC):

            batch_pages = product_category_page[left_index: left_index + self.BATCH_SIZE_ASYNC]
            tasks = [self.parse_single_category(page) for page in batch_pages]
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

    async def parse_single_category(self, page: str) -> list:
        try :
            async with self.request_dispatcher.get(page) as response:
                if not response.ok:
                    self.logger.error(f"Failed to fetch page {page}: {response.status}")
                    return []
                response = await response.text()

            soup = self.get_bs4_object(response, 'html.parser')

            # 1. category name
            category_link = soup.select_one('li.breadcrumb-item.active[aria-current="page"]')
            category_name = normalize_text(category_link.text, 'u') if category_link else None

            if not category_name:
                self.logger.error(f"Failed to extract category name from page {page}")
                return []

            category_id = self.product_category_id.get(category_name)
            if not category_id:
                self.logger.warning(f"Category '{category_name}' not found in product_category_id")
                return []

            # 2. find cards with products
            card_blocks = soup.select("div.card-deck.card-tiles")
            if not card_blocks:
                self.logger.warning(f"Sending soup to method : parse_special_category")
                # Special category html handling
                return await self.parse_special_category(soup= soup)

            products = []

            for card_block in card_blocks:
                # data-key → Needed to extract price
                data_key = card_block.get("data-key")
                if not data_key:
                    self.logger.warning("Card block missing data-key")
                    continue

                card = card_block.select_one("div.card")
                if not card:
                    self.logger.warning("Missing .card inside .card-deck")
                    continue

                # 1. link to product
                link_to_product = card.select_one("a[href^='/product/']")
                if not link_to_product:
                    self.logger.warning("No product link found in card")
                    continue

                link_to_source = link_to_product.get("href")
                match = re.search(r"-(\d+)$", link_to_source)
                if not match:
                    self.logger.warning(f"Can't extract source_id from href: {link_to_source}")
                    continue

                source_id = int(match.group(1))

                # 2. title
                title_tag = card.select_one("div.card-title a")
                if not title_tag:
                    self.logger.warning(f"No title found for source_id: {source_id}")
                    continue
                title = normalize_text(title_tag.text.strip(), 'u')

                # 3. price
                price_tag = soup.select_one(f"span.dvizh-shop-price.dvizh-shop-price-{data_key}")
                price_text = re.sub(r"\s+", "", price_tag.text) if price_tag else None

                if not price_text or not price_text.isdigit():
                    self.logger.warning(f"No price or invalid price for data-key {data_key} (source_id: {source_id})")
                    continue

                # 4. URL to product
                product_url = f"{self.url}{link_to_source}"

                if not product_url:
                    self.logger.warning(f"Product URL is empty for source_id: {source_id}")
                    continue

                product = await Product.fill_products(
                    source_id=source_id,
                    title=title,
                    category_id=category_id,
                    price=int(price_text),
                    product_url=product_url
                )
                products.append(product)
                self.product_url.append(product_url)

            return products
        except Exception as e:
            self.logger.error(f"Error while parsing single category {page}: {e}")
            return []


    async def parse_special_category(self, soup: BeautifulSoup) -> list:
        try :
            # link to category name (String)
            category_link = soup.select_one('li.breadcrumb-item.active[aria-current="page"]')
            category_name = normalize_text(category_link.text.strip(), 'u') if category_link else None

            if not category_name:
                self.logger.error(f"Failed to extract category name from soup in method : parse_special_category")
                return []

            # get category id from product_category
            category_id = self.product_category_id.get(category_name)

            if not category_id:
                self.logger.warning(f"Not found category_id for category in method : parse_special_category {category_name}")
                return []

            # take elements
            cards = soup.select("div.maincard")
            if not cards:
                self.logger.warning(f"No product cards found in soup im method : parse_special_category {category_name} >> reason : {soup.select_one('#w1 > div').text}")
                return []

            products = []

            for card in cards:
                # take product code
                product_code_link = card.select_one("div.col-6.mcard-small > small")
                product_code = re.search(r"(\d+)", product_code_link.text) if product_code_link else None

                if not product_code:
                    self.logger.warning(f"No product code found on card method : parse_special_category {card}")
                    continue

                product_code = int(product_code.group(1))

                # 2. take title
                title_link = card.select_one("div.card-title a[href^='/product/']")
                if not title_link:
                    self.logger.warning(f"No product title found on card in method : parse_special_category {card}")
                    continue

                title = title_link.text.strip()

                # 3. take price
                price_link = card.select_one("span.dvizh-shop-price")
                price = re.sub(r"\s+", "", price_link.text) if price_link else None
                if not price:
                    self.logger.warning(f"No price found on card in method : parse_special_category  {card}")
                    continue

                # 4. take product_url
                product_name = title_link.get('href')
                if not product_name:
                    self.logger.warning(f"No product name found on card in method : parse_special_category {card}")
                    continue

                product_url = f"{self.url}{product_name}"

                product = await Product.fill_products(
                    source_id=product_code,
                    title=title,
                    category_id=category_id,
                    price=int(price),
                    product_url=product_url
                )
                products.append(product)
                self.product_url.append(product_url)

            return products
        except Exception as e:
            self.logger.error(f"Error while parsing special category: {e}")
            return []
