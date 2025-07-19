from logging import Logger

from bs4 import Tag
from scrapyx.base import BaseScraperSync
from scrapyx.clients import Requests, PostgreSQL
from scrapyx.utils import normalize_text

from models import Category

class ParserCategory(BaseScraperSync):
    URL = "https://bestpack.kz/products"

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.category_links_without_city = []

    async def parse(self) -> dict:
        async with self.request_dispatcher.get(self.URL) as response:
            if not response.ok:
                self.logger.exception(f"Failed to get categories from  {response.URL}: {response.status}")
            html_text = await response.text()


        category_name_id_map = {}
        categories_name = await self.extract_categories(html_text=html_text, category_name_id_map=category_name_id_map)
        # Insert
        await self.db.insert_batch(data=categories_name)
        # Info
        self.logger.info(f"Found {len(categories_name)} categories and inserted into database")
        return category_name_id_map


    async def extract_cities(self) -> list:
        async with self.request_dispatcher.get(self.URL) as response:
            if not response.ok:
                self.logger.exception(f"Failed to get categories from  {response.URL}: {response.status}")
            html_text = await response.text()

        soup = self.get_bs4_object(content=html_text, markup='html.parser')
        # default city : Astana (Nur-Sultan)
        full_category_urls = []
        for category in self.category_links_without_city:
            full_category_urls.append(f"https://bestpack.kz{category}")
        # take cities except Nur-Sultan
        cities = soup.select("div.city_list div.city_item:not([data-city='nur-sultan'])")
        if not cities:
            self.logger.error(f"Failed to get cities")
            return full_category_urls

        for city in cities:
            city_name = city.get('data-city')
            if not city_name:
                continue

            for category in self.category_links_without_city:
                full_category_urls.append(f"https://bestpack.kz/{city_name}{category}")

        return full_category_urls

    async def extract_categories(self, html_text: str, category_name_id_map: dict) -> list:
        soup = self.get_bs4_object(content=html_text, markup='html.parser')
        # take elements
        category_elements = soup.select("ul.catalog_cats_list > li")
        if not category_elements:
            self.logger.error(f"Failed to get categories: {html_text}")
            return []

        categories = []
        category_id = 1

        for category in category_elements:
            category_id = await self.extract_recursive(
                category_element=category,
                categories=categories,
                category_id=category_id,
                category_name_id_map=category_name_id_map
            )

        return categories

    async def extract_recursive(self, category_element: Tag, categories: list, category_id, category_name_id_map: dict) -> int:
        # take category
        category = category_element.select_one("a.cat_item")
        if not category or not isinstance(category.text, str):
            return category_id

        category_name = normalize_text(text=category.text, case='u')
        if not category_name or category_name == "ПОКАЗАТЬ ВСЕ":
            return category_id

        # take link to category
        link_to_category = category.get('href')
        if not link_to_category:
            return category_id
        self.category_links_without_city.append(link_to_category)

        categories.append(Category(
            category_id=category_id,
            name=category_name
        ))
        # take category_id for product
        category_name_id_map[category_name] = category_id

        return category_id + 1
