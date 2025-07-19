from logging import Logger
from scrapyx.base import BaseScraperSync
from scrapyx.clients import Requests, PostgreSQL
from scrapyx.utils import normalize_text

from models import Category

class ParserCategory(BaseScraperSync):
    url = "https://pulser.kz/"
    category_id_counter = 1

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.category_for_product = {}

    async def parse(self) -> dict:
        async with self.request_dispatcher.get(self.url) as response:
            if not response.ok:
                self.logger.exception(f"Failed to get categories from  {response.URL}: {response.status}")
            response = await response.text()


        categories = await self.extract_categories(response)
        # Insert
        await self.db.insert_batch(categories)

        # Info
        self.logger.info(f"Found {len(categories)} categories and inserted into database")

        return self.category_for_product


    async def extract_categories(self, html_text) -> list:
        soup = self.get_bs4_object(html_text, 'html.parser')

        # take elements
        category_elements = soup.select("li.nav-item.dropdown")
        if not category_elements:
            self.logger.error(f"Failed to get categories: {html_text}")
            return []

        categories = []
        for category in category_elements:
            await self.extract_recursive(category_element=category, parent_id=None, depth=1, categories=categories)

        return categories

    async def extract_recursive(self, category_element, parent_id, depth, categories) -> None:
        # take parent
        parent_category = category_element.select_one("a.nav-link")
        if not parent_category:
            return None

        parent_name = normalize_text(parent_category.text, 'u')
        if not parent_name:
            return None

        current_id = self.category_id_counter
        self.category_id_counter += 1

        categories.append(await Category.fill_category(
            category_id=current_id,
            parent_id=parent_id,
            name=parent_name,
            priority=depth
        ))

        self.category_for_product[parent_name] = current_id

        # take child
        child_category = category_element.select("ul.nav-cat li.nav-item")

        for child in child_category:
            await self.extract_recursive(category_element=child, parent_id=current_id, depth=depth + 1, categories=categories)
        return None
