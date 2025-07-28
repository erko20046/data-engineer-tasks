from logging import Logger
from itertools import count
from typing import Optional
from scrapyx.base import BaseScraperSync
from scrapyx.clients import Requests, PostgreSQL
from scrapyx.utils import normalize_text

from models import Category


class ParserCategory(BaseScraperSync):
    CATALOG_URL = "https://upack.kz/api/v1/catalog/"
    URL = "https://upack.kz"
    PAGE = 'page=1&per_page=48'

    def __init__(self, request_dispatcher: Requests, logger: Logger, database: PostgreSQL):
        self.request_dispatcher: Requests = request_dispatcher
        self.logger: Logger = logger
        self.db: PostgreSQL = database
        self.category_urls = []

    async def parse(self) -> tuple[dict[str, int], list[str]]:
        self.logger.info("Starting to extract category ...")

        async with self.request_dispatcher.get(self.CATALOG_URL) as response:
            data = await response.json()

        category_name_id_map = {}
        categories = []
        category_id_counter = count(start=1)

        await self.extract_recursive(data=data, categories=categories, category_id_counter=category_id_counter, parent_id=None, priority=1, category_name_id_map=category_name_id_map)

        # Insert in DB
        await self.db.insert_batch(data=categories)

        return category_name_id_map, self.category_urls

    async def extract_recursive(self, data: list[dict], categories: list[Category], category_id_counter: count, parent_id: Optional[int], priority: int, category_name_id_map: dict) -> None:
        for node in data:
            title = node.get("title", "")
            if not title:
                continue

            category_name = normalize_text(title, case="u")

            url_path = node.get("path")
            if not url_path:
                continue
            full_url = f"{self.URL}{url_path}?{self.PAGE}"

            current_id = next(category_id_counter)
            category_name_id_map[category_name] = current_id
            if priority == 1:
                self.category_urls.append(full_url)

            categories.append(Category(
                category_id=current_id,
                parent_id=parent_id,
                name=category_name,
                priority=priority,
                category_url=full_url
            ))

            children = node.get("children", [])
            if children:
                await self.extract_recursive(data=children, categories=categories, category_id_counter=category_id_counter, parent_id=current_id, priority=priority + 1, category_name_id_map=category_name_id_map)
