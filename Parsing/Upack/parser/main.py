import asyncio
from scrapyx import ClientFactory
from parsers import ParserCategory, ParserProducts


async def main():
    factory = ClientFactory(config_path='config.json')

    logger = factory.clients.logger
    requests = factory.clients.requests
    postgresql_parsing = factory.clients.postgresql.postgresql_parsing
    files = factory.clients.files

    parser_categories = ParserCategory(
        logger=logger,
        request_dispatcher=requests,
        database=postgresql_parsing
    )

    parser_product = ParserProducts(
        logger=logger,
        request_dispatcher=requests,
        database=postgresql_parsing,
        files=files
    )

    await postgresql_parsing.inspect_parser_status()

    category_name_id_map, category_urls = await parser_categories.parse()

    if not category_urls or not category_name_id_map:
        raise Exception('No parser category urls or category_name_id_map provided')

    await parser_product.parse(category_urls=category_urls, category_name_id_map=category_name_id_map)

    await postgresql_parsing.parsed_successfully()


if __name__ == '__main__':
    asyncio.run(main())
