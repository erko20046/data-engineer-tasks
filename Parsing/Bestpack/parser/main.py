import asyncio
from scrapyx import ClientFactory
from parsers import ParserCategory, ParserProducts, ParserCharacteristicAndPicture


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

    parser_characteristic_and_pictures = ParserCharacteristicAndPicture(
        logger=logger,
        request_dispatcher=requests,
        database=postgresql_parsing,
        files=files
    )

    parser_product = ParserProducts(
        logger=logger,
        request_dispatcher=requests,
        database=postgresql_parsing
    )

    await postgresql_parsing.inspect_parser_status()

    category_name_id_map = await parser_categories.parse()
    full_category_urls = await parser_categories.extract_cities()

    product_urls = await parser_product.parse(category_name_id_map= category_name_id_map, full_category_urls=full_category_urls)

    await parser_characteristic_and_pictures.parse(product_urls=product_urls)

    await postgresql_parsing.parsed_successfully()

if __name__ == '__main__':
    asyncio.run(main())