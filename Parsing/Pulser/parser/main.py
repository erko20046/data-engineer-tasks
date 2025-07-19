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

    product_category = await parser_categories.parse()
    product_url = await parser_product.parse(product_category_id=product_category)

    await parser_characteristic_and_pictures.parse(product_url=product_url)

    await postgresql_parsing.parsed_successfully()


if __name__ == '__main__':
    asyncio.run(main())