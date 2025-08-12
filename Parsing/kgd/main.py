import asyncio

from grpc import aio
from grpc_reflection.v1alpha import reflection
from scrapyx import ClientFactory

from settings import configurations
from parsers import IndividualParser, TaxpayerParser
from mappers import IndividualMapper, TaxpayerMapper
from services import IndividualService, TaxpayerService
from protos import kgdindividual_pb2, kgdindividual_pb2_grpc, kgdtaxpayer_pb2_grpc, kgdtaxpayer_pb2


async def main():
    factory = ClientFactory(config_path='config.json')

    logger = factory.clients.logger
    nca_node = factory.clients.nca_node
    request_dispatcher = factory.clients.requests
    captcha_solver = factory.clients.captcha

    individual_parser = IndividualParser(
        nca_node=nca_node,
        request_dispatcher=request_dispatcher,
        captcha_solver=captcha_solver,
        logger=logger
    )

    taxpayer_parser = TaxpayerParser(
        request_dispatcher=request_dispatcher,
        captcha_solver=captcha_solver,
        logger=logger
    )

    individual_mapper = IndividualMapper()
    taxpayer_mapper = TaxpayerMapper()

    individual_service = IndividualService(
        individual_parser=individual_parser,
        individual_mapper=individual_mapper,
        logger=logger
    )

    taxpayer_service = TaxpayerService(
        taxpayer_parser=taxpayer_parser,
        taxpayer_mapper=taxpayer_mapper,
        logger=logger
    )

    server = aio.server()

    kgdindividual_pb2_grpc.add_KgdIndividualServicer_to_server(individual_service, server)
    kgdtaxpayer_pb2_grpc.add_TaxpayerScraperServicer_to_server(taxpayer_service, server)

    SERVICE_NAMES = (
        kgdindividual_pb2.DESCRIPTOR.services_by_name['KgdIndividual'].full_name,
        kgdtaxpayer_pb2.DESCRIPTOR.services_by_name['TaxpayerScraper'].full_name,
    )

    reflection.enable_server_reflection(SERVICE_NAMES, server)
    listen_addr = "[::]:" + configurations.port
    server.add_insecure_port(listen_addr)

    logger.info(f"Starting server on {listen_addr}\n")

    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(main())
