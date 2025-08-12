from logging import Logger

import grpc

from mappers import TaxpayerMapper
from parsers import TaxpayerParser
from parsers import DataNotFoundException
from protos import kgdtaxpayer_pb2, kgdtaxpayer_pb2_grpc


class TaxpayerService(kgdtaxpayer_pb2_grpc.TaxpayerScraperServicer):
    def __init__(
            self,
            taxpayer_parser: TaxpayerParser,
            taxpayer_mapper: TaxpayerMapper,
            logger: Logger
    ):
        self.taxpayer_parser: TaxpayerParser = taxpayer_parser
        self.taxpayer_mapper: TaxpayerMapper = taxpayer_mapper
        self.logger: Logger = logger

    async def LoadData(self, request, context):
        uin_or_name = request.uin_or_name
        type_query = request.mode
        try:
            self.logger.info(f'Received request for TaxpayerService with uin_or_name {uin_or_name} and type {type_query}', extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerService', 'type': type_query})

            parsed_data: list = await self.taxpayer_parser.parse(uin_or_name=uin_or_name, type_query=type_query)
            reply: kgdtaxpayer_pb2.LoadDataReply = await self.taxpayer_mapper.map(parsed=parsed_data)
            self.logger.info(f'Data was parsed successfully for TaxpayerService with {uin_or_name} and type {type_query}', extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerService', 'type': type_query})
            return reply

        except DataNotFoundException as not_found_exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(not_found_exc))
            self.logger.info(f'Data was not found for TaxpayerService with uin_or_name {uin_or_name} and type {type_query}', extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerService', 'type': type_query})
            return context

        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            self.logger.exception(e, extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerService'})
            self.logger.exception(f'Unknown exception for TaxpayerService with uin_or_name {uin_or_name} and type {type_query}', extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerService', 'type': type_query})
            return context
