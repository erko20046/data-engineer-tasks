from logging import Logger

import grpc

from mappers import IndividualMapper
from parsers import IndividualParser
from parsers import DataNotFoundException, AuthorizationFailed
from protos import kgdindividual_pb2, kgdindividual_pb2_grpc


class IndividualService(kgdindividual_pb2_grpc.KgdIndividualServicer):
    def __init__(
            self,
            individual_parser: IndividualParser,
            individual_mapper: IndividualMapper,
            logger: Logger
    ):
        self.individual_parser: IndividualParser = individual_parser
        self.individual_mapper: IndividualMapper = individual_mapper
        self.logger: Logger = logger

    async def LoadData(self, request, context):
        iin = request.iin
        try:
            self.logger.info(f'Received request for IndividualService with iin {iin}', extra={'iin': iin, 'service': 'IndividualService'})

            parsed_data: dict = await self.individual_parser.parse(iin=iin)
            reply: kgdindividual_pb2.LoadDataReply = await self.individual_mapper.map(parsed=parsed_data)
            self.logger.info(f'Data was parsed successfully for IndividualService with {iin}', extra={'iin': iin, 'service': 'IndividualService'})
            return reply

        except DataNotFoundException as not_found_exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(not_found_exc))
            self.logger.info(f'Data was not found for IndividualService with iin {iin}', extra={'iin': iin, 'service': 'IndividualService'})
            return context

        except AuthorizationFailed as auth_exc:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(str(auth_exc))
            self.logger.exception(auth_exc, extra={'iin': iin, 'service': 'IndividualService'})
            self.logger.exception(f'Authorization error for IndividualService with iin {iin}', extra={'iin': iin, 'service': 'IndividualService'})
            return context

        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            self.logger.exception(e, extra={'iin': iin, 'service': 'IndividualService'})
            self.logger.exception(f'Unknown exception for IndividualService with iin {iin}', extra={'iin': iin, 'service': 'IndividualService'})
            return context
