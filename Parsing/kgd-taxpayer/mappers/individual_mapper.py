
from asyncio.log import logger
from datetime import datetime
from typing import Optional

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from protos import kgdindividual_pb2

class IndividualMapper:
    async def map(self, parsed: dict):
        reply = kgdindividual_pb2.Data(
            first_name=await self.str_or_null(parsed.get('first_name')) if parsed.get('first_name') else None,
            last_name=await self.str_or_null(parsed.get('last_name')) if parsed.get('last_name') else None,
            middle_name=await self.str_or_null(parsed.get('middle_name')) if parsed.get('middle_name') else None,
            begin_date=await self.get_timestamp(parsed.get('begin_date')) if parsed.get('begin_date') else None,
        )

        return kgdindividual_pb2.LoadDataReply(message='ok', data=reply)

    @staticmethod
    async def get_timestamp(date: str):
        """ Converts dates to google.Timestamp for .proto file. """
        if date:
            try:
                timestamp = Timestamp()
                date = datetime.strptime(date, "%Y-%m-%d")
                timestamp.FromDatetime(date)
                date = timestamp
                return date
            except Exception as e:
                logger.exception(e)
        return None

    @staticmethod
    async def str_or_null(value: str) -> Optional[StringValue]:
        return StringValue(value=value) if value else None
