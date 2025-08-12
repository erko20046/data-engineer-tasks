import html
from asyncio.log import logger
from datetime import datetime
from typing import Optional
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

from protos import kgdtaxpayer_pb2

class TaxpayerMapper:
    async def map(self, parsed: list):
        reply = [
            kgdtaxpayer_pb2.Taxpayer(
                rnn = None,
                date_reg = await self.get_timestamp(item.get('begin_date')),
                date_unreg = await self.get_timestamp(item.get('end_date')),
                reason_unreg = await self.str_or_null(item.get('end_reason')),
                add_info = None,
                full_name = await self.str_or_null(item.get('name')),
                type = await self.str_or_null(item.get('type')),
                iin_bin = await self.str_or_null(item.get('code')),
                stop_period = await self.str_or_null(item.get('period')),
            ) for item in parsed
        ]

        return kgdtaxpayer_pb2.LoadDataReply(message='ok', data=kgdtaxpayer_pb2.TaxpayerData(taxpayers=reply))

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
    async def str_or_null(value: Optional[str]) -> Optional[StringValue]:
        if value:
            value = html.unescape(value).strip()
            return StringValue(value=value)
        else:
            return None
