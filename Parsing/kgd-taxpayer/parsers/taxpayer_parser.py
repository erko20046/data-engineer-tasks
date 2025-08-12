from json import JSONDecodeError
from logging import Logger
from typing import Optional

from scrapyx.clients import Requests, Captcha
from scrapyx.utils import normalize_text
from .exceptions import DataNotFoundException, EmptyResponseSourceException, CorruptedJsonException, WrongDataFormatException, InconsistentRequest


class TaxpayerParser:
    GOOGLE_KEY = '6LfqTn0rAAAAAGwdZJCgo7x6d7dhwEWovkGNfi_r'
    TYPE = {
        'IP' : 'ИП',
        'UL' : 'ЮЛ',
        'PP' : 'Частный судебный исполнитель'
    }
    PP = 'LZCHP'

    def __init__(self, request_dispatcher: Requests, captcha_solver: Captcha, logger: Logger):
        self.logger: Logger = logger
        self.request_dispatcher: Requests = request_dispatcher
        self.captcha_solver: Captcha = captcha_solver

    async def parse(self, uin_or_name: str, type_query: str) -> Optional[list]:
        if not uin_or_name or not type_query or not isinstance(type_query, str) or not isinstance(uin_or_name, str) or not uin_or_name.strip() or not type_query.strip():
            raise InconsistentRequest()

        type_query = type_query.upper()
        if type_query not in self.TYPE or len(type_query) != 2:
            raise Exception(f"Type query: '{type_query}' is not supported!")

        url = f"https://portal.kgd.gov.kz/services/isnaportalsync/open-api/taxpayer-data?taxpayerCode={uin_or_name}&taxpayerType={type_query}" if type_query != 'PP' else f"https://portal.kgd.gov.kz/services/isnaportalsync/open-api/taxpayer-data?taxpayerCode={uin_or_name}&taxpayerType={self.PP}"

        # Источник ебанный может вернуть пустой ответ, даже если ответ должен быть. Поэтому делаем несколько попыток. На всякий случай.
        data = None

        for idx in range(3):
            token = await self.captcha_solver.get_recaptcha_v3(url=url, google_key=self.GOOGLE_KEY, min_score=0.5, action='demo', service_name='KGD')

            headers = {
                'recaptchatoken' : token,
                'referer' : 'https://portal.kgd.gov.kz/ru/pages/info-services/find-taxpayer',
                'x-portal-language' : 'ru',
            }

            try:
                async with self.request_dispatcher.get(url=url, headers=headers, raise_on_status=False, log_info=False) as response:
                    if not response:
                        raise EmptyResponseSourceException()

                    try:
                        response = await response.json()
                    except JSONDecodeError:
                        raise CorruptedJsonException()
                    except Exception as exc:
                        raise exc

                data = response.get('taxpayerPortalSearchResponses')

                if not data:
                    self.logger.warning(f"Data not found for uin_or_name: {uin_or_name} and type: {type_query} with attempt {idx + 1}", extra={'uin_or_name': uin_or_name, 'service': 'TaxpayerParser', 'type': type_query})
                    raise DataNotFoundException()

                break

            except DataNotFoundException:
                continue
            except Exception as exc:
                raise exc

        if not data:
            raise DataNotFoundException()
        if not isinstance(data, list):
            raise WrongDataFormatException(format_type='list')

        result = await self.dictionary_map_general(data=data, type_query=type_query)
        if not result:
            raise DataNotFoundException('Result is empty')

        return result

    async def dictionary_map_general(self, data: list, type_query: str) -> Optional[list]:
        if type_query == 'PP':
            return await self.dictionary_map_pp(data=data, type_query=type_query)

        result = []

        for item in data:
            if not isinstance(item, dict):
                raise WrongDataFormatException(format_type='dict')

            name = item.get('name')
            begin_date = item.get('beginDate')
            code = item.get('code')

            if not name or not begin_date or not code:
                raise Exception('Not all essential fields are present in the response')

            end_date = item.get('endDate') if item.get('endDate') else None
            end_reason = normalize_text(item.get('endReason').get('ru'), case='u') if item.get('endReason') and isinstance(item.get('endReason'), dict) and item.get('endReason').get('ru') else None

            period = None
            add_info = item.get('additionalInfo')
            if add_info and isinstance(add_info, dict):
                begin_date = add_info.get('beginDate')
                end_date = add_info.get('endDate')
                if begin_date and end_date and isinstance(begin_date, str) and isinstance(end_date, str) and begin_date.strip() and end_date.strip():
                    period = f"{begin_date} - {end_date}"

            result.append({
                'name' : normalize_text(name, case='u'),
                'begin_date' : begin_date,
                'code' : code.strip(),
                'end_date' : end_date,
                'end_reason' : end_reason,
                'type' : self.TYPE.get(type_query),
                'period' : period
            })
        return result

    async def dictionary_map_pp(self, data: list, type_query: str) -> Optional[list]:
        result = []

        for item in data:
            if not isinstance(item, dict):
                raise WrongDataFormatException(format_type='dict')

            full_name = item.get('fullName')
            if not full_name or not isinstance(full_name, dict):
                raise Exception('Full name is not present or not in the correct format')

            last_name = normalize_text(full_name.get('lastName'), case='u') if full_name.get('lastName') else None
            first_name = normalize_text(full_name.get('firstName'), case='u') if full_name.get('firstName') else None

            if not last_name or not first_name:
                raise Exception('Not all essential fields of full name are present in the response')
            name = f"{last_name} {first_name}"

            middle_name = normalize_text(full_name.get('middleName'), case='u') if full_name.get('middleName') else None
            if middle_name:
                name += f" {middle_name}"

            code = item.get('code')
            if not code or not isinstance(code, str):
                raise Exception('Code is not present or not in the correct format')

            lzchp_types = item.get('lzchpTypes')[0] if item.get('lzchpTypes') and isinstance(item.get('lzchpTypes'), list) else None
            if not lzchp_types or not isinstance(lzchp_types, dict):
                self.logger.warning(f"LZCHP type '{type_query}' is not supported!", extra={'uin_or_name': code, 'service': 'TaxpayerService', 'type': type_query})

            begin_date = lzchp_types.get('beginDate')
            if not begin_date or not isinstance(begin_date, str):
                self.logger.warning(f"Begin date is not present or not in the correct format", extra={'uin_or_name': code, 'service': 'TaxpayerService', 'type': type_query})

            end_date = lzchp_types.get('endDate') if lzchp_types.get('endDate') else None
            end_reason = normalize_text(lzchp_types.get('endReason').get('ru'), case='u') if lzchp_types.get('endReason') and isinstance(lzchp_types.get('endReason'), dict) and lzchp_types.get('endReason').get('ru') else None

            result.append({
                'name' : normalize_text(name, case='u'),
                'begin_date' : begin_date,
                'code' : code,
                'end_date' : end_date,
                'end_reason' : end_reason,
                'type' : self.TYPE.get(type_query)
            })
        return result