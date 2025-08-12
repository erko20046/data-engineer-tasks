import base64
import json

from datetime import datetime
from logging import Logger
from typing import Optional
from zoneinfo import ZoneInfo

from aiohttp.web_exceptions import HTTPBadRequest
from scrapyx.clients import NCANode, Requests, Captcha
from scrapyx.utils import normalize_text

from settings import configurations
from .exceptions import AuthorizationFailed, DataNotFoundException, EmptyResponseSourceException, InconsistentData

class IndividualParser:
    TOKEN_URL = 'https://portal.kgd.gov.kz/services/isnaportal/open-api/register/generate-token'
    XML_VALIDATION_URL = "https://portal.kgd.gov.kz/services/isnaportal/open-api/kalkan/validate"
    LOGIN_URL = 'https://portal.kgd.gov.kz/site/ru/master/isna-portal-cms/_/idprovider/jwt/login'

    def __init__(self, nca_node: NCANode, request_dispatcher: Requests, captcha_solver: Captcha, logger: Logger):
        self.nca_node: NCANode = nca_node
        self.logger: Logger = logger
        self.request_dispatcher: Requests = request_dispatcher
        self.captcha_solver: Captcha = captcha_solver

    async def parse(self, iin: str):
        iin = str(iin).zfill(12)

        authorization = None

        for _ in range(3):
            authorization = await self.authorize()
            if authorization:
                break

            self.logger.error('Authorization failed')

        if not authorization:
            raise AuthorizationFailed('Authorization failed')

        url = f"https://portal.kgd.gov.kz/services/isnaportalsync/open-api/taxpayer-data?taxpayerCode={iin}&taxpayerType=FL"

        token = await self.captcha_solver.get_recaptcha_v3(url=f'https://portal.kgd.gov.kz/services/isnaportalsync/open-api/taxpayer-data?taxpayerCode={iin}&taxpayerType=FL', google_key='6LfqTn0rAAAAAGwdZJCgo7x6d7dhwEWovkGNfi_r', min_score=0.5, action='login', service_name='KGD')

        headers = {
            'Authorization': authorization,
            'recaptchaToken': token,
        }

        async with self.request_dispatcher.get(url=url, headers=headers, log_info=False, raise_on_status=False) as response:
            if not response:
                raise EmptyResponseSourceException()

            response = await response.json()

        data = response.get('taxpayerPortalSearchResponses')

        if not data:
            raise DataNotFoundException()

        last = None

        if isinstance(data, list):
            if len(data) > 1:
                for record in data:

                    if not last:
                        last = record
                        continue

                    if datetime.strptime(record.get('beginDate'), '%Y-%M-%d') >= datetime.strptime(last.get('beginDate'), '%Y-%M-%d'):
                        last = record

            else:
                last = data[0]
        else:
            last = data

        data = await self.dictionary_map(last=last)

        return data

    @staticmethod
    async def dictionary_map(last: dict) -> dict:
        data = {}

        if last.get('fullName'):
            data['first_name'] = normalize_text(last.get('fullName').get('firstName'), 'u') if last.get('fullName').get('firstName') else None
            data['middle_name'] = normalize_text(last.get('fullName').get('middleName'), 'u') if last.get('fullName').get('middleName') else None
            data['last_name'] = normalize_text(last.get('fullName').get('lastName'), 'u') if last.get('fullName').get('lastName') else None

        data['begin_date'] = last.get('beginDate') if last.get('beginDate') else None

        if not data['first_name'] or not data['last_name']:
            raise InconsistentData()

        return data

    async def authorize(self) -> Optional[str]:

        # Отправка запроса на первичную генерацию токена
        async with self.request_dispatcher.get(url=self.TOKEN_URL, log_info=False) as response:
            if not response:
                raise EmptyResponseSourceException('Generate token request failed or empty content was received')

            response = await response.text()

        session_time = self.get_session_time()

        xml_template = f'<body><text><registrationSessionToken>{response}</registrationSessionToken>{session_time} (Kazakhstan Time)</text></body>'

        signed = await self.nca_node.sign_xml(xml=xml_template)
        signed = f'<?xml version="1.0" encoding="UTF-8" standalone="no"?>{signed}'
        base_64_encoded = base64.b64encode(signed.encode()).decode()

        # Проверка подписанной XML
        async with self.request_dispatcher.post(self.XML_VALIDATION_URL, data=base_64_encoded, log_info=False) as response:
            if not response:
                raise EmptyResponseSourceException('Xml validation request failed or empty content was received')

            response = await response.json()

            if not response.get('result'):
                raise Exception(f'Invalid response from {self.XML_VALIDATION_URL}')

        payload = json.dumps({
            "action": "login",
            "signedData": base_64_encoded,
            "password": configurations.password,
            "login": configurations.login
        })

        # Авторизация
        try:
            async with self.request_dispatcher.post(url=self.LOGIN_URL, data=payload, log_info=False) as response:
                if not response:
                    raise EmptyResponseSourceException('Authorization request failed or empty content was received')

                response = await response.json()

        except HTTPBadRequest:
            self.logger.error('Bad Request authorizing. Login or password could not be verified')
            raise AuthorizationFailed('Login or password could not be verified')

        except Exception as e:
            raise e

        access_token, token_type = response.get('access_token'), response.get('token_type')

        authorization = f'{token_type} {access_token}' if token_type and access_token else None

        return authorization

    @staticmethod
    def get_session_time() -> str:
        tz = ZoneInfo("Asia/Almaty")
        now = datetime.now(tz)
        session_time = now.strftime(f"%a %b %d %Y %H:%M:%S GMT%z")

        return session_time