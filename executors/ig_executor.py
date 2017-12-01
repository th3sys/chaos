import aiohttp
import asyncio
import async_timeout
import json
import os
import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import functools
import typing


class StoreManager(object):
    def __init__(self, logger):
        self.__logger = logger

    async def GetSecurities(self):
        loop = asyncio.get_event_loop()
        try:
            self.__logger.info('Calling securities query ...')
            response: typing.Mapping = \
                await loop.run_in_executor(  # type: ignore
                    None, functools.partial(
                        self.__Securities.query,
                        KeyConditionExpression=Key('Symbol').eq('VX') & Key('Broker').eq('IG')))
        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return 'Security', None
        except Exception as e:
            self.__logger.error(e)
            return 'Security', None
        return 'Security', response['Items']

    async def __aenter__(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.__Securities = db.Table('Securities')
        self.__logger.info('StoreManager created')
        return self

    async def __aexit__(self, typ, value, traceback):
        self.__logger.info('StoreManager destroyed')


class IGClient:
    """IG client."""

    def __init__(self, identifier, password, url, key, logger, loop=None):
        self.__timeout = 10
        self.__logger = logger
        self.__id = identifier
        self.__password = password
        self.__url = url
        self.__key = key
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    async def GetEncryptionKey(self):
        try:
            url = '%s/%s' % (self.__url, 'session/encryptionKey')
            with async_timeout.timeout(self.__timeout):
                async with self.__session.get(url=url) as response:
                    return await response.json()
        except Exception as e:
            self.__logger.error('GetEncryptionKey: %s, %s' % (self.__url, e))
            return None

    async def Login(self):
        try:
            url = '%s/%s' % (self.__url, 'session')
            with async_timeout.timeout(self.__timeout):
                authenticationRequest = {
                    'identifier': self.__id,
                    'password': self.__password,
                    'encryptedPassword': None
                }
                self.__logger.info('Calling authenticationRequest ...')
                async with self.__session.post(url=url, json=authenticationRequest) as response:
                    payload = await response.json()
                    return 'Login', payload
        except Exception as e:
            self.__logger.error('Login: %s, %s' % (self.__url, e))
            return 'Login', None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(verify_ssl=False)
        self.__session = aiohttp.ClientSession(loop=self.__loop, connector=connector,
                                               headers={'X-IG-API-KEY': self.__key})
        self.__logger.info('Session created')
        return self

    async def __aexit__(self, typ, value, traceback):
        self.__session.close()
        self.__logger.info('Session destroyed')


class Scheduler:
    def __init__(self, logger, loop=None):
        self.__timeout = 10
        self.__logger = logger
        self.__store = None
        self.__client = None
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    async def __aenter__(self):
        self.__logger.info('Scheduler created')
        return self

    async def __aexit__(self, typ, value, traceback):
        self.__logger.info('Scheduler destroyed')

    def AddWorkers(self, client, store):
        self.__client = client
        self.__store = store

    def SendEmail(self, text):
        pass

    async def RunTasks(self):
        futures = [self.__client.Login(), self.__store.GetSecurities()]
        done, _ = await asyncio.wait(futures, timeout=self.__timeout)

        for future in done:
            name, payload = future.result()
            self.__logger.info(payload)
            if name == 'Login' and (payload is None or 'errorCode' in payload):
                self.SendEmail('There was a problem logging into IG')
                return
            if name == 'Security' and (payload is None or len(payload) != 1):
                self.SendEmail('There was a problem getting security definition for VX')
                return
            if name == 'Security':
                vix = payload[0]
                maxPosition = vix['Risk']['MaxPosition']
                riskFactor = vix['Risk']['RiskFactor']
                enabled = vix['TradingEnabled']
                self.__logger.info('MaxPosition is {}, RiskFactor is {}, Enabled {}'.format(maxPosition, riskFactor, enabled))
            if name == 'Login':
                funds = payload['accountInfo']['available']
                self.__logger.info('Funds {}'.format(funds))


async def main(loop, logger):
    url = os.environ['IG_URL']
    key = os.environ['X-IG-API-KEY']
    identifier = os.environ['IDENTIFIER']
    password = os.environ['PASSWORD']

    async with IGClient(identifier, password, url, key, logger, loop) as client:
        async with StoreManager(logger) as store:
            async with Scheduler(logger, loop) as scheduler:
                scheduler.AddWorkers(client, store)
                await scheduler.RunTasks()
                logger.info('RunTasks completed')


def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)

    if 'IG_URL' not in os.environ or 'X-IG-API-KEY' not in os.environ or\
            'IDENTIFIER' not in os.environ or 'PASSWORD' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger))
    app_loop.close()

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    lambda_handler(None, None)
