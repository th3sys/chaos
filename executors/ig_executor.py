import aiohttp
import asyncio
import async_timeout
import json
import logging
import os


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
            self.__logger.info('GetEncryptionKey: %s, %s' % (self.__url, e))

    async def Login(self):
        try:
            url = '%s/%s' % (self.__url, 'session')
            with async_timeout.timeout(self.__timeout):
                authenticationRequest = {
                    'identifier': self.__id,
                    'password': self.__password,
                    'encryptedPassword': None
                }
                async with self.__session.post(url=url, json=authenticationRequest) as response:
                    return await response.json()
        except Exception as e:
            self.__logger.info('Login: %s, %s' % (self.__url, e))

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(verify_ssl=False)
        self.__session = aiohttp.ClientSession(loop=self.__loop, connector=connector,
                                               headers={'X-IG-API-KEY': self.__key})
        self.__logger.info('Session created')
        return self

    async def __aexit__(self, typ, value, traceback):
        self.__session.close()
        self.__logger.info('Session destroyed')


async def main(loop, logger):
    url = os.environ['IG_URL']
    key = os.environ['X-IG-API-KEY']
    identifier = os.environ['IDENTIFIER']
    password = os.environ['PASSWORD']
    async with IGClient(identifier, password, url, key, logger, loop) as client:
        accountInfo = await client.Login()
        logger.info(json.dumps(accountInfo))
        if accountInfo is None or 'errorCode' in accountInfo:
            logger.error('Could not logging for identifier: %s' % identifier)
            # send email
            return


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

