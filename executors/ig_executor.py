import aiohttp
import asyncio
import async_timeout
import json
import os
import boto3
import logging
from utils import Connection
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import functools
from functools import reduce
import typing


class Money(object):
    def __init__(self, amount, ccy):
        self.Ccy = ccy
        self.Amount = amount


class StoreManager(object):
    def __init__(self, logger, orders):
        self.__logger = logger
        self.__orders = orders

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
        self.__tokens = None
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    @Connection.ioreliable
    async def Logout(self):
        try:
            url = '%s/%s' % (self.__url, 'session')
            with async_timeout.timeout(self.__timeout):
                self.__logger.info('Calling Logout ...')
                response = await self.__connection.delete(url=url, headers=self.__tokens)
                self.__logger.info('Logout Response Code: {}'.format(response.status))
                return True
        except Exception as e:
            self.__logger.error('Logout: %s, %s' % (self.__url, e))
            return False

    @Connection.ioreliable
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
                response = await self.__connection.post(url=url, json=authenticationRequest)
                self.__logger.info('Login Response Code: {}'.format(response.status))
                self.__tokens = {'X-SECURITY-TOKEN': response.headers['X-SECURITY-TOKEN'],
                                 'CST': response.headers['CST']}
                payload = await response.json()
                return payload
        except Exception as e:
            self.__logger.error('Login: %s, %s' % (self.__url, e))
            return None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(verify_ssl=False)
        self.__session = aiohttp.ClientSession(loop=self.__loop, connector=connector,
                                               headers={'X-IG-API-KEY': self.__key})
        self.__connection = await self.__session.__aenter__()
        self.__logger.info('Session created')
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.__session.__aexit__(*args, **kwargs)
        self.__logger.info('Session destroyed')


class Scheduler:
    def __init__(self, identifier, password, url, key, logger, loop=None):
        self.__timeout = 10
        self.AllowedRisk = 0.01
        self.__logger = logger
        self.__id = identifier
        self.__password = password
        self.__url = url
        self.__key = key
        self.__store = None
        self.Balance = None
        self.__client = None
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    async def __aenter__(self):
        self.__client = IGClient(self.__id, self.__password, self.__url, self.__key, self.__logger, self.__loop)
        self.__connection = await self.__client.__aenter__()
        auth = await self.__connection.Login()
        self.Balance = Money(auth['accountInfo']['available'], auth['currencyIsoCode'])
        self.__logger.info('{}'.format(auth))
        self.__logger.info('Scheduler created')
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.__connection.Logout()
        await self.__client.__aexit__(*args, **kwargs)
        self.__logger.info('Scheduler destroyed')

    async def BalanceCheck(self, orders):
        try:
            totalPosition = reduce(lambda x, y: x+y, map(lambda x: float(x['Order']['M']['Size']['N']), orders))
            self.__logger.info('Balance {}. Position {}. Risk {}'
                               .format(self.Balance.Amount, totalPosition, totalPosition / self.Balance.Amount))
            return totalPosition / self.Balance.Amount < self.AllowedRisk
        except Exception as e:
            self.__logger.error('BalanceCheck Error: %s' % e)
            return False

    def SendEmail(self, text):
        pass

    async def RunTasks(self):
        futures = [self.__client.Login(), self.__store.GetSecurities()]
        done, _ = await asyncio.wait(futures, timeout=self.__timeout)

        for fut in done:
            name, payload = fut.result()
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
                self.__logger.info('MaxPosition is {}, RiskFactor is {}, Enabled {}'.format(maxPosition,
                                                                                            riskFactor, enabled))
            if name == 'Login':
                funds = payload['accountInfo']['available']
                self.__logger.info('Funds {}'.format(funds))


async def main(loop, logger, event):
    try:
        url = os.environ['IG_URL']
        key = os.environ['X-IG-API-KEY']
        identifier = os.environ['IDENTIFIER']
        password = os.environ['PASSWORD']

        orders = []
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                orderId = record['dynamodb']['Keys']['OrderId']['S']
                logger.info('New Order received OrderId: %s', orderId)
                orders.append(record['dynamodb']['NewImage'])
            else:
                logger.info('Not INSERT event is ignored')
        if len(orders) == 0:
            logger.info('No Orders. Event is ignored')
            return

        async with Scheduler(identifier, password, url, key, logger, loop) as scheduler:
            if not await scheduler.BalanceCheck(orders):
                scheduler.SendEmail('')
                return

    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)

    if 'IG_URL' not in os.environ or 'X-IG-API-KEY' not in os.environ or 'IDENTIFIER' not in os.environ \
            or 'PASSWORD' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, event))
    app_loop.close()

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    with open("event.json") as json_file:
        test_event = json.load(fp=json_file)
        lambda_handler(test_event, None)
