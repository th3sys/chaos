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
import smtplib
from utils import Connection
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import reduce
import copy
import decimal


class Side:
    Buy = 'BUY'
    Sell = 'SELL'


class OrderStatus(object):
    Filled = 'FILLED'
    Pending = 'PENDING'
    Failed = 'FAILED'


class IGParams(object):
    def __init__(self):
        self.Url = ''
        self.Key = ''
        self.Identifier = ''
        self.Password = ''
        self.EAddress = ''
        self.EUser = ''
        self.EPassword = ''
        self.ESmtp = ''


class Order(object):
    def __init__(self, orderId, transactionTime, symbol, side, size, ordType, maturity, name, group, risk, maxPos):
        self.OrderId = orderId
        self.TransactionTime = transactionTime
        self.Side = side
        self.Size = float(size)
        self.OrdType = ordType
        self.Symbol = symbol
        self.Maturity = datetime.strptime(maturity, '%Y%m').strftime('%b-%y').upper()
        self.Name = name
        self.MarketGroup = group
        self.RiskFactor = risk
        self.MaxPosition = maxPos
        self.Epic = ''
        self.Ccy = ''
        self.FillTime = None
        self.FillPrice = None
        self.FillSize = None
        self.Status = OrderStatus.Pending
        self.BrokerReferenceId = ''


class Money(object):
    def __init__(self, amount, ccy):
        self.Ccy = ccy
        self.Amount = amount


class StoreManager(object):
    def __init__(self, logger, loop=None):
        self.__timeout = 10
        self.__logger = logger
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    def UpdateStatus(self, order):
        update = 'UpdateStatus: '
        try:
            if order.Status == OrderStatus.Filled:
                trade = {
                  "FillTime": order.FillTime,
                  "Side": order.Side,
                  "FilledSize": decimal.Decimal(str(order.FillSize)),
                  "Price": decimal.Decimal(str(order.FillPrice)),
                  "Broker": {"Name": "IG", "RefType": "dealId", "Ref": order.BrokerReferenceId}
                }
            if order.Status == OrderStatus.Failed:
                trade = {}

            response = self.__Orders.update_item(
                Key={
                    'OrderId': order.OrderId,
                    'TransactionTime': order.TransactionTime,
                },
                UpdateExpression="set #s = :s, Trade = :t",
                ConditionExpression="#s = :p",
                ExpressionAttributeNames={
                    '#s': 'Status'
                },
                ExpressionAttributeValues={
                    ':s': order.Status,
                    ':t': trade,
                    ':p': 'PENDING'
                },
                ReturnValues="UPDATED_NEW")
            update += '%s' % response['Attributes']

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            update += e.response['Error']['Message']
        except Exception as e:
            self.__logger.error(e)
            update += e
        else:
            update += ". UpdateItem succeeded."
            self.__logger.info(response)

        self.__logger.info('Update: %s', update)
        return update

    @Connection.ioreliable
    async def GetSecurities(self, securities):
        try:
            self.__logger.info('Calling securities query ...')
            pairs = list(map(lambda x: Key('Symbol').eq(x[0]) & Key('Broker').eq(x[1]), securities))
            keyCondition = reduce(lambda x, y: x | y, pairs) if len(pairs) > 1 else pairs[0]

            with async_timeout.timeout(self.__timeout):
                response = await self.__loop.run_in_executor(None,
                                                             functools.partial(self.__Securities.scan,
                                                                               FilterExpression=keyCondition))
                return response['Items']

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.__logger.error(e)
            return None

    async def __aenter__(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.__Securities = db.Table('Securities')
        self.__Orders = db.Table('Orders')
        self.__logger.info('StoreManager created')
        return self

    async def __aexit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')


class IGClient:
    """IG client."""

    def __init__(self, params, logger, loop=None):
        self.__timeout = 10
        self.__logger = logger
        self.__id = params.Identifier
        self.__password = params.Password
        self.__url = params.Url
        self.__key = params.Key
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

    @Connection.ioreliable
    async def CreatePosition(self, order):
        try:
            url = '%s/%s' % (self.__url, 'positions/otc')
            with async_timeout.timeout(self.__timeout):
                request = {
                    "currencyCode": order.Ccy,
                    "direction": order.Side,
                    "epic": order.Epic,
                    "expiry": order.Maturity,
                    "forceOpen": False,
                    "guaranteedStop": False,
                    "level": None,
                    "limitDistance": None,
                    "limitLevel": None,
                    "orderType": order.OrdType,
                    "quoteId": None,
                    "size": order.Size,
                    "stopDistance": None,
                    "stopLevel": None,
                    "timeInForce": "FILL_OR_KILL",
                    "trailingStop": None,
                    "trailingStopIncrement": None,
                }
                self.__logger.info('Calling CreatePosition ...')
                tokens = copy.deepcopy(self.__tokens)
                tokens['Version'] = "2"
                response = await self.__connection.post(url=url, headers=tokens, json=request)
                self.__logger.info('CreatePosition Response Code: {}'.format(response.status))
                payload = await response.json()
                return payload
        except Exception as e:
            self.__logger.error('CreatePosition: %s, %s' % (self.__url, e))
            return None

    @Connection.ioreliable
    async def GetPositions(self):
        try:
            url = '%s/positions' % self.__url
            with async_timeout.timeout(self.__timeout):
                self.__logger.info('Calling GetPositions ...')
                tokens = copy.deepcopy(self.__tokens)
                tokens['Version'] = "2"
                response = await self.__connection.get(url=url, headers=tokens)
                self.__logger.info('GetPositions Response Code: {}'.format(response.status))
                payload = await response.json()
                return payload
        except Exception as e:
            self.__logger.error('GetPositions: %s, %s' % (self.__url, e))
            return None

    @Connection.ioreliable
    async def GetPosition(self, dealId):
        try:
            url = '%s/positions/%s' % (self.__url, dealId)
            with async_timeout.timeout(self.__timeout):
                self.__logger.info('Calling GetPosition ...')
                response = await self.__connection.get(url=url, headers=self.__tokens)
                self.__logger.info('GetPosition Response Code: {}'.format(response.status))
                payload = await response.json()
                return payload
        except Exception as e:
            self.__logger.error('GetPosition: %s, %s' % (self.__url, e))
            return None

    @Connection.ioreliable
    async def SearchMarkets(self, term):
        try:
            url = '%s/markets?searchTerm=%s' % (self.__url, term)
            with async_timeout.timeout(self.__timeout):
                self.__logger.info('Calling SearchMarkets ...')
                response = await self.__connection.get(url=url, headers=self.__tokens)
                self.__logger.info('SearchMarkets Response Code: {}'.format(response.status))
                payload = await response.json()
                return payload
        except Exception as e:
            self.__logger.error('SearchMarkets: %s, %s' % (self.__url, e))
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
    def __init__(self, params, logger, loop=None):
        self.Timeout = 10
        self.__logger = logger
        self.__params = params
        self.__store = None
        self.Balance = None
        self.__client = None
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    async def __aenter__(self):
        self.__store = StoreManager(self.__logger, self.__loop)
        await self.__store.__aenter__()
        self.__client = IGClient(self.__params, self.__logger, self.__loop)
        self.__connection = await self.__client.__aenter__()
        auth = await self.__connection.Login()
        self.Balance = Money(auth['accountInfo']['available'], auth['currencyIsoCode'])
        self.__logger.info('{}'.format(auth))
        self.__logger.info('Scheduler created')
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.__connection.Logout()
        await self.__client.__aexit__(*args, **kwargs)
        await self.__store.__aexit__(*args, **kwargs)
        self.__logger.info('Scheduler destroyed')

    async def ValidateOrders(self, orders):
        keys = [(x['Symbol']['S'], x['Broker']['S']) for x in orders]
        securities = await self.__store.GetSecurities(keys)
        self.__logger.info('Securities %s' % securities)

        found = [(x['Symbol'], x['Description']['Name'], x['Description']['MarketGroup'],
                  x['Risk']['RiskFactor'], x['Risk']['MaxPosition']) for x in securities
                 if x['TradingEnabled'] is True and x['Broker'] == 'IG']

        pending = [(x['OrderId']['S'], x['TransactionTime']['S'], x['Symbol']['S'], x['Order']['M']['Side']['S'],
                    x['Order']['M']['Size']['N'], x['Order']['M']['OrdType']['S'], x['Maturity']['S'])
                   for x in orders if x['Broker']['S'] == 'IG']

        valid = [Order(p[0], p[1], p[2], p[3], p[4], p[5], p[6], f[1], f[2], f[3], f[4])
                 for f in found for p in pending if f[0] == p[2]]

        invalid = [key for key in keys if key not in map(lambda y: (y[0], 'IG'), found)]
        return valid, invalid

    @Connection.reliable
    def GetCurrentPosition(self, order, trades):

        if trades is None or 'positions' not in trades or len(trades['positions']) == 0:
            self.__logger.info('OrderId: %s. No positions have been found' % order.OrderId)
            return 0

        found = [p['position'] for p in trades['positions']
                if p['market']['expiry'] == order.Maturity
                 and p['market']['instrumentName'] == order.Name and p['market']['instrumentType'] == order.MarketGroup]

        if len(found) == 0:
            self.__logger.info('OrderId: %s. No open positions have been found' % order.OrderId)
            return 0

        long = reduce(lambda x, y: x + y,
                      map(lambda x: x['size'], filter(lambda x: x['direction'] == 'BUY', found)), 0)
        short = reduce(lambda x, y: x + y,
                       map(lambda x: x['size'], filter(lambda x: x['direction'] == 'SELL', found)), 0)

        return long - short

    def BalanceCheck(self, order, trades):
        try:
            position = self.GetCurrentPosition(order, trades)

            self.__logger.info('OrderId {}, symbol {}, riskFactor {}, risk{}, maxPosition {}, size {}, currentOpnPos {}'
                    .format(order.OrderId, order.Symbol, order.RiskFactor, order.Size/self.Balance.Amount,
                            order.MaxPosition, order.Size, position))
            if order.Size/self.Balance.Amount > order.RiskFactor:
                return order, False
            if order.Size > order.MaxPosition:
                return order, False

            if order.Side == Side.Buy and order.MaxPosition < float(position) + order.Size:
                return order, False
            if order.Side == Side.Sell and order.MaxPosition < abs(float(position) - order.Size):
                return order, False
            return order, True
        except Exception as e:
            self.__logger.error('BalanceCheck Error: %s' % e)
            return order, False

    def SendEmail(self, text):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'IG EXECUTOR RESULTS'
        msg['From'] = self.__params.EAddress
        msg['To'] = self.__params.EAddress
        mime_text = MIMEText(text, 'html')
        msg.attach(mime_text)

        server = smtplib.SMTP(self.__params.ESmtp, 587, timeout=10)
        # server.set_debuglevel(10)
        server.starttls()
        server.ehlo()
        server.login(self.__params.EUser, self.__params.EPassword)
        server.sendmail(self.__params.EAddress,  self.__params.EAddress, msg.as_string())
        res = server.quit()
        self.__logger.info(res)

    async def GetPositions(self):
        positions = await self.__client.GetPositions()
        self.__logger.info('GetPositions: %s' % positions)
        return positions

    async def SendOrder(self, order):
        try:
            lookup = await self.__client.SearchMarkets(order.Symbol)
            contract = [o for o in lookup['markets']
                     if o['instrumentName'] == order.Name and o['instrumentType'] == order.MarketGroup
                     and o['expiry'] == order.Maturity]
            self.__logger.info('OrderId: %s. Search for %s, %s returned %s'
                               % (order.OrderId, order.Symbol, order.Maturity, contract))

            if len(contract) == 1 and 'epic' in contract[0] and 'expiry' in contract[0]:
                order.Epic = contract[0]['epic']
                order.Ccy = self.Balance.Ccy
                deal = await self.__client.CreatePosition(order)
                self.__logger.info('OrderId: %s. CreatePosition: %s' % (order.OrderId, deal))
                result = 'Sent %s %s to IG. Received: %s. ' % (order.Symbol, order.Maturity, deal)
                if 'errorCode' in deal:
                    return order.OrderId, result

                positions = await self.__client.GetPositions()
                self.__logger.info('GetPositions: %s' % positions)
                fill = [p['position'] for p in positions['positions']
                       if p['position']['dealReference'] == deal['dealReference']]
                if len(fill) == 1:
                    order.FillTime = fill[0]['createdDateUTC']
                    order.FillPrice = fill[0]['level']
                    order.FillSize = fill[0]['size']
                    order.Status = OrderStatus.Filled
                    order.BrokerReferenceId = fill[0]['dealId']
                    update = self.__store.UpdateStatus(order)
                    result += update
                else:
                    order.Status = OrderStatus.Failed
                    update = self.__store.UpdateStatus(order)
                    result += 'Order Failed. %s' % update
            else:
                result = 'Contract for %s %s could not be found' % (order.Symbol, order.Maturity)
            return order.OrderId, result

        except Exception as e:
            self.__logger.error('SendOrder Error: %s' % e)
            return order.OrderId, 'There was critical exception processing Order: %s' % order.OrderId


async def main(loop, logger, event):
    try:
        params = IGParams()
        params.Url = os.environ['IG_URL']
        params.Key = os.environ['X_IG_API_KEY']
        params.Identifier = os.environ['IDENTIFIER']
        params.Password = os.environ['PASSWORD']
        params.EAddress = os.environ['EMAIL_ADDRESS']
        params.EUser = os.environ['EMAIL_USER']
        params.EPassword = os.environ['EMAIL_PASSWORD']
        params.ESmtp = os.environ['EMAIL_SMTP']

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

        async with Scheduler(params, logger, loop) as scheduler:

            valid, invalid = await scheduler.ValidateOrders(orders)
            if len(valid) == 0:
                scheduler.SendEmail('No Valid Security Definition has been found.')
                return
            logger.info('all validated orders %s' % [o.OrderId for o in valid])

            trades = await scheduler.GetPositions()

            passRisk = [order for order in valid if scheduler.BalanceCheck(order, trades)[1]]
            failedRisk = [order for order in valid if order not in passRisk]
            if len(passRisk) == 0:
                scheduler.SendEmail('No Security has been accepted by Risk Manager.')
                return
            logger.info('all passRisk orders %s' % [o.OrderId for o in passRisk])

            futures = [scheduler.SendOrder(o) for o in passRisk]
            done, _ = await asyncio.wait(futures, timeout=scheduler.Timeout)

            results = []
            for fut in done:
                name, payload = fut.result()
                results.append((name, payload))

            text = '<br>Orders where definition has not been found, not enabled for trading or not IG order %s\n' \
                   % invalid
            text += '<br>Orders where MaxPosition or RiskFactor in Securities table is exceeded %s\n' \
                    % [o.OrderId for o in failedRisk]
            text += '<br>The results of the trades sent to the IG REST API %s\n' % results
            scheduler.SendEmail(text)

    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)

    if 'IG_URL' not in os.environ or 'X_IG_API_KEY' not in os.environ or 'IDENTIFIER' not in os.environ \
            or 'PASSWORD' not in os.environ or 'EMAIL_ADDRESS' not in os.environ or 'EMAIL_USER' not in os.environ \
            or 'EMAIL_PASSWORD' not in os.environ or 'EMAIL_SMTP' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, event))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    with open("event.json") as json_file:
        test_event = json.load(json_file)
        lambda_handler(test_event, None)
