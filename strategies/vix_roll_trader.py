import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json
from utils import Connection, DecimalEncoder
from contracts import SecurityDefinition, Futures
import datetime
import decimal
from dateutil.relativedelta import relativedelta
from functools import reduce
import uuid
import time
import os


class Side:
    Buy = 'BUY'
    Sell = 'SELL'


class Quote(object):
    def __init__(self, symbol):
        self.Symbol = symbol
        self.Date = None
        self.Close = 0.0


class VixTrader(object):
    def __init__(self, logger, today):
        self.secDef = SecurityDefinition()
        self.Logger = logger
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.__isStopAttached = 'STOP_DISTANCE' in os.environ
        self.__stop = 0 if not self.__isStopAttached else int(os.environ['STOP_DISTANCE'])

        self.__isTest = False if os.environ['BACK_TEST'] == 'False' else True
        self.__QuotesEod = db.Table(os.environ['QUOTES_TABLE'])
        self.__Securities = db.Table(os.environ['SECURITIES_TABLE'])
        self.__Orders = db.Table(os.environ['ORDERS_TABLE'])
        s3 = boto3.resource('s3')
        debug = os.environ["DEBUG_FOLDER"]
        self.__debug = s3.Bucket(debug)
        self.Today = today

        self.__FrontFuture = Quote(self.secDef.get_front_month_future('VX', today.date()))
        self.__OpenPosition = 0
        self.__MaxRoll = 0.10
        self.__StdSize = int(os.environ['STD_SIZE'])
        self.__VIX = Quote('VIX')

    def S3Debug(self, line):
        file = os.environ['ROLL_FILE']
        self.__debug.download_file(file, '/tmp/%s' % file)

        check = open('/tmp/%s' % file, 'r')
        lines = check.readlines()
        check.close()
        if line in lines:
            return False

        f = open('/tmp/%s' % file, 'a')
        f.write(line)
        f.close()
        self.__debug.upload_file('/tmp/%s' % file, file)
        return True

    def BothQuotesArrived(self):
        today = self.Today.strftime('%Y%m%d')
        vix = self.GetQuotes(self.__VIX.Symbol, today)
        if len(vix) > 0:
            self.__VIX.Close = vix[0]['Details']['Close']
            self.__VIX.Date = vix[0]['Date']
            self.Logger.info('VIX quote for EOD %s has arrived' % today)
        future = self.GetQuotes(self.__FrontFuture.Symbol, today)
        if len(future) > 0:
            self.__FrontFuture.Close = future[0]['Details']['Close']
            self.__FrontFuture.Date = future[0]['Date']
            self.Logger.info('%s quote for EOD %s has arrived' % (self.__FrontFuture.Symbol, today))
        return len(vix) and len(future)

    def GetCurrentPosition(self, date):
        trades = filter(lambda x: x['Status'] == 'FILLED' or x['Status'] == 'PART_FILLED',
                        self.GetOrders('VX', 'IG'))

        expiry = self.secDef.get_next_expiry_date(symbol=Futures.VX, today=date)
        nextMonth = list(map(lambda x: x['Trade'],
                             filter(lambda x: x['Maturity'] == expiry.strftime('%Y%m'), trades)))

        if len(nextMonth) == 0:
            self.Logger.info('No open positions have been found')
            return 0

        long = reduce(lambda x, y: x + y,
                      map(lambda x: x['FilledSize'], filter(lambda x: x['Side'] == 'BUY', nextMonth)), 0)
        short = reduce(lambda x, y: x + y,
                       map(lambda x: x['FilledSize'], filter(lambda x: x['Side'] == 'SELL', nextMonth)), 0)

        return long - short

    def IsExceeded(self, side, quantity, position):
        vix = self.GetSecurities()
        if vix is None or len(vix) == 0:
            self.Logger.error('No VX in security definition table')
            return True
        if not vix[0]['TradingEnabled']:
            self.Logger.error('Trading disabled for VX in security definition table')
            return True

        maxPosition = vix[0]['Risk']['MaxPosition']
        self.Logger.info('MaxPosition is %s' % maxPosition)
        if side == Side.Buy and maxPosition < position + quantity:
            return True
        if side == Side.Sell and maxPosition < abs(position - quantity):
            return True

        return False

    def SendOrder(self, symbol, maturity, side, size, reason):
        try:

            if self.__isStopAttached and reason == 'OPEN':
                order = {
                    "Side": side,
                    "Size": decimal.Decimal(str(size)),
                    "OrdType": "MARKET",
                    "StopDistance": decimal.Decimal(str(self.__stop)),
                }
            else:
                order = {
                    "Side": side,
                    "Size": decimal.Decimal(str(size)),
                    "OrdType": "MARKET"
                }

            # assume immediate fill on test
            state = 'FILLED' if self.__isTest else 'PENDING'
            if self.__isTest:
                trade = {
                      "FillTime": str(time.time()),
                      "Side": side,
                      "FilledSize": decimal.Decimal(str(size)),
                      "Price": decimal.Decimal(str(self.__FrontFuture.Close))
                    }
            else:
                trade = {}

            strategy = {
                "Name": "VIX ROLL",
                "Reason": reason
            }

            response = self.__Orders.update_item(
                Key={
                    'OrderId': str(uuid.uuid4().hex),
                    'TransactionTime': str(time.time()),
                },
                UpdateExpression="set #st = :st, #s = :s, #m = :m, #p = :p, #b = :b, #o = :o, #t = :t, #str = :str",
                ExpressionAttributeNames={
                    '#st': 'Status',
                    '#s': 'Symbol',
                    '#m': 'Maturity',
                    '#p': 'ProductType',
                    '#b': 'Broker',
                    '#o': 'Order',
                    '#t': 'Trade',
                    '#str': 'Strategy'
                },
                ExpressionAttributeValues={
                    ':st': state,
                    ':s': symbol,
                    ':m': maturity,
                    ':p': 'SPREAD',
                    ':b': 'IG',
                    ':o': order,
                    ':t': trade,
                    ':str': strategy
                },
                ReturnValues="UPDATED_NEW")

        except ClientError as e:
            self.Logger.error(e.response['Error']['Message'])
        except Exception as e:
            self.Logger.error(e)
        else:
            self.Logger.info('Order Created')
            self.Logger.info(json.dumps(response, indent=4, cls=DecimalEncoder))

    def Run(self, symbol):
        self.Logger.info('Run for symbol %s, FrontFuture %s' % (symbol, self.__FrontFuture.Symbol))
        if symbol != self.__VIX.Symbol and symbol != self.__FrontFuture.Symbol:
            self.Logger.warn('Neither spot or Front Future')
            return

        date = self.Today.date()

        if not self.BothQuotesArrived():
            self.Logger.warn('Need both spot and future to run the strategy')
            return

        expiry = self.secDef.get_next_expiry_date(Futures.VX, date)
        days_left = (expiry - date).days
        if days_left <= 0:
            self.Logger.warn('Expiry in the past. Expiry: %s. Today: %s' % (expiry, date))
            return

        roll = (self.__FrontFuture.Close - self.__VIX.Close) / days_left
        roll = round(roll, 2)

        if not self.S3Debug('%s,%s,%s,%s,%s,%s\n'
                     % (date.strftime('%Y%m%d'), self.__FrontFuture.Symbol, self.__FrontFuture.Close,
                        self.__VIX.Close, days_left, roll)):
            self.Logger.info('Already ran for %s' % symbol)
            return

        self.Logger.info('The %s roll on %s with %s days left' % (roll, self.__FrontFuture.Symbol, days_left))

        self.__OpenPosition = self.GetCurrentPosition(date)
        self.Logger.info('Found VX open position. Maturity %s. Size %s'
                         % (expiry.strftime('%Y%m'), self.__OpenPosition))
        if self.__OpenPosition != 0 and date == expiry - relativedelta(days=+1):
            self.Logger.warn('Close any open %s trades one day before the expiry on %s' %
                             (self.__FrontFuture.Symbol, expiry))
            side = Side.Sell if self.__OpenPosition > 0 else Side.Buy
            size = abs(self.__OpenPosition)
            self.SendOrder(symbol='VX', side=side, size=size,
                           maturity=expiry.strftime('%Y%m'), reason='CLOSE')
            return

        if days_left <= 1:
            self.Logger.warn('Only reduce positions in the future so close to expiry: %s %s' % (expiry, date))
            return

        abs_roll = abs(roll)
        self.Logger.info('Checking: %s >= %s' % (abs_roll, self.__MaxRoll))
        self.Logger.info('Checking types: %s, %s' % (type(abs_roll), type(self.__MaxRoll)))
        self.Logger.info('Checking result: ' % abs_roll >= self.__MaxRoll)

        if abs_roll >= self.__MaxRoll:
            self.Logger.info('Conditions have been met. Will create an order')
            side = Side.Sell if (self.__FrontFuture.Close - self.__VIX.Close) >= 0 else Side.Buy
            if self.IsExceeded(side=side, quantity=self.__StdSize, position=self.__OpenPosition):
                self.Logger.warn('Exceeded MaxPosition size: %s, pos: %s' % (self.__StdSize, self.__OpenPosition))
                return

            self.SendOrder(symbol='VX', side=side, size=self.__StdSize,
                           maturity=expiry.strftime('%Y%m'), reason='OPEN')

    @Connection.reliable
    def GetSecurities(self):
        try:
            self.Logger.info('Calling securities query ...')
            response = self.__Securities.query(
                KeyConditionExpression=Key('Symbol').eq('VX') & Key('Broker').eq('IG'))
        except ClientError as e:
            self.Logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.Logger.error(e)
            return None
        else:
            if 'Items' in response:
                return response['Items']

    @Connection.reliable
    def GetOrders(self, symbol, broker):
        try:
            self.Logger.info('Calling orders scan attr: %s %s' % (symbol, broker))
            response = self.__Orders.scan(FilterExpression=Attr('Symbol').eq(symbol) & Attr('Broker').eq(broker))

        except ClientError as e:
            self.Logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.Logger.error(e)
            return None
        else:
            if 'Items' in response:
                return response['Items']

    @Connection.reliable
    def GetQuotes(self, symbol, date):
        try:
            self.Logger.info('Calling quotes query Date key: %s' % date)
            response = self.__QuotesEod.query(
                KeyConditionExpression=Key('Symbol').eq(symbol) & Key('Date').eq(date)
            )
        except ClientError as e:
            self.Logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.Logger.error(e)
            return None
        else:
            if 'Items' in response:
                return response['Items']


def main(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)

    if 'SECURITIES_TABLE' not in os.environ or 'ORDERS_TABLE' not in os.environ or 'ROLL_FILE' not in os.environ \
            or 'QUOTES_TABLE' not in os.environ or 'DEBUG_FOLDER' not in os.environ or 'BACK_TEST' not in os.environ \
            or 'STD_SIZE' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    response = {'State': 'OK'}
    try:
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                t = record['dynamodb']['Keys']['Date']['S']
                today = datetime.datetime.strptime(t, '%Y%m%d')
                symbol = record['dynamodb']['Keys']['Symbol']['S']
                logger.info('New Quote received Symbol: %s', symbol)
                vix = VixTrader(logger, today)
                vix.Run(symbol)
            else:
                logger.info('Not INSERT event is ignored')

        logger.info('Stop VIX trader')

    except Exception as e:
        logger.error(e)
        response['State'] = 'ERROR'

    return response


def lambda_handler(event, context):
    res = main(event, context)
    return json.dumps(res)


if __name__ == '__main__':
    with open("event.json") as json_file:
        test_event = json.load(json_file, parse_float=DecimalEncoder)
    re = main(test_event, None)
    print(json.dumps(re))
