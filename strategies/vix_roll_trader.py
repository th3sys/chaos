import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json
from utils import Connection, DecimalEncoder
import contracts


class VixTrader(object):
    def __init__(self, logger):
        self.secDef = contracts.SecurityDefinition()
        self.Logger = logger
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.__QuotesEod = db.Table('Quotes.EOD')
        self.__Securities = db.Table('Securities')

        self.Logger.info('VixTrader Created')

    def Run(self):
        pass

    @Connection.reliable
    def GetSecurities(self):
        try:
            self.Logger.info('Calling securities scan ...')
            response = self.__Securities.scan(FilterExpression=Attr('SubscriptionEnabled').eq(True))
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
            self.Logger.info(json.dumps(response, indent=4, cls=DecimalEncoder))
            if 'Items' in response:
                return response['Items']


def main(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)

    response = {'State': 'OK'}
    try:
        vix = VixTrader(logger)

        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                logger.info('New Quote received Symbol: %s', record['dynamodb']['Keys']['Symbol'])
                vix.Run()
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
