import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'python-intercom'])
import pandas as pd
import numpy as np
import time
from intercom.client import Client
from keboola import docker

""" KEBOOLA PARAMS """
cfg = docker.Config()
parameters = cfg.get_parameters()
PERSONAL_ACCESS_TOKEN = parameters.get('personalAccessToken')
TIMEOUT_BETWEEN_APICALLS = parameters.get('timeoutBetweenAPICalls')
MAX_ITEMS_PER_REQUEST = parameters.get('maxItemsPerRequest')

""" 0: DATA LOAD """
orderBasicTable = pd.read_csv('in/tables/order_last2000.csv')
orderOfferTable = pd.read_csv('in/tables/orderoffer_increment.csv')
rideBasicTable = pd.read_csv('in/tables/ride_last2000.csv')

""" 1: DATE TRANSFORMATIONS """
# Columns to `datetime` format
## RideTable
for dateCol in ['orderedAt','finishedAt','startedAt']:
    rideBasicTable[dateCol] = pd.to_datetime(rideBasicTable[dateCol])
    rideBasicTable[dateCol+'_UNIX'] = rideBasicTable[dateCol].astype(np.int64) // 10**9
## OrderTable
orderBasicTable['createdAt'] = pd.to_datetime(orderBasicTable['createdAt'])
orderBasicTable['createdAt_UNIX'] = orderBasicTable['createdAt'].astype(np.int64) // 10**9
## OrderOfferTable
orderOfferTable['offeredAt'] = pd.to_datetime(orderOfferTable['offeredAt'])
orderOfferTable['offeredAt_UNIX'] = orderOfferTable['offeredAt'].astype(np.int64) // 10**9

""" 2: BASIC JOINS """
# Basic Joins
orderJoinedTable = orderBasicTable.merge(orderOfferTable,how='left',left_on='orderId',right_on='order_last2000_pk')

# No-offer-orders
noOfferOrders = orderJoinedTable.loc[pd.isnull(orderJoinedTable['order_last2000_pk']),['orderId','passengerId','createdAt_UNIX','orderState','region']]
noOfferOrders = noOfferOrders[~pd.isnull(noOfferOrders['passengerId'])]
finishedRides = rideBasicTable.loc[rideBasicTable['rideState']=='FINISHED',['orderId','rideId','passengerId','orderedAt_UNIX','finishedAt_UNIX']]
finishedRides = finishedRides[~pd.isnull(finishedRides['passengerId'])]

""" 3: INTERCOM JSON CREATION """
bulkExportList = []

def IO_noOfferOrder(row):
    bulkExportList.append(
    {
        'event_name': 'no-offer-order',
        'created_at': int(row['createdAt_UNIX']),
        'user_id': str(int(row['passengerId'])),
        'metadata': {
            'ORDER_ID': str(row['orderId']),
            'STATE': row['orderState'],
            'CITY':row['region']
        }
    }) 
    
def IO_noOfferOrderPrague(row):
    bulkExportList.append(
    {
        'event_name': 'no-offer-order-prague',
        'created_at': int(row['createdAt_UNIX']),
        'user_id': str(int(row['passengerId'])),
        'metadata': {
            'ORDER_ID': str(row['orderId']),
            'STATE': row['orderState'],
            'CITY':row['region']
        }
    })  
    
def IO_finishedRide(row):
    bulkExportList.append(
    {
        'event_name': 'finished-ride',
        'created_at': int(row['finishedAt_UNIX']),
        'user_id': str(int(row['passengerId'])),
        'metadata': {
            'ORDEREDAT_DATE': int(row['orderedAt_UNIX']),
            'RIDEID': 'https://www.liftago.com/admin/rides/'+str(int(row['rideId'])),
            'ORDERID': str(int(row['orderId']))        
        }
    })               
            
noOfferOrders.apply(IO_noOfferOrder,axis=1)
noOfferOrders.apply(IO_noOfferOrderPrague,axis=1)
finishedRides.apply(IO_finishedRide,axis=1)    

""" 4: INTERCOM API HIT """
# Connect to INTERCOM
intercom = Client(personal_access_token=PERSONAL_ACCESS_TOKEN)    

# Slice the huge bulk into more eatable chunks 
bulkExportSublists = [bulkExportList[i:i+MAX_ITEMS_PER_REQUEST] for i in range(0, len(bulkExportList), MAX_ITEMS_PER_REQUEST)]

# Hit the INTERCOM API
for i,subBulk in enumerate(bulkExportSublists):
    intercom.events.submit_bulk_job(create_items = subBulk)
    print('SubBulk n.{} uploaded'.format(i))
    time.sleep(TIMEOUT_BETWEEN_APICALLS)
    print('Sleep is over')
