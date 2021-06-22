# coding=utf-8
import sys
import urllib.request as urllib2
import xml.etree.ElementTree
import csv
from io import StringIO
import arcpy
import logging
import json, requests

def insertCarParkData():
    withError = False
    data = []

    # Fetech data by URL and insert them
    try:
        logging.info('Start Data preparation...')
        kmbRoute_url = "https://data.etabus.gov.hk/v1/transport/kmb/route/"
        kmbRouteStop_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-stop"

        resp = requests.get(url=kmbRoute_url)
        kmbRoute_data = json.loads(resp.text)['data']

        RSresp = requests.get(url=kmbRouteStop_url)
        kmbRouteStop_data = json.loads(RSresp.text)['data']

        for route in kmbRoute_data:
            row = []
            row.append(route['route'])
            row.append(route['bound'])
            row.append(route['service_type'])
            row.append(route['orig_en'])
            row.append(route['orig_tc'])
            row.append(route['orig_sc'])
            row.append(route['dest_en'])
            row.append(route['dest_tc'])
            row.append(route['dest_sc'])

            data.append(row)


    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Smart Car Parking Data URL insert ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

if __name__ == '__main__':
    insertCarParkData()