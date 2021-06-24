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
    data = {}
    newData = []

    # Fetech data by URL and insert them
    try:
        logging.info('Start Data preparation...')
        kmbRoute_url = "https://data.etabus.gov.hk/v1/transport/kmb/route/"

        resp = requests.get(url=kmbRoute_url)
        kmbRoute_data = json.loads(resp.text)['data']

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

            data[(route['route'], route['bound'], route['service_type'])] = row
            newData.append((route['route'], route['bound'], route['service_type']))

        with arcpy.da.UpdateCursor("GDB/KMB.gdb/Route", (
                "route", "bound", "service_type", "orig_en", "orig_tc", "orig_sc", "dest_en", "dest_tc",
                "dest_sc")) as uCursor:
            for row in uCursor:
                idx = (row[0], row[1], row[2])
                if idx not in newData:
                    uCursor.deleteRow()
                # Update old dataset
                elif idx in data:
                    uCursor.updateRow(data[idx])
                    del data[idx]
        if data:
            with arcpy.da.InsertCursor("GDB/KMB.gdb/Route", (
                    "route", "bound", "service_type", "orig_en", "orig_tc", "orig_sc", "dest_en", "dest_tc",
                    "dest_sc")) as iCursor:
                for data_row in data.values():
                    iCursor.insertRow(data_row)


    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Insert/Update Route data ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    insertCarParkData()
