# coding=utf-8
import json
import sys
import logging
import xml

import arcpy

from datetime import datetime

import requests


def createTable_Stop():
    data = []

    filename = datetime.now().strftime('log/Create_KMB_Stop_%H_%M_%d_%m_%Y.log')
    logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(message)s')
    try:
        logging.info('Start Data preparation...')
        kmbStop_url = "https://data.etabus.gov.hk/v1/transport/kmb/stop/"
        
        stopResp = requests.get(url=kmbStop_url)
        kmbStop_data = json.loads(stopResp.text)['data']

        for stop in kmbStop_data:
            row = []
            row.append(stop['stop'])
            row.append(stop['name_en'])
            row.append(stop['name_tc'])
            row.append(stop['name_sc'])

            point = []
            point.append(float(stop['long']))
            point.append(float(stop['lat']))

            row.append(point)

            data.append(row)

    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Smart Car Parking Data URL insert ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    try:
        arcpy.management.CreateFeatureclass("GDB/KMB.gdb/", "Stop","POINT")
        arcpy.management.AddField("GDB/KMB.gdb/Stop/", "stop", "TEXT", field_length=25, field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/Stop/", "name_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/Stop/", "name_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/Stop/", "name_sc", "TEXT", field_length=25)
        arcpy.management.TruncateTable("GDB/KMB.gdb/Stop/")
    except Exception as inst:
        print(inst)

    try:
        with arcpy.da.InsertCursor("GDB/KMB.gdb/Stop", ("stop", "name_en", "name_tc", "name_sc","SHAPE@XY")) as iCursor:
            for row in data:
                iCursor.insertRow(row)

    except Exception as inst:
        logging.info('Create stop table records failure')
        logging.info(inst)
        print(inst)
        logging.info('Continue to the next dataset module.')
        withError = True

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    createTable_Stop()
