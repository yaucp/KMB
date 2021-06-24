# coding=utf-8
import json
import sys
import logging
import xml

import arcpy

from datetime import datetime

import requests


def main():
    data = []

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
        arcpy.CreateTable_management("GDB/KMB.gdb/", "Route")
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "route", "TEXT", field_length=10, field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "bound", "TEXT", field_length=1,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "service_type", "TEXT", field_length=10,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "orig_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "orig_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "orig_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "dest_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "dest_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/Route/", "dest_sc", "TEXT", field_length=25)

    except Exception as inst:
        print(inst)

    try:
        with arcpy.da.InsertCursor("GDB/KMB.gdb/Route", (
                "route", "bound", "service_type","orig_en", "orig_tc", "orig_sc","dest_en", "dest_tc", "dest_sc")) as iCursor:
            for row in data:
                iCursor.insertRow(row)

    except Exception as inst:
        logging.info('Create Route Table failure')
        logging.info(inst)
        print(inst)
        logging.info('Continue to the next dataset module.')
        withError = True

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    main()
