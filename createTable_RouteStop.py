# coding=utf-8
import json
import sys
import logging
import xml

import arcpy

from datetime import datetime

import requests


def main():
    stopData = {}
    RSdata = []
    try:
        logging.info('Start Data preparation...')
        kmbStop_url = "https://data.etabus.gov.hk/v1/transport/kmb/stop/"
        kmbRouteStop_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-stop"

        routeStopResp = requests.get(url=kmbRouteStop_url)
        kmbRouteStop_data = json.loads(routeStopResp.text)['data']

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

            stopData[stop['stop']] = row

        for routeStop in kmbRouteStop_data:
            row = []
            row.append(routeStop['route'])
            row.append(routeStop['bound'])
            row.append(routeStop['service_type'])
            row.append(routeStop['seq'])
            row.append(routeStop['stop'])

            if routeStop['stop'] in stopData:
                row += stopData[routeStop['stop']][1:]
            else:
                logging.info('Chinese Record does not exist for ' + routeStop['stop'])
                logging.info('Continue to process next record.')
                withError = True
                continue
            RSdata.append(row)

    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Smart Car Parking Data URL insert ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    try:
        arcpy.management.CreateFeatureclass("GDB/KMB.gdb/", "RouteStop","POINT")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "route", "TEXT", field_length=10, field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "bound", "TEXT", field_length=1,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "service_type", "TEXT", field_length=10,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "seq", "SHORT", field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "stop", "TEXT", field_length=25, field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_sc", "TEXT", field_length=25)

    except Exception as inst:
        print(inst)

    try:
        with arcpy.da.InsertCursor("GDB/KMB.gdb/RouteStop", (
                "route", "bound", "service_type", "seq", "stop", "name_en", "name_tc", "name_sc",
                "SHAPE@XY")) as iCursor:
            for row in RSdata:
                iCursor.insertRow(row)

    except Exception as inst:
        logging.info('Insert EN 24pc records failure')
        logging.info(inst)
        print(inst)
        logging.info('Continue to the next dataset module.')
        withError = True

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    main()