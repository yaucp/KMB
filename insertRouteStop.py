# coding=utf-8
import sys
import urllib.request as urllib2
import xml.etree.ElementTree
import csv
from io import StringIO
import arcpy
import logging
import json, requests


def insertRouteStop():
    withError = False
    stopData = {}
    RSdata = {}
    newData = []

    # Fetech data by URL and insert them
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
            RSdata[(routeStop["route"], routeStop["bound"], routeStop["service_type"], routeStop["seq"],
                    routeStop["stop"])] = row
            newData.append((routeStop["route"], routeStop["bound"], routeStop["service_type"], routeStop["seq"],
                            routeStop["stop"]))

        with arcpy.da.UpdateCursor("GDB/KMB.gdb/RouteStop", (
                "route", "bound", "service_type", "seq", "stop", "name_en", "name_tc", "name_sc",
                "SHAPE@XY")) as uCursor:
            for row in uCursor:
                idx = (row[0], row[1], row[2], str(row[3]), row[4])
                if idx not in newData:
                    uCursor.deleteRow()
                # Update old dataset
                elif idx in RSdata:
                    uCursor.updateRow(RSdata[idx])
                    del RSdata[idx]
        if RSdata:
            with arcpy.da.InsertCursor("GDB/KMB.gdb/RouteStop", (
                    "route", "bound", "service_type", "seq", "stop", "name_en", "name_tc", "name_sc",
                    "SHAPE@XY")) as iCursor:
                for data_row in RSdata.values():
                    iCursor.insertRow(data_row)



    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Insert/update RouteStop data failure')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    insertRouteStop()
