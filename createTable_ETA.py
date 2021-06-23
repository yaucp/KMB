# coding=utf-8
import json
import sys
import logging
import time
import xml

import arcpy

from datetime import datetime

import requests


def main():
    RSdata = []

    try:
        # arcpy.management.CreateFeatureclass("GDB/KMB.gdb/", "ETA","POINT")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "route", "TEXT", field_length=10,
        #                           field_is_required="REQUIRED")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "bound", "TEXT", field_length=1,
        #                           field_is_required="REQUIRED")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "service_type", "TEXT", field_length=10,
        #                           field_is_required="REQUIRED")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "seq", "SHORT", field_is_required="REQUIRED")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "stop", "TEXT", field_length=25,
        #                           field_is_required="REQUIRED")
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_en", "TEXT", field_length=50)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_tc", "TEXT", field_length=25)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_sc", "TEXT", field_length=25)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta_seq", "SHORT", field_precision=1)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta", "TEXT", field_length=25)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_en", "TEXT", field_length=100)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_tc", "TEXT", field_length=25)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_sc", "TEXT", field_length=25)
        # arcpy.management.AddField("GDB/KMB.gdb/ETA/", "timestamp", "TEXT", field_length=25)
        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")

    except Exception as inst:
        print(inst)

    try:
        # Get RouteStop data in order to know what route exists and fetch ETA data from
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # Get first bus route ETA data
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        currQ = (RSdata[0][0], RSdata[0][2])
        query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])

        etaResp = requests.get(url=query_url)
        resp_data = json.loads(etaResp.text)['data']
        routeETA_data = parseRouteETA(resp_data)

        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY', 'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        iCursor = arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field)
        for RS in RSdata:
            # Check current RouteStop route, service_type match last fetched data.
            # If not, fetch new ETA data and update currQ
            if (RS[0], RS[2]) != currQ:
                currQ = (RS[0], RS[2])
                query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                etaResp = requests.get(url=query_url)

                resp_data = json.loads(etaResp.text)['data']
                routeETA_data = parseRouteETA(resp_data)

            # Check whether the specific stop have ETA data or not
            # If not, make the rest of the column None/null
            if (RS[1], RS[3]) not in routeETA_data:
                row = list(RS) + [None] * 9
                iCursor.insertRow(row)
            else:
                # Add the remaining ETA data to the column
                for data in routeETA_data[(RS[1], RS[3])]:
                    row = list(RS) + list(data.values())[5:]
                    iCursor.insertRow(row)
        del iCursor


    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Smart Car Parking Data URL insert ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


# Make the a tuple consisting of bus route direction (I/O) and the bus stop number sequencce in that route as the key
# to make searching for data much faster and easier to do so.
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result


if __name__ == '__main__':
    main()
