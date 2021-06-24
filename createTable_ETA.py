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
    # Initiation
    RSdata = []
    
    # Create Arsgis Pro table with name "ETA" inside database "GDB/KMB.gdb/"
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

    # Extract KMB data from online and write into Arcgis Pro table "ETA" according to "GDB/KMB.gdb/RouteStop"
    try:
        # Extract data from table "GDB/KMB.gdb/RouteStop" as search cursor type
        # with specific field and append to list named RSdata
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # Base url link for first extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        # Used as current indicator and stored route NO. and service type
        currQ = (RSdata[0][0], RSdata[0][2])
        query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])

        # Get data from online url link
        etaResp = requests.get(url=query_url)
        # Decode JSON data
        resp_data = json.loads(etaResp.text)['data']
        # Change to dictionary type with the tuple of direction
        # and stop sequence number of a bus route as key
        routeETA_data = parseRouteETA(resp_data)

        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY', 'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        iCursor = arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field)

        # Start extraction and importing data according to each row of RSdata
        for RS in RSdata:
            # Check whether route NO. and service type is the same as currQ
            if (RS[0], RS[2]) != currQ:
                # Change currQ to match with RS and load new data
                currQ = (RS[0], RS[2])
                query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                etaResp = requests.get(url=query_url)

                resp_data = json.loads(etaResp.text)['data']
                routeETA_data = parseRouteETA(resp_data)

            # check whether the current bus route and service_type have ETA data
            if (RS[1], RS[3]) not in routeETA_data:
                # Write None data with current row of RSdata appended
                row = list(RS) + [None] * 9
                iCursor.insertRow(row)
            else:
                # Insert row with ETA data
                for data in routeETA_data[(RS[1],RS[3])]:
                    row = list(RS) + list(data.values())[5:]
                    iCursor.insertRow(row)
        del iCursor

    except Exception as inst:
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Create ETA feature class failure')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")

# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result

if __name__ == '__main__':
    main()
