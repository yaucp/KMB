# coding=utf-8
import json
import sys
import logging
import time
import xml

import arcpy

from datetime import datetime

import grequests

def main():
    # Initiation
    RSdata = []
    etaURL = []
    etaData = {}
    
    # Create Arsgis Pro table with name "ETA" inside database "GDB/KMB.gdb/"
    try:
        arcpy.management.CreateFeatureclass("GDB/KMB.gdb/", "ETA","POINT")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "route", "TEXT", field_length=10,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "bound", "TEXT", field_length=1,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "service_type", "TEXT", field_length=10,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "seq", "SHORT", field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "stop", "TEXT", field_length=25,
                                  field_is_required="REQUIRED")
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "name_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "name_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "name_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "orig_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "orig_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "orig_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta_seq", "SHORT", field_precision=1)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_en", "TEXT", field_length=100)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "timestamp", "TEXT", field_length=25)
        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")
    except Exception as inst:
        print(inst)

    # Extract KMB data from online and write into Arcgis Pro table "ETA" according to "GDB/KMB.gdb/RouteStop"
    try:
        # Get all route-stop data to get info on route, stop for query link
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop',
                                    'SHAPE@XY', 'name_en', 'name_tc', 'name_sc', 'orig_en',
                                    'orig_tc', 'orig_sc')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # Base url link for route eta data extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"

        # Get all responses from all the url of every possible route, service_type
        for RS in RSdata:
            tempURL = kmbETA_url + r"{}/{}".format(RS[0], RS[2])
            if tempURL not in etaURL:
                etaURL.append(tempURL)

        requests = (grequests.get(u) for u in etaURL)
        responses = grequests.map(requests)

        # Loop through responses and json data to make route and service_type as their key in etaData var.
        for r in responses:
            resp_data = json.loads(r.text)['data']
            # If eta data exists, can extract keys from json data
            if resp_data:
                idx = (resp_data[0]['route'], str(resp_data[0]['service_type']))
                etaData[idx] = parseRouteETA(resp_data)

        # Delete all rows in the table to re-insert the data
        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")
        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
            'name_en', 'name_tc', 'name_sc', 'orig_en', 'orig_tc', 'orig_sc',
            'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        with arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field) as iCursor:
            # Start extraction and importing data according to each row of RSdata
            for RS in RSdata:
                # Check whether the Route Stop data have eta data and whether the specific bound and seq have eta
                # data as well
                if (RS[0], RS[2]) not in etaData or (RS[1], RS[3]) not in etaData[(RS[0], RS[2])]:
                    # Write None data with current row of RSdata appended
                    row = RS + [None] * 9
                    iCursor.insertRow(row)
                else:
                    # Insert row with ETA data
                    data = etaData[(RS[0], RS[2])]
                    for eta in data[(RS[1], RS[3])]:
                        row = RS + list(eta.values())[5:]
                        iCursor.insertRow(row)

    except Exception as inst:
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Create ETA feature class failure')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')

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
