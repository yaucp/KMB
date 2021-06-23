# coding=utf-8
# import json library
import json
# import sys library
import sys
# import logging library
import logging
import time
import xml

# import arcpy library for database import
import arcpy

from datetime import datetime

# import requests for online kmb data extraction
import requests

# main program
def main():
    # data = []
    # initiation
    RSdata = []
    # etaQuery = []
    # etaData = {}
    
    # create Arsgis Pro table with name "ETA" inside database "GDB/KMB.gdb/"
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
    # print error message
    except Exception as inst:
        print(inst)

    # extract KMB data from online and write into Arcgis Pro table "ETA" according to "GDB/KMB.gdb/RouteStop"
    try:
        # extract data from table "GDB/KMB.gdb/RouteStop" as sCursor type
        # with pattern : 'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY'
        # and append to list name RSdata
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # initiation
        # url link for first extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        # used as current indicator and stored route NO. and service type
        currQ = (RSdata[0][0], RSdata[0][2])
        # data source as url link
        query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])

        # get data from online url link
        etaResp = requests.get(url=query_url)
        # change to list type
        resp_data = json.loads(etaResp.text)['data']
        # change to dictionary type with the turple of direction
        # and stop sequence number of a bus route as key
        routeETA_data = parseRouteETA(resp_data)

        # for row in RSdata:
        #     iCursor.insertRow(row)
        # pattern of insertion
        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY', 'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        # Cursor type used for importing data
        iCursor = arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field)

        # start extraction and importing data according to each row of RSdata
        for RS in RSdata:
            # check whether route NO. and service type is the same as currQ
            if (RS[0], RS[2]) != currQ:
                # change currQ to match with RS
                currQ = (RS[0], RS[2])
                # new url for online source
                query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                # get data
                etaResp = requests.get(url=query_url)

                # change to list type
                resp_data = json.loads(etaResp.text)['data']
                # change to dictionary type with the turple of direction
                # and stop sequence number of a bus route as key
                routeETA_data = parseRouteETA(resp_data)

            # check whether data of direction
            # and stop sequence number of a bus route is inside routeETA_data
            if (RS[1], RS[3]) not in routeETA_data:
                # write None data with current row of RSdata appended
                row = list(RS) + [None] * 9
                iCursor.insertRow(row)
            else:
                # write gotten data with current row of RSdata appended
                for data in routeETA_data[(RS[1],RS[3])]:
                    row = list(RS) + list(data.values())[5:]
                    iCursor.insertRow(row)
        # delete cursor type variable
        del iCursor

    # action for encountering error
    except Exception as inst:
        # print out error message
        print("Error encounted.")
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
        logging.info('Smart Car Parking Data URL insert ERROR')
        logging.info(inst)
        logging.info('Continue to the next dataset module.')
        return False
    # message for successful run
    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")

# function for changing list resp into dictionary type result
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result

# name protection for program being import as library
if __name__ == '__main__':
    main()
