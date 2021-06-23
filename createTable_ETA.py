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
    # data = []
    RSdata = []
    # etaQuery = []
    # etaData = {}
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
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_en", "TEXT", field_length=50)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "dest_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta_seq", "SHORT", field_precision=1)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "eta", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_en", "TEXT", field_length=100)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_tc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "rmk_sc", "TEXT", field_length=25)
        arcpy.management.AddField("GDB/KMB.gdb/ETA/", "timestamp", "TEXT", field_length=25)

    except Exception as inst:
        print(inst)

    try:
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))
        # with arcpy.da.InsertCursor("GDB/KMB.gdb/ETA",('route', 'bound', 'service_type', 'seq', 'stop', 'dest_en', dest_tc', 'dest_sc', 'eta_seq', 'eta', 'rmk_en', 'rmk_tc', 'rmk_sc', 'timestamp','SHAPE@XY')) as iCursor:
        #     for row in RSdata:
        #         iCursor.insertRow(row)
        #
        # logging.info('Start Data preparation...')
        # kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        #

        i = 0
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        currQ = (RSdata[0][0], RSdata[0][2])
        query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])

        etaResp = requests.get(url=query_url)
        resp_data = json.loads(etaResp.text)['data']
        routeETA_data = parseRouteETA(resp_data)
        debug = {}
        debug[0] = routeETA_data

        with arcpy.da.InsertCursor("GDB/KMB.gdb/ETA",('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY', 'dest_tc', 'dest_sc','dest_en', 'eta_seq', 'eta',  'rmk_tc', 'rmk_sc','rmk_en', 'timestamp')) as iCursor:
            # for row in RSdata:
            #     iCursor.insertRow(row)
            i = 1
            for RS in RSdata:
                if (RS[0], RS[2]) != currQ:
                    currQ = (RS[0], RS[2])
                    query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                    try:
                        etaResp = requests.get(url=query_url)
                    except ConnectionError as inst:
                        print(inst)
                        print("Try to sleep for 5s")
                        time.sleep(5)
                        etaResp = requests.get(url=query_url)
                    resp_data = json.loads(etaResp.text)['data']
                    routeETA_data = parseRouteETA(resp_data)
                    debug[i] = routeETA_data
                    i += 1
                if (RS[1],RS[3]) not in routeETA_data:
                    iCursor.insertRow(row)
                else:
                    for data in routeETA_data[(RS[1],RS[3])]:
                        if data['route'] == "3S":
                            print(data)
                        row = list(RS) + list(data.values())[5:]
                        iCursor.insertRow(row)



        # for RS in RSdata:
        #     if (RS[0], RS[2]) != currQ:
        #         currQ = (RS[0], RS[2])
        #         query_url = kmbETA_url + r"{}/{}".format(currQ)
        #
        #         etaResp = requests.get(url=query_url)
        #         eta_data = json.loads(etaResp.text)['data']
        #
        #
        #     check = {}
        #     for eta in eta_data:
        #         check[(eta['route'], eta['dir'],eta['service_type'] , eta['seq'], eta['eta_seq'])] = eta
        #
        #     row = list(RSdata[i])
        #     row.append('KMB')
        #     row += check[(row[1],)]
        # rsList = []
        data = []
        # with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop", ('route', 'service_type')) as sCursor:
        #     for row in sCursor:
        #         if (row[0], row[1]) not in rsList: rsList.append((row[0], row[1]))
        #     logging.info('Start Data preparation...')
        #

        # i = 0
        # for q in rsList:
        #     query_url = kmbETA_url + r"{}/{}".format(q[0], q[1])
        #
        #     etaResp = requests.get(url=query_url)
        #     eta_data = json.loads(etaResp.text)['data']
        #     for etaData in eta_data:
        #         row = list(etaData.values())[:9]
        #         row.append(etaData['eta'].replace('T', ' ')[:-6] if etaData['eta'] else None)
        #         row.append(etaData['rmk_tc'])
        #         row.append(etaData['rmk_sc'])
        #         row.append(etaData['rmk_en'])
        #         row.append(etaData['data_timestamp'].replace('T', ' ')[:-6])
        #         data.append(row)
        #         i += 1
        # rsList = {}
        # with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop", ('route', 'bound', 'service_type', 'seq', 'stop', 'name_en', 'name_tc', 'name_sc', 'SHAPE@XY')) as sCursor:
        #     for row in sCursor:
        #         rsList[(row[0], row[1], row[2], row[3])] = row[4:]

                # idx = (eta['route'], eta['dir'], eta['seq'])
                # etaData.setdefault(idx, [eta]).append(list(eta.values()))



            #
            # for eta in eta_data:
            #     co = eta['co']
            #     eta_seq = eta['eta_seq']
            #     time = eta['eta']
            #     rmk_tc = eta['rmk_tc']
            #     rmk_sc = eta['rmk_sc']
            #     rmk_en = eta['rmk_en']
            #     timestamp = eta['data_timestamp']
            #
            #     row =

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
    print(debug)

def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result

if __name__ == '__main__':
    main()
