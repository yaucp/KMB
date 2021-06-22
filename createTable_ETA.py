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
    RSdata = []
    etaQuery = []
    etaDict = {}
    try:
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop","*") as sCursor:
            for row in sCursor:
                RSdata.append(list(row)[1:])
        print(RSdata[0])

        logging.info('Start Data preparation...')
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"

        for RS in RSdata:
            idx = (RS[1], RS[3])
            if idx not in etaQuery:
                etaQuery.append(idx)

        for query in etaQuery:
            query_url = kmbETA_url + r"{}/{}".format(query[0], query[1])

            etaResp = requests.get(url=query_url)
            eta_data = json.loads(etaResp.text)['data']


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

    # try:
    #     arcpy.management.CreateFeatureclass("GDB/KMB.gdb/", "RouteStop","POINT")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "route", "TEXT", field_length=10, field_is_required="REQUIRED")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "bound", "TEXT", field_length=1,
    #                               field_is_required="REQUIRED")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "service_type", "TEXT", field_length=10,
    #                               field_is_required="REQUIRED")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "seq", "SHORT", field_is_required="REQUIRED")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "stop", "TEXT", field_length=25, field_is_required="REQUIRED")
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_en", "TEXT", field_length=50)
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_tc", "TEXT", field_length=25)
    #     arcpy.management.AddField("GDB/KMB.gdb/RouteStop/", "name_sc", "TEXT", field_length=25)
    #
    # except Exception as inst:
    #     print(inst)


    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    main()
