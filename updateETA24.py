# import library
import json
import arcpy
import requests
import logging


# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result


def updateETA():
    RSdata = {}
    newData = []

    try:
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop',
                                    'SHAPE@XY', 'name_en', 'name_tc', 'name_sc', 'orig_en',
                                    'orig_tc', 'orig_sc')) as sCursor:
            for row in sCursor:
                RSdata[(row[0], row[1], row[2], row[3])] = list(row)

        # online url link for first extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        # Used as current indicator and stored route NO. and service type
        currQ = None
        routeETA_data = {}

        logging.info('Start Data Update...')
        with arcpy.da.UpdateCursor("GDB/KMB.gdb/ETA",
                                   ('route', 'bound', 'service_type', 'seq', 'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
                                    'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')) as uCursor:
            for row in uCursor:
                if (row[0], row[1]) != currQ:
                    newData += list(routeETA_data.values())
                    # Change currQ to match with RS and load new data
                    currQ = (row[0], row[2])
                    query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                    etaResp = requests.get(url=query_url)
                    resp_data = json.loads(etaResp.text)['data']
                    routeETA_data = parseRouteETA(resp_data)

                idx = (row[1], row[3])
                if idx in routeETA_data and row[7] <= len(routeETA_data[idx]):
                    row[4:] = list(routeETA_data[idx])[row[7] - 1][5:]
                    uCursor.updateRow(row)
                    del routeETA_data[idx]
                elif idx not in routeETA_data:
                    uCursor.deleteRow()

# 63604
            newData += routeETA_data.values()

        if newData:
            with arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", (
                    'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
                    'name_en', 'name_tc', 'name_sc', 'orig_en', 'orig_tc', 'orig_sc',
                    'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
                    'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')) as iCursor:
                for data in newData:
                    idx = (data[1],data[2], str(data[3]), data[4])
                    iCursor.insertRow(RSdata[idx] + data[5:])

                # updating data when index is less that total length of current founded online data
                # and the route shop sequence is the same as that of the row pointed by idx
                # if ((idx < len_of_data ) and (row[2] == list(resp_data[idx].values())[4])):
                #     # update current row
                #     row[3:] = list(resp_data[idx].values())[8:]
                #     uCursor.updateRow(row)
                #     # point to next row of online data
                #     idx += 1
                # else:
                #     # reset row to None
                #     row[3:] = [None] * 6
                #     uCursor.updateRow(row)


    except Exception as inst:
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    updateETA()
