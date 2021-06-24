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

    try:

        # online url link for first extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        # Used as current indicator and stored route NO. and service type
        # initial as None
        currQ = None
        # indicating the position of list from online url source
        idx = 0
        # length of list from online url source
        len_of_data = 0

        with arcpy.da.UpdateCursor("GDB/KMB.gdb/ETA",
                                   ('route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
                                    'name_en', 'name_tc', 'name_sc',
                                    'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
                                    'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')) as sCursor:
            for row in sCursor:
                # Check whether route NO. and service type is the same as currQ
                if (row[0], row[2]) != currQ:
                    # Change currQ to match with RS and load new data
                    currQ = (row[0], row[2])
                    query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                    etaResp = requests.get(url=query_url)
                    resp_data = json.loads(etaResp.text)['data']
                    routeETA_data = parseRouteETA(resp_data)
                    # reset index as 0
                    idx = 0
                    # new length 
                    len_of_data = len(resp_data)

                # update when idx < length of list from online url source and seq of row is equal to that of data indicated by idx
                if (idx < len_of_data ) and row[3] == list(resp_data[idx].values())[4]:
                    row[9:] = list(resp_data[idx].values())[5:]
                    sCursor.updateRow(row)
                    idx += 1

    # print exception
    except Exception as inst:
        print(inst)
    # message foe finished running
    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    updateETA()
