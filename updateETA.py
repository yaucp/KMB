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
        currQ = None
        idx = 0
        len_of_data = 0

        logging.info('Start Data Update...')
        with arcpy.da.UpdateCursor("GDB/KMB.gdb/ETA",
                                   ('route', 'bound', 'seq', 'eta_seq',
                                    'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')) as sCursor:
            for row in sCursor:
                if (row[0], row[1]) != currQ:
                    # Change currQ to match with RS and load new data
                    currQ = (row[0], row[1])
                    query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                    etaResp = requests.get(url=query_url)
                    resp_data = json.loads(etaResp.text)['data']
                    routeETA_data = parseRouteETA(resp_data)
                    idx = 0
                    len_of_data = len(resp_data)
                # updating data
                if ((idx < len_of_data ) and (row[2] == list(resp_data[idx].values())[4])):
                    row[3:] = list(resp_data[idx].values())[8:]

                    sCursor.updateRow(row)
                    idx += 1
                else:
                    row[3:] = [None] * 6
                    sCursor.updateRow(row)
        del sCursor


    except Exception as inst:
        print(inst)

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    updateETA()
