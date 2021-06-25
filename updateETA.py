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
        # used as pointer of current founded online data
        idx = 0
        #store the length of current founded online data
        len_of_data = 0

        logging.info('Start Data Update...')
        with arcpy.da.UpdateCursor("GDB/KMB.gdb/ETA",
                                   ('route', 'service_type', 'seq', 'eta_seq',
                                    'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')) as uCursor:
            for row in uCursor:
                if (row[0], row[1]) != currQ:
                    # Change currQ to match with RS and load new data
                    currQ = (row[0], row[1])
                    query_url = kmbETA_url + r"{}/{}".format(currQ[0], currQ[1])
                    etaResp = requests.get(url=query_url)
                    resp_data = json.loads(etaResp.text)['data']
                    routeETA_data = parseRouteETA(resp_data)
                    # reset pointer and length of current founded online data
                    idx = 0
                    len_of_data = len(resp_data)
                # updating data when index is less that total length of current founded online data 
                # and the route shop sequence is the same as that of the row pointed by idx 
                if ((idx < len_of_data ) and (row[2] == list(resp_data[idx].values())[4])):
                    # update current row
                    row[3:] = list(resp_data[idx].values())[8:]
                    uCursor.updateRow(row)
                    # point to next row of online data
                    idx += 1
                else:
                    # reset row to None
                    row[3:] = [None] * 6
                    uCursor.updateRow(row)


    except Exception as inst:
        print(inst)

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


if __name__ == '__main__':
    updateETA()
