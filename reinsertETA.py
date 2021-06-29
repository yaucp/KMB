# import library
import json
import arcpy
import logging
import grequests


def updateETA():
    RSdata = []
    etaURL = []
    etaData = {}
    try:
        # Get all route-stop data to get info on route, stop for query link
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop',
                                    'SHAPE@XY', 'name_en', 'name_tc', 'name_sc', 'orig_en',
                                    'orig_tc', 'orig_sc')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # Base url link for data extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"

        # Get all responses from all the url of every possible route
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
            # Or else, parse the url in the response object as the key
            if resp_data:
                idx = (resp_data[0]['route'], str(resp_data[0]['service_type']))
                etaData[idx] = parseRouteETA(resp_data)

        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")
        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
            'name_en', 'name_tc', 'name_sc', 'orig_en', 'orig_tc', 'orig_sc',
            'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        with arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field) as iCursor:

            # Start extraction and importing data according to each row of RSdata
            for RS in RSdata:
                if (RS[0], RS[2]) not in etaData or (RS[1], RS[3]) not in etaData[(RS[0], RS[2])]:
                    # Write None data with current row of RSdata appended
                    row = list(RS) + [None] * 9
                    iCursor.insertRow(row)
                    # for i in range(total_etaseq):
                    #     iCursor.insertRow(row)
                else:
                    # Insert row with ETA data
                    data = etaData[(RS[0], RS[2])]
                    for eta in data[(RS[1], RS[3])]:
                        row = list(RS) + list(eta.values())[5:]
                        iCursor.insertRow(row)
                    # insert row when number of inserted row is less that total_etaseq
                    # if total_etaseq != 1:
                    #     for i in range(total_etaseq):
                    #         row = list(RS) + [None] * 6
                    #         iCursor.insertRow(row)

    except Exception as inst:
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result


if __name__ == '__main__':
    updateETA()
