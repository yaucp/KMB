# import library
import json
import arcpy
import logging
import grequests


# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result


def updateETA():
    RSdata = []
    etaURL = []
    etaData = {}
    try:
        with arcpy.da.SearchCursor("GDB/KMB.gdb/RouteStop",
                                   ('route', 'bound', 'service_type', 'seq', 'stop',
                                    'SHAPE@XY', 'name_en', 'name_tc', 'name_sc', 'orig_en',
                                    'orig_tc', 'orig_sc')) as sCursor:
            for row in sCursor:
                RSdata.append(list(row))

        # Base url link for data extraction
        kmbETA_url = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta/"
        # Used as current indicator and stored route NO. and service type
        currQ = None

        for RS in RSdata:
            tempURL = kmbETA_url + r"{}/{}".format(RS[0], RS[2])
            if tempURL not in etaURL:
                etaURL.append(tempURL)

        requests = (grequests.get(u) for u in etaURL)
        responses = grequests.map(requests)

        for r in responses:
            resp_data = json.loads(r.text)['data']
            if resp_data != []:
                idx = (resp_data[0]['route'], str(resp_data[0]['service_type']))
            else:
                idx = (r.url[54:r.url.find("/", 55)], r.url[r.url.find("/", 55) + 1:])
            etaData[idx] = parseRouteETA(resp_data)

        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")
        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
            'name_en', 'name_tc', 'name_sc', 'orig_en', 'orig_tc', 'orig_sc',
            'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        iCursor = arcpy.da.InsertCursor("GDB/KMB.gdb/ETA", field)

        # Start extraction and importing data according to each row of RSdata
        for RS in RSdata:
            data = etaData[(RS[0], RS[2])]
            if (RS[1], RS[3]) not in data:
                # Write None data with current row of RSdata appended
                row = list(RS) + [None] * 9
                iCursor.insertRow(row)
                # for i in range(total_etaseq):
                #     iCursor.insertRow(row)
            else:
                # Insert row with ETA data
                for eta in data[(RS[1], RS[3])]:
                    row = list(RS) + list(eta.values())[5:]
                    iCursor.insertRow(row)
                # insert row when number of inserted row is less that total_etaseq
                # if total_etaseq != 1:
                #     for i in range(total_etaseq):
                #         row = list(RS) + [None] * 6
                #         iCursor.insertRow(row)
        del iCursor

    except Exception as inst:
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))

    logging.info('Main Finished')
    print("Program Finished. Please check the log file for more information.")


# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(
            etaData)
    return result


if __name__ == '__main__':
    updateETA()
