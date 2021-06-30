# import library
# import for accessing arcgis pro database
import arcpy
# import for multi-threading
import concurrent.futures
# for requesting online database
import requests
# for time counting
import time
# for converting json type online data into list
import json

# Function for changing list resp into dictionary with the bus route's direction and sequence stop number as key
def parseRouteETA(resp):
    result = {}
    for etaData in resp:
        result.setdefault((etaData['dir'], etaData['seq']), []).append(etaData)
    return result

# used for online data extraction
def online_extract(url):
    data = requests.get(url)
    return data

def main():

    # initation
    RSdata = []
    etaURL = []
    etaData = {}

    try:

        # import data from Routestop database for url
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

        # making necessary url for online data extraction
        for RS in RSdata:
            tempURL = kmbETA_url + r"{}/{}".format(RS[0], RS[2])
            if tempURL not in etaURL:
                etaURL.append(tempURL)

        # extracting online data using multi-threading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            responses = executor.map(online_extract, etaURL)

        # convert extracted data into dictionary type variable
        for r in responses:
            resp_data = json.loads(r.text)['data']
            if resp_data != []:
                idx = (resp_data[0]['route'], str(resp_data[0]['service_type']))
            else:
                idx = (r.url[54:r.url.find("/", 55)], r.url[r.url.find("/", 55) + 1:])
            etaData[idx] = parseRouteETA(resp_data)

        # dalete all existing data inside the ETA database
        arcpy.management.TruncateTable("GDB/KMB.gdb/ETA/")
        # database colume
        field = (
            'route', 'bound', 'service_type', 'seq', 'stop', 'SHAPE@XY',
            'name_en', 'name_tc', 'name_sc', 'orig_en', 'orig_tc', 'orig_sc',
            'dest_tc', 'dest_sc', 'dest_en', 'eta_seq',
            'eta', 'rmk_tc', 'rmk_sc', 'rmk_en', 'timestamp')

        # insertcursor
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
    # print error
    except Exception as inst:
        print(inst)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))

if __name__ == '__main__':
    starttime = time.time()
    main()
    endtime = time.time()
    print("Program Finished. Please check the log file for more information.")
    print(endtime - starttime)
