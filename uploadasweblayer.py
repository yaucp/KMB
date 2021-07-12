# import library
import arcpy, time, os
from arcgis.gis import GIS

def uploadasweblayer():
    # information needed
    username = "esrichinahk"
    password = "OpenData7856"
    portalURL = r'https://www.arcgis.com'
    serviceName = "KMB_ETA_current"
    sddraftPath = r"C:\Users\user\PycharmProjects\pythonProject\PublishingSamples\Output\%s.sddraft" % (serviceName)
    sdPath = r"C:\Users\user\PycharmProjects\pythonProject\PublishingSamples\Output\%s.sd" % (serviceName)

    # login to arcgis online
    arcpy.SignInToPortal(portalURL, username, password)
    gis = GIS(portalURL, username, password)

    # Maintain a reference of an ArcGISProject object pointing to your project
    aprx = arcpy.mp.ArcGISProject(r"C:\Users\user\PycharmProjects\pythonProject\GDB\KMB.aprx")

    # Maintain a reference of a Map object pointing to your desired map
    aprxMap = aprx.listMaps("Map")[0]

    # Create FeatureSharingDraft and set service properties
    sharing_draft = aprxMap.getWebLayerSharingDraft("HOSTING_SERVER","FEATURE",serviceName,)
    sharing_draft.portalUrl = portalURL
    sharing_draft.summary = "My Summary"
    sharing_draft.tags = "My Tags"
    sharing_draft.description = "My Description"
    sharing_draft.credits = "My Credits"
    sharing_draft.useLimitations = "My Use Limitations"

    # remove files when files exist
    if os.path.exists(sddraftPath):
        os.remove(sddraftPath)
        os.remove(sdPath)

    # create Service Definition Draft
    sharing_draft.exportToSDDraft(sddraftPath)

    # convert Service Definition Draft to Service Definition
    arcpy.StageService_server(sddraftPath, sdPath)

    # update data
    sdltem = gis.content.search("{} AND owner:{}".format(serviceName, username), item_type="Service Definition")[0]
    sdltem.update(data = sdPath)

    # Upload to Arcgis Online
    # arcpy.UploadServiceDefinition_server(sdPath, 'My Hosted Services')

if __name__ == "__main__":
    starttime = time.time()
    uploadasweblayer()
    end = time.time()
    print(end - starttime)
