#==========================================================================#
'''
GDAL WFS Extraction Tool

    Purpose:
        Extracts data from WFS sources and writes to file or database

    Features:
        * Automates data extraction and write process
        * Can be used as part of a full ETL solution
        * Allows for extraction from multiple sources
        * Makes requests to WFS server via a generated XML doc - allows
          for requests to be made to password protected services, and
          for further customisation to be made for request
        * Data can be output to any GDAL supported format
        * Threading has been implemented to allow for simultaneous
          downloads
        * User input (config) currently administered via csv, for
          ease of testing; however a simple GUI could be implemented

    Requirements:
        * GDAL 2.4.x
        * Python 3.x (OSGEO4W packaged interpreter recommended for use)

    Contact:
        * Kieran Togher
        * https://github.com/ktogher
		
	MIT License

'''
#==========================================================================#


import sys, os, csv, threading
from osgeo import ogr, gdal
from xml.etree import ElementTree


def gdal_error_handler(err_class, err_num, err_msg): #Function initialises GDAL error handler

    errtype = {
            gdal.CE_None: 'None',
            gdal.CE_Debug: 'Debug',
            gdal.CE_Warning: 'Warning',
            gdal.CE_Failure: 'Failure',
            gdal.CE_Fatal: 'Fatal'
    }
    err_msg = err_msg.replace('\n', ' ')
    err_class = errtype.get(err_class, 'None')
    ''' #Remove block comments to enable error printing
    print('Error Number: %s' % (err_num))
    print('Error Type: %s' % (err_class))
    print('Error Message: %s' % (err_msg))
    '''

class ConfigSheet(object): #Only one class used due to all config info being stored in a single csv (for testing) - should be altered depending on config setup
    #Object containing config values
    def __init__(self,name,url,wfs_uid,wfs_pass,layer,sql_clause,sql_field,sql_operator,sql_attribute,minX,minY,maxX,maxY,path,server,database,db_uid,db_pass):
        self.name = name
        self.url = url
        self.wfs_uid = wfs_uid
        self.wfs_pass = wfs_pass
        self.layer = layer
        self.sql_clause = sql_clause
        self.sql_field = sql_field
        self.sql_operator = sql_operator
        self.sql_attribute = sql_attribute
        self.minX = minX
        self.minY = minY
        self.maxX = maxX
        self.maxY = maxY
        self.path = path
        self.server = server
        self.database = database
        self.db_uid = db_uid
        self.db_pass = db_pass


def main():

    ''' #Remove block comments to print quantity of items to be downloaded
    pre_count = GetConfig()
    row_count = sum(1 for items in pre_count)
    show_count = str(row_count)
    print(show_count + ' items queued for download')
    '''
    #Reads data in config csv
    config_file = GetConfig()

    #Calls main worker, uses threading due to heavy download/read+write req
    t = []
    for row in config_file:
        config = ConfigSheet(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7]), str(row[8]), float(row[9]), float(row[10]), float(row[11]), float(row[12]), str(row[13]), str(row[14]), str(row[15]), str(row[16]), str(row[17]))
        p = threading.Thread(target=Generate(config.name, config.url, config.wfs_uid, config.wfs_pass, config.layer, config.sql_clause, config.sql_field, config.sql_operator, config.sql_attribute, config.minX, config.minY, config.maxX, config.maxY, config.path, config.server, config.database, config.db_uid, config.db_pass), args=(row,))
        p.daemon = True
        p.start()
        t.append(p)
    for p in t:
        p.join()


def Generate(ConfigName, ConfigURL, ConfigWFS_UID, ConfigWFS_Pass, ConfigLayer, ConfigClause, ConfigField, ConfigOperator, ConfigAttribute, ConfigMinX, ConfigMinY, ConfigMaxX, ConfigMaxY, ConfigPath, ConfigServer, ConfigDB, ConfigDB_UID, ConfigDB_Pass): #Function makes request to WFS server, retrieves data, filters data, writes data (for each dataset)

    #Set the driver
    wfs_drv = ogr.GetDriverByName('WFS')

    #Set GDAL config
    gdal.SetConfigOption('OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN', 'NO')
    gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED', 'YES')
    gdal.SetConfigOption('OGR_WFS_PAGE_SIZE', '10000')
    gdal.SetConfigOption('GDAL_HTTP_UNSAFESSL', 'YES')

    #Create xml to be sent as WFS 'url' (allows for requests to be made to password protected services)
    xmlParent = ElementTree.Element('OGRWFSDataSource')
    xmlChild1 = ElementTree.SubElement(xmlParent, 'URL')
    xmlChild1.text = ConfigURL
    xmlChild2 = ElementTree.SubElement(xmlParent, 'UserPwd')
    xmlChild2.text = ConfigWFS_UID+':'+ConfigWFS_Pass
    xmlChild3 = ElementTree.SubElement(xmlParent, 'HttpAuth')
    xmlChild3.text = 'BASIC'
    xmlChild4 = ElementTree.SubElement(xmlParent, 'Version')
    xmlChild4.text = '1.1.0'
    xmlChild5 = ElementTree.SubElement(xmlParent, 'PagingAllowed')
    xmlChild5.text = 'ON'
    xmlString = ElementTree.tostring(xmlParent, encoding="UTF-8")
    xmlConfig = open(ConfigPath + ConfigName + '.xml', 'wb')
    xmlConfig.write(xmlString)
    xmlConfig.close()

    #Make WFS request
    url = str(ConfigPath + ConfigName + '.xml')
    wfs_ds = wfs_drv.Open(url)
    if not wfs_ds:
        sys.exit('ERROR: can not open WFS datasource')
    else:
        pass

    # iterate over available layers
    for i in range(wfs_ds.GetLayerCount()):
        ''' # Remove block comments to display layer names available from WFS
        layers = wfs_ds.GetLayerByIndex(i)
        layername = layers.GetName()
        print(layername)
        '''

        #Requests specified layer
        foundLayer = wfs_ds.GetLayerByName(ConfigLayer)
        if not foundLayer:
            sys.exit('ERROR: can not find layer in service')
        else:
            pass

    #print('\n''Now downloading ' + ConfigLayer + '...''\n')

    #Create projection (.prj) file
    spatialref = foundLayer.GetSpatialRef()
    srs = spatialref.ExportToWkt()
    prj = open(ConfigPath + ConfigName + '.prj', 'w')
    prj.write(srs)
    prj.close()

    #BBOX constraint queries WFS @ server side
    foundLayer.SetSpatialFilterRect(ConfigMinX, ConfigMinY, ConfigMaxX, ConfigMaxY)

    # Attribute filtering (runs client side for most WFS requests)
    filter = str('"%s" %s '"'%s'") % (ConfigField, ConfigOperator, ConfigAttribute)
    foundLayer.SetAttributeFilter(filter)
    ''' #If it is preferred the below ExecuteSQL() method can be used as an alternative to the SetAttributeFilter - it still runs client side, but allows for operations such as joins to be performed
    sqlQuery = str('select * from %s %s %s %s %s') % ('"' + getLayer + '"', '' + ConfigClause + '', '' + ConfigField + '', '' + ConfigOperator + '',"" + ConfigAttribute + "")
    inLayer = wfs_ds.ExecuteSQL(sqlQuery)
    '''

    #Declare input data
    inLayer = foundLayer

    #############
    #!IMPORTANT!# - [The following comment applies to the rest of this function] - Remove references to .shp or MS SQL if only one output format is desired, otherwise leave as is to output WFS data to these formats; or change accordingly to a desired format e.g. GeoJSON, PG SQL, GML etc
    #############

    #Path for output .shp file
    output = ConfigPath + ConfigName + '.shp'

    #Set drivers
    driver = ogr.GetDriverByName('ESRI Shapefile')
    sqlDriver = ogr.GetDriverByName('MSSQLSpatial')

    #Get geometry for requested layer
    geometry = foundLayer.GetGeomType()

    #Make connection to MS SQL db
    connection = "MSSQL:uid="+ConfigDB_UID+";pwd="+ConfigDB_Pass+";server="+ConfigServer+";database="+ConfigDB+";trusted_connection=yes"
    openDB = sqlDriver.Open(connection)

    #Creates table in db - do not use overwrite method if data is to be appended
    sqlLayer = openDB.CreateLayer(ConfigName, spatialref, geometry, ['OVERWRITE=YES'])

    try:
        #Replaces existing duplicate .shp file if exists - again don't use if features are being appended
        if os.path.exists(output):
            driver.DeleteDataSource(output)

        #Creates output .shp
        outDataSource = driver.CreateDataSource(output)
        outLayer = outDataSource.CreateLayer(ConfigName, spatialref, geom_type=geometry)
    except:
        print('Error - please ensure file to be written to is not currently open and path is correct')

    #Get input layer definition from retrieved data
    inLayerDefn = inLayer.GetLayerDefn()

    try:
        #Get input data fields, create output data fields
        for i in range(0, inLayerDefn.GetFieldCount()):
            fieldDefn = inLayerDefn.GetFieldDefn(i)
            outLayer.CreateField(fieldDefn)
            sqlLayer.CreateField(fieldDefn)

        #Output layer def from fields created
        outLayerDefn = outLayer.GetLayerDefn()

        # Add features to the output Layer
        for inFeature in inLayer:

            # Create output Feature
            outFeature = ogr.Feature(outLayerDefn)

            # Add field values from input Layer
            for i in range(0, outLayerDefn.GetFieldCount()):
                outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(),
                                    inFeature.GetField(i))

            #Set geometry
            geom = inFeature.GetGeometryRef()
            outFeature.SetGeometry(geom.Clone())

            # Add new features to output Layer
            outLayer.CreateFeature(outFeature)
            sqlLayer.CreateFeature(outFeature)
    except:
        print('Error writing input feature data to output layer - check that you have write permissions to all output locations')

    try:
        #Create spatial index on .shp (can be removed if not needed)
        indexData = driver.Open(output, 1)
        indexData.ExecuteSQL("CREATE SPATIAL INDEX ON "+ConfigName+"")
    except:
        print('Error creating spatial index')


def GetConfig(): #Opens and reads config file

    try:
        config_location = str('C:/WFS_Extraction/config.csv') #Change as needed
        open_config = open(config_location, 'r')
        read_config = csv.reader(open_config, delimiter = ',')
        next(open_config) #skip header
        return read_config
    except:
        print("Error reading config file")


if __name__ == '__main__':
    # install error handler
    gdal.PushErrorHandler(gdal_error_handler)

    #Run main funtion
    main()