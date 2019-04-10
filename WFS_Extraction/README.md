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
        * User input (config) currently administered via csv, for
          ease of testing; however a simple GUI could be implemented

    Requirements:
        * GDAL 2.4.x
        * Python 3.x (OSGEO4W packaged interpreter recommended for use)

    Contact:
        * Kieran Togher
        * https://github.com/ktogher
	

MIT License

Copyright (c) [2019] [Kieran Togher]