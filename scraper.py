# Scrape Thames CSO data

# scraperwiki.geo was removed in version 0.3.0; 0.2.2 was last available
import scraperwiki

# 2016-05-30: Attempt at coding around latest scraperwiki classic lib without geo;
# but AssertionError: 'T' is not an OSGB 500km square
# from osgb import convert
# TODO: unvestigate 'ukgeo' (0.2.2 README comment)

import xlrd
import datetime
import re
from collections import OrderedDict
from urllib2 import HTTPError
import xml.etree.ElementTree as ElementTree

# Normalised version of "discharge_type"
DISCHARGE_TYPES = {
	'Sewage Pumping Station': re.compile('(sps|sewage\s+pumping\s+station)', re.I),
	'Storm Sewer Overflow':   re.compile('(sewer\s+storm\s+overflow|storm\s+sewer\s+overflow)', re.I),
	'Storm Tank Overflow':    re.compile('(storm\s+tank)', re.I),
	'Outfall':                re.compile('(outfall|land\s+drain)', re.I),
};

SAFARI_WATERCOURSES = {
	'1': 'River Crane',
	'2': 'Yeading Brook (east)',
	'3': 'Yeading Brook (west)',
};


def scrapeXlsData(dataSetId, srcUrl, tableName):
	print "Scraping XLS dataset: ",dataSetId+" from: "+srcUrl

	xlbin = scraperwiki.scrape(srcUrl)
	book = xlrd.open_workbook(file_contents=xlbin)
	sheet = book.sheet_by_index(0) 

	keys = sheet.row_values(0)
	for i in range(len(keys)):
		keys[i] = keys[i].replace(' ','_').lower()

	rowNumber = 0
	rowsSaved = 0
	for rowNumber in range(1, sheet.nrows):
		# create dictionary of the row values
		values = [ cellval(c, book.datemode) for c in sheet.row(rowNumber) ]
		# zip(keys,values) combines the two arrays: keys (column headings) and values into a single map.
		data = dict(zip(keys, values))
		data['rownumber'] = rowNumber
		data['datasetid'] = dataSetId

		if 'grid_reference' in data:
			data['grid_ref'] = data['grid_reference']
			del data['grid_reference']

		if data.get('eastings') != None and data.get('northings') != None :
			location = scraperwiki.geo.os_easting_northing_to_latlng(data['eastings'], data['northings'])
			#print "east:",data['eastings']," north:",data['northings']," location:",location
			data['lat'] = location[0];
			data['lng'] = location[1];

		elif data.get('grid_ref') != None :
			location = scraperwiki.geo.osgb_to_lonlat(data['grid_ref'])
			#print "grid_ref:",data['grid_ref']," location:",location
			data['lat'] = location[1];
			data['lng'] = location[0];


		# Find normalised version of "discharge_type"
		data['ndt'] = normalisedDischargeType( data.get('discharge_type') )

		# only save if it is a full row (rather than a blank line or a note)
		if isValidRow(data):
			scraperwiki.sqlite.save(unique_keys = ['datasetid', 'rownumber'], 
						data = data, 
						table_name = tableName)
			print ("row({0},{1} saved: {2}".format(data['datasetid'], data['rownumber'], debug(data)))
			rowsSaved = rowsSaved + 1

	print "Dataset: {0} saved: {1}/{2}".format(dataSetId, rowsSaved, rowNumber)
	return rowsSaved


def cellval(cell, datemode):
	if cell.ctype == xlrd.XL_CELL_DATE:
		datetuple = xlrd.xldate_as_tuple(cell.value, datemode)
		if datetuple[3:] == (0, 0, 0):
			return datetime.date(datetuple[0], datetuple[1], datetuple[2])
		return datetime.date(datetuple[0], datetuple[1], datetuple[2], datetuple[3], datetuple[4], datetuple[5])
	if cell.ctype == xlrd.XL_CELL_EMPTY:    return None
	if cell.ctype == xlrd.XL_CELL_BOOLEAN:  return cell.value == 1
	return cell.value


def scrapeEpicollectXMLData(dataSetId, srcUrl, tableName):
	rowsSaved = 0
	rowNumber = 0
	print "Scraping Outfall dataset: ",dataSetId+" from: "+srcUrl
	xml = scraperwiki.scrape(srcUrl)
	dom = ElementTree.XML(xml)
	for entry in dom.findall('./table/entry'):
		rowNumber += 1
		data = dict()
		data['datasetid'] = dataSetId
		siteid = elementValueInt(entry, 'id')
		data['rownumber'] = siteid
		data['site_id'] = siteid
		data['site_name'] = elementValue(entry, 'AddOutFDesc', 'Outfall_Assessment_key')
		data['lat'] = elementValueFloat(entry, 'PWSI_GPS_lat')
		data['lng'] = elementValueFloat(entry, 'PWSI_GPS_lon')
		data['receiving_water'] = lookupWatercourse(entry)
		data['discharge_type'] = 'Outfall'
		data['ndt'] = normalisedDischargeType( data.get('discharge_type') )
		
		if isValidRow(data):
			scraperwiki.sqlite.save(unique_keys=['datasetid', 'rownumber'],
						data = data, 
						table_name = tableName)
			print ("Saved row: ({0},{1})".format(data['datasetid'], data['rownumber']))
			rowsSaved += 1

	print ("Dataset: {0} saved: {1}/{2} rows".format(dataSetId, rowsSaved, rowNumber))
	return rowsSaved


def isValidRow(row):
	return (row.get('datasetid') != None and 
		row.get('rownumber') != None)
		
		
def normalisedDischargeType(text):
	if (text is not None):
		for ndt in DISCHARGE_TYPES:
			if (DISCHARGE_TYPES[ndt].search(text) is not None):
				return ndt
	return None

def lookupWatercourse(entry):
	wcid = elementValue(entry, 'PWSO_watercourse')
	if (wcid is not None):
		wcname = SAFARI_WATERCOURSES.get(wcid)
		if (wcname is not None):
			return wcname
	return wcid

def elementValueInt(entry, *keys):
	value = elementValue(entry, *keys)
	try:
		return int(value)
	except:
		pass
	return value

def elementValueFloat(entry, *keys):
	value = elementValue(entry, *keys)
	try:
		return float(value)
	except:
		pass
	return value

def elementValue(entry, *keys):
	for k in keys:
		element = entry.find(k)
		if (element is None):
			continue
		if (element.text):
			return element.text.strip()
	return None


def debug(obj):
 	if isinstance(obj, dict):
 		out = []
		for k, v in sorted( obj.items() ):
			out.append( u'{0}: {1}'.format(k, v) )
		return '{'+', '.join(out)+'}'

	elif isinstance(obj, list) or isinstance(obj, tuple):
		out = []   
		for v in obj:
			out.append( v )
		return '['+', '.join(out)+']'

	else: 
		return obj;


def dropTable(tableName):
	sql = "DROP TABLE '{0}' IF EXISTS".format(tableName)
	executeSQL(sql)

def truncateTable(tableName, dataSetId):
	sql = "DELETE FROM '{0}' WHERE datasetid='{1}';".format(tableName, dataSetId)
	executeSQL(sql)

def createTable(tableName):
	columns = {
		'datasetid': 'text',
		'rownumber': 'integer',
		'site_name': 'text',
		'site_id': 'text',
		'discharge_type': 'text',
		'ndt': 'text',
		'receiving_water': 'text',
		'consent_reference': 'text',
		'lat': 'real',
		'lng': 'real',
		'eastings': 'real',
		'northings': 'real',
		'grid_ref': 'text'
	}
	
	sql = "CREATE TABLE IF NOT EXISTS '{0}' (".format(tableName)
	first = True
	for (colname, coltype) in columns.items():
		if (first):
			first = False
		else:
			sql += ', '
		sql += "'{0}' {1}".format(colname, coltype)
	sql += ' );'
	
	executeSQL(sql)

def executeSQL(sql):
	try:
		print("Executing SQL: {0}".format(sql))
		scraperwiki.sqlite.execute(sql);
	except BaseException as ex:
		print("SQL warning : {0}".format(ex))

	
# Main program

TABLENAME = 'cso_locations'

# truncateTable(TABLENAME, 'Crane-Outfall-Safari')
# dropTable(TABLENAME)
# createTable(TABLENAME)

SOURCES=[
		# { 'title':"DEP2009-2983", 'url':"http://www.parliament.uk/deposits/depositedpapers/2009/DEP2009-2983.xls" }, # old location
		#{ 'title':"DEP2009-2983", 'type':'xls', 'url':'http://data.parliament.uk/DepositedPapers/Files/DEP2009-2983/DEP2009-2983.xls' },
		#{ 'title':"Xl0000007", 'type':'xls', 'url':'http://www.cassilis.plus.com/TAC/Xl0000007.xls' },
		#{ 'title':"Crane-CSOs", 'type':'xls', 'url':'http://www.cassilis.plus.com/TAC/crane-cso-locations.xls' },
		#{ 'title':"Tributary-CSOs", 'type':'xls', 'url':'http://www.cassilis.plus.com/TAC/tributary-cso-locations.xls' },
		{ 'title':"Crane-Outfall-Safari", 'type':'epicollect', 'url':'http://plus.epicollect.net/RiverCraneZSL/download' },
	]

rowsTotal = 0
for source in SOURCES:
	try:
		if (source['type'] == 'xls'):
			rowsTotal += scrapeXlsData(source['title'], source['url'], TABLENAME)
		elif (source['type'] == 'epicollect'):
			rowsTotal += scrapeEpicollectXMLData(source['title'], source['url'], TABLENAME)
			
	except (HTTPError) as err:
		print ("Could not load url: {0} - {1}".format(source['url'], err))

print ("Saved {0} rows total to {1}".format(rowsTotal, TABLENAME))

