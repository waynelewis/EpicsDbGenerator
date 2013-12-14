#!/usr/bin/env python

import MySQLdb as mdb
import math

### Here are the database parameters ###

dbHost = 'localhost'
dbUser = 'cdbuser'
dbPassword = 'cdbuser'
dbName = 'NSLS2CableDatabase'
dbPort = 3306

trayREGEXP = "[[:<:]]{0}[A-Z][[:>:]]|[[:<:]]{0}[[:>:]]"

dbColumns = ['branch', 'vacuumSection', 'sourceID', 'destinationId', 'cableType', 'cableUse']
columnNames =  ['Branch', 'Vacuum Section', 'Source ID', 'Destination ID', 'Cable Type', 'Cable Use']

### End database parameters ###

def databaseConnect():
    """Connect to SQL database and return connection object"""
    try:
        con = mdb.connect(dbHost, dbUser,dbPassword,dbName, dbPort); 
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        return None
    return con
    
def databaseSelect(cmd, toPass = None):
	"""Make mySQL select and return rows.
	
	Open DB connection and make select.
	Return all rows and then close connection to database. 
	Returns rows from fetchall()
	"""
	db = databaseConnect()
	cur = db.cursor()
	if toPass is None:
		cur.execute(cmd)
	else:
		cur.execute(cmd, toPass)
	rows = cur.fetchall()
	cur.close()
	db.close()
	return rows
	
def doConnectionSearch(criteria):
	"""Search database for specific cable use and return connection details"""
	cmd =  "SELECT id,sourceConnection, destinationConnection, branch"
	cmd += " FROM CableDatabase"
	cmd += " WHERE "
	
	whereList = list()
	for key,val in criteria.iteritems():
		if type(val) == type(list()):
			whereList.append('{0} IN ({1})'.format(key,",".join([("'" + v + "'") for v in val])))
		else:
			whereList.append("{0} = '{1}'".format(key,val))
	
	cmd += " AND ".join(whereList)
	
	return databaseSelect(cmd)
	
def doCableSearch(keys):
	"""Do search of cable database and return ids of results"""
	cmd =  "SELECT a.*, b.cableLongName"
	cmd += " FROM CableDatabase a, CableTypes b WHERE a.cableType = b.cableType "
	
	s = []
	for key,arg in keys.items():
		if arg is not None:
			if type(arg) is not list:
				a = ["'" + arg + "'"]
			else:
				a = ["'" + aa + "'" for aa in arg]
			c = 'a.%s in (' % key
			c += ",".join(a)
			c += ")"
			s.append(c)
	if len(s):
		cmd += " AND " + " AND ".join(s)
		
	return databaseSelect(cmd)


def makeCableLabel(data, end):
    """Make cable label from initial data

    Assumes that data is in form

    0 : ID
    1 : Branch
    9 : Source ID
    10 : Destination ID

    end is the string to terminate, such as 'A'
    """
    return "23ID:%d-%s-%s-%s-%s" % (data[1], data[0], data[9], data[10], end)

def getCableSpecSheet(id):
	"""Return file of cable spec sheet"""
	cmd = "SELECT a.mimetype,a.data from Files a, CableTypes b WHERE b.cableType = '{0}' AND a.id = b.specFile".format(id)
	rows = databaseSelect(cmd)
	if rows[0] is None:
		return None, None	
	return rows[0]
	
def getCableWiring(id):
	"""Return file of cable spec sheet"""
	cmd = "SELECT a.mimetype,a.data from Files a, CableWiring b WHERE b.cableWiring = '{0}' AND a.id = b.specFile".format(id)
	rows = databaseSelect(cmd)
	if rows[0] is None:
		return None, None	
	return rows[0]

def getSourceConnectionList(source):
    """Get source connection list from database and return rows"""
    
    cmd =  "SELECT a.*,b.cableLongName"
    #cmd += "a.cableUse,a.sourceConnection,a.destinationConnection,a.cableWiring"
    cmd += ' FROM CableDatabase a, CableTypes b WHERE a.sourceID = "{0}" AND a.cableType = b.cableType'.format(source)
    return databaseSelect(cmd)
    
#def getPullSheetData(keys):
#	"""Get Pullsheet Data from search made up of keys"""
#	rows = doCableSearch(['a.sourceID, a.cablePath, a.destinationID'], keys)
#	return rows
#	
#def getAllData(keys):
#	"""Get all columns of use from the database"""
#	rows = doCableSearch(['a.sourceID, a.destinationID, a.cablePath, a.cableWiring, a.cableInstalled'], keys)
#	return rows
	
def getData(ids):
	"""Get all data based on list of ids"""
	cmd = "SELECT * FROM CableDatabase WHERE id in ({0})".format(",".join(ids))
	return databaseSelect(cmd)

def getAllAndSort(table, field):
	"""Return all the unique values from a field"""
	cmd = "SELECT DISTINCT({0}) AS {0} FROM {1} ORDER BY {0};".format(field, table)
	return databaseSelect(cmd)

def getAllTrayLoadings():
	cmd = "SELECT id from CableTrays"
	rows = databaseSelect(cmd)
	
	allrtn = []
	for row in rows:
		rtn = calculateTrayLoading(row[0])
		if rtn is not None:
			allrtn.append([row[0]] + list(rtn))
			
	return allrtn
	
def getCableCount(trayTag):
	"""return the ids of the cables in a given tray element"""
	# First get all cable types
	
	cableTypes = getAllAndSort('CableDatabase', 'cableType')
	
	# now loop over all cables, returning number in given segment.
	
	cablesDict = dict()
	for cable in cableTypes:
		cmd =  "SELECT id FROM CableDatabase"
		cmd += " WHERE cableType='{0}'".format(cable[0])
		cmd += " AND (cablePath REGEXP '{0}' OR destinationID = '{1}');".format(trayREGEXP.format(trayTag),trayTag)
		rows = databaseSelect(cmd)
		cablesDict[cable[0]] = [row[0] for row in rows]
	
	return cablesDict
	
def getAllCableTypes():
	"""Return Cable Types and Description"""
	cmd = "SELECT cableType, cableLongName from CableTypes ORDER BY cableType"
	rtn = databaseSelect(cmd)
	return rtn
	
def getAllCableCount():
	"""Return a list of all cables in all trays"""
	cmd = "SELECT id from CableTrays ORDER BY id"
	rows = databaseSelect(cmd)
	
	allrtn = dict()
	for row in rows:
		rtn = getCableCount(row[0])
		allrtn[row[0]] = rtn
		
	return allrtn
	
def calculateTrayLoading(trayTag):
	"""Calculate the tray loading"""
	# To calculate the tray loading for a given tray we first do
	# a regex on the database to get the number of cables.
	
	rexp = trayREGEXP.format(trayTag)
		 	
	cmd =  "SELECT a.id, b.cableDiameter, b.defaultDivider, a.cablePath, a.sourceID, a.destinationID"
	cmd += " FROM CableDatabase a, CableTypes b"
	cmd += " WHERE a.cableType = b.cableType" 
	cmd += " AND (a.destinationID = '{0}' OR a.cablePath REGEXP '{1}')".format(trayTag, rexp)
		
	rows = databaseSelect(cmd)
		
	if rows is None:
		return None
		
	loading = dict()
	count = dict()
	noneCount = 0;
	
	for row in rows:
		div = row[2]
		dia = row[1]
		if dia is None:
			noneCount += 1;
			continue
		dia = math.pi * math.pow((row[1]/2.0),2)
		if div in loading:
			loading[div] += dia
			count[div] += 1
		else:
			loading[div] = dia
			count[div] = 1
		
	# Now get divider schedule

	if len(loading) == 0:
		return None
	
	cmd =  "SELECT " + ",".join(["div{0}Size".format(a) for a in loading.keys()])
	cmd += " FROM CableTrays WHERE id = '{0}'".format(trayTag)

	trow = databaseSelect(cmd)
	trow = trow[0] # Use first row, there is only one!
	
	# Now go through all the dividers that we have and get the areas to calculate
	# percentage fill
	
	area = dict()
	percentFill = dict()
	for a,b in zip(loading.keys(), trow):
		area[a] = b
		percentFill[a] = loading[a] / area[a]
	
	return (loading, percentFill, count, noneCount)
