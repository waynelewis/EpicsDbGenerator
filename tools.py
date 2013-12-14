#
# tools.py (c) Stuart B. Wilkins 2013
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Part of the "CableDatabase" package
#

"""
Python module to provide tools to use the CableDatabase
to auto configuring of EPICS substitution files and channel archiver
"""

import pystache
import os

# Dictionary mapping from MKS chanel connection (on controller) to channel number.

mksChan   = {'A' : 1, 'A1' : 1, 'A2' : 2,
			 'B' : 3, 'B1' : 3, 'B2' : 4,
			 'C' : 4, 'C1' : 4, 'C2' : 5}

# Dictionary mapping from MKS channel connection to relay number.
			   
mksRelays = {'A'  : [1,2,3,4],    'A1' : [1,2],  'A2' : [3,4],
			 'B'  : [5,6,7,8],    'B1' : [5,6],  'B2' : [7,8],
			 'C'  : [9,10,11,12], 'C1' : [9,10], 'C2' : [11,12]}

# Dictionary mapping Gamma Vacuum IPC connection to channel.

gammaChan  = { '1' : 1 , '2' : 1, '3' : 2, '4' : 2}

# Dictionary mapping Gamma Vacuum IPC channel to relay number.

gammaRelays = {'1' : [1,2,3,4], '2' : [1,2,3,4],
	           '3' : [5,6,7,8], '4' : [5,6,7,8]}

chanDict =  {'mksvgc'    : mksChan,
			 'gammaipc'  : gammaChan}
			
relayDict = {'mksvgc'    : mksRelays,
	         'gammaipc'  : gammaRelays}

def render(template, ofile, dictionary):
	"""Render the dictionary from a template file and write output.
	
	Render using pystache the template based on the dictionary. The template
	directory is generated from the location of this module. Write the output
	to file. 
	
	template   : Filename of the template.
	ofile      : Filename of output file.
	dictionary : Python dictionary of parameters for template.
	
	"""
	renderer = pystache.Renderer()
	templateFile = os.path.join(os.path.abspath(os.path.dirname(__file__)),'templates/{0}'.format(template))
	data = renderer.render_path(templateFile, dictionary)
	f = open(ofile, 'w')
	f.write(data)
	f.close()
	
def makeArchiverDict(sys, rows, name, signals, addSubsystem = False):
	"""Make dictionary to add to archiver
	
	Make a python dictionary for use with the EPICS channel
	archiver and pystache template. Multiple entries are added
	by providing multiple signals as a list. 
	
	row[2] defines the device, sys defines the system. The dictionary
	consists of a list of entries:
	
	{pv : PV + SIGNAL}
	
	sys 	     : String of system.
	rows	     : Table of data.
	name         : String of name for group.
	signals      : List of signals to archive.
	addSubsystem : If True, add the -XX subsystem to the system. 
	"""
	
	pvs = list()
	for row in rows:
		if row[2]:
			for signal in signals:
				if addSubsystem:
					subsys = ':{0:01d}'.format(row[3])
				else:
					subsys = ''
				pv = dict(pv=sys + subsys + '{' + row[2] + '}' + signal)
				pvs.append(pv)
			
	return [dict(name = name, channels = pvs)]

def makeSimpleDictionary(sys,rows, ports, source = False, unique = False, addSubsystem = False):
	"""Make Dictionary from all devices in list
	
	Make a "simple" dictionary for use with a pystache templates.
	A dictionary of the form:
	
	{ 
	  'sys'  : sys, (will use row[3]) for subsystem
	  'dev'  : row[1] for source (before the delimiting '-')
	           row[2] for destination.
	  'port' : row[1] (before the delimiting '-')
	}
	  
	If unique is true then each entry is unique and non-unique rows will be 
	ignored. 
	"""

	devices = list()	
	deviceList = list()
	for row in rows:
		if source:
			d = row[1].split('-')[0]
		else:
			d = row[2]
			
		if d:
			d = '{' + d + '}'
			if not (unique and (d in deviceList)):
				dev = dict()
				if addSubsystem:
					dev['dev'] = ':{0:01d}'.format(row[3])
				else:
					dev['dev'] = ''
				dev['port'] = ports['{' + row[1].split('-')[0] + '}']
				dev['sys'] = sys
				dev['dev']  = dev['dev'] + d
				devices.append(dev)
				deviceList.append(d)
	return devices

def makeVacuumDictionary(vtype, sys,rows,ports, addSubsystem = False):
	"""Make Dictionary for substitution file for vacuum devices
	
	vtype = dictionary type. ('mksvac' or 'gammaipc')
	
	Make a vacuum dictionary of the form:
	
	{
		'sys'   : sys, (will use row[3]) for subsystem
		'dev'	: row[2],
		'chan'	: Lookup of dictionary based on row[1] after delimiter.
		'cntl'	: row[1] before delimiter (including {})
		'port'	: Dictionay lookup of ports with key the same as cntl.
	}
	
	
	"""
	gauges = list()
	relays = list()
	for row in rows:
		# Each row is a CC Gauge
		# First do the actual Gauge
		
		if (row[1] is not '') and (row[2] is not ''):
			gauge = dict()
			
			if addSubsystem:
				gauge['dev'] = ':{0:01d}'.format(row[3])
			else:
				gauge['dev'] = ''
			gauge['sys'] = sys
			gauge['dev'] = gauge['dev'] + '{' + row[2] + '}'
			gauge['chan'] = chanDict[vtype][row[1].split('-')[1]]
			gauge['cntl'] = '{' + row[1].split('-')[0] + '}'
			gauge['port'] = ports[gauge['cntl']]
			gauges.append(gauge)
			
			# Now set the relay
			for spnum in relayDict[vtype][row[1].split('-')[1]]:
				relay = gauge.copy()
				relay['spnum'] = spnum
				relays.append(relay)
			
	return gauges, relays
	
