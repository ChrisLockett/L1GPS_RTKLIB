#!/usr/bin/python

# automate the processing of L1 GPS data using RTKLIB
#
# Chris G Lockett
#

import sys
import os
import subprocess
import time
import datetime
import calendar
import ConfigParser
import pymysql
import ftplib

debugoutput = True

configfilename = 'l1.config'
host = ''
port = 0
user = ''
passwd = ''
ubxdb = ''
solutionsdb = ''
stations = []
#ftp server to get the nav files from
ftpserver = ''

# Station to use as the "base station"
basestation = ''
# Good enuff location to use as the base station location
# more research needs to be done to know how good the location needs to be
baselocation = ''

# The maximum time wconvert or rnx2rtkp can take before giving up.
# If ether process takes longer then this it will be killed
processtimeout = 300 #sec ~5min

# options to use in the call of RTKLIB
rtkoptions = '-p 3 -u -c -m 15 -a -l'

#read in the details from the config file
def readConfigFile():
	configfile = configfilename
	Config = ConfigParser.ConfigParser()
	Config.read(configfile)

	#open the config file
	global host
	host = Config.get('DB',"dbhost")
	global port
	port = int(Config.get('DB',"dbport"))
	global user
	user = Config.get('DB',"dbuser")
	global passwd
	passwd = Config.get('DB',"dbpass")
	global ubxdb
	ubxdb = Config.get('DB',"ubxdb")
	global solutionsdb
	solutionsdb = Config.get('DB',"solutionsdb")
	global ftpserver
	ftpserver = Config.get('FTPNAV',"ftpserver")
	global localnavroot
	localnavroot = Config.get('FTPNAV',"localnavroot")
	global minnavsize
	minnavsize = int(Config.get('FTPNAV',"minnavsize"))
	global basestation
	basestation = Config.get('STATIONS',"basestation")
	global baselocation
	baselocation = Config.get('STATIONS',"baselocation")
	global numberofstations
	numberofstations = int(Config.get('STATIONS','numberofstations'))
	global processtimeout
	processtimeout = int(Config.get('OTHER',"processtimeout"))
	global dayspast
	dayspast = int(Config.get('OTHER',"dayspast"))

	if debugoutput:
		print "host = "+host
		print "port = "+str(port)
		print "user = "+user
		print "passwd = "+passwd
		print "ubxdb = "+ubxdb
		print "solutionsdb = "+solutionsdb
		print "ftpserver = "+ftpserver
		print "localnavroot = "+localnavroot
		print "basestation = "+basestation
		print "baselocation = "+baselocation
		print "processtimeout = "+str(processtimeout)
		print "numberofstations = "+str(numberofstations)
		print "dayspast = "+str(dayspast)

	global stations
	for i in range(1,numberofstations):
		stations.append(Config.get('STATIONS',"station"+str(i)+"name"))
	#now add the base station
	stations.append(Config.get('STATIONS',"basestation"))

	global rtkoptions
	rtkoptions = Config.get('RTK',"rtkoptions")
	if debugoutput:
		print str(stations)


#converts the datetime to the number of j2ksecs
def datetimeToJ2k(dt):
		unixtime = calendar.timegm(dt.utctimetuple())
		j2ksec = unixtime - 946728000
		return j2ksec

# Connects to the database and downloads the raw data from table into a file named
# outfilename.
def dbToUBXfile(table, starttime, duration, outfilename):

	# connect to the database
	print "Connecting to the database server " + host
	conn = pymysql.connect(host=host,port=port,user=user,passwd=passwd,db=ubxdb)
	cur = conn.cursor()

	#change the start time to j2ksec
	startj2k = datetimeToJ2k(starttime)

	#get the end time in j2k secs
	endj2k = startj2k + duration

	#open a out file
	outfile = open(outfilename, 'wb')

	#get all of the blobs that are in the time range
	#print "getting gps data from station "+table+" ("+str(startj2k)+","+str(endj2k)+")"
	q = "SELECT j2ksec AS t, rawGPS AS b FROM "+table+" WHERE j2ksec > "+str(startj2k)+" AND j2ksec < "+str(endj2k)
	r = cur.execute(q)
	print datetime.datetime.utcnow().strftime("%H:%M:%S.%f")+" "+table + " time("+str(startj2k)+","+str(endj2k)+") blobs retrieved " +  str(r)

	#write all of the blobs to the file
	for r in cur.fetchall():
		outfile.write(r[1])

	#close the file
	outfile.close()

	#closes the connection to the database
	cur.close()
	conn.close()

# returns the juilan day of the datetime
def julianDay(dt):
	return dt.timetuple().tm_yday

# download Nav data from the NASA ftp site if it is not stored locally for the
# given day in t
def downloadNavData(t):
	year4 = str(t.year)
	year2 = year4[2:4]
	dayofyear = str(t.timetuple().tm_yday).zfill(3)

	filename = "brdc"+dayofyear+"0."+year2+"n.Z"
	filenamedecom = "brdc"+dayofyear+"0."+year2+"n"
	topath = localnavroot+"\\"+year4+"\\"+dayofyear+"\\"+filename
	topathdecom = localnavroot+"\\"+year4+"\\"+dayofyear+"\\"+filenamedecom
	frompath = "/gps/data/daily/"+year4+"/"+dayofyear+"/"+year2+"n/"+filename

	#if there is a zero lenth file remove it
	if( os.path.isfile(topathdecom)):
		if(os.path.getsize(topathdecom) == 0):
			print "removing file "+topathdecom+" it has no data"
			os.remove(topathdecom)
		if(os.path.getsize(topathdecom) < minnavsize):
			print "removing file "+topathdecom+" it is too small and needs to be re downloaded"
			os.remove(topathdecom)

	#if the file exsists then don't download it
	if(os.path.isfile(topathdecom)):
		print topathdecom + " does not need to be downloaded"
		return

	print "frompath  - "+frompath
	print "topath    - "+topath

	print "connecting to the ftp server"
	ftp = ftplib.FTP(ftpserver)
	ftp.login("","")
	try:
		print "downloading file"
		d = os.path.dirname(topath)
		if not os.path.exists(d):
			os.makedirs(d)
		ftp.retrbinary("RETR " + frompath ,open(topath, 'wb').write)
	except:
		print "Error failed downloading nav file"
	ftp.quit()
	ftp.close()

	# if a file was downloaded
	if(os.path.isfile(topath)):
		# if the file size > 0 decompress it
		if os.path.getsize(topath) > 0:
                        try:
                                print "decompressing "+topath
                                os.system("gzip -d "+'"'+topath+'"')
                        except:
                                print "Error decompressing "+topath
		# if the file size is 0 remove it
		if(os.path.isfile(topathdecom)):
                        if os.path.getsize(topathdecom) == 0:
                                # delete the file
        			print "removing file "+topath+" it has no data"
                		os.remove(topath)

# gets the UBX files needed from the database
def getUBXfiles(st, duration):
	print datetime.datetime.utcnow().strftime("%H:%M:%S.%f")+" "+'Getting UBX files from database'
	#get the ubx files for all of the stations
	for station in stations:
		stationname = station+'.ubx'
		dbToUBXfile(station,st,duration,stationname)

# checks if any process in prcs is running
def anyProcessRunning(prcs):
	for prc in prcs:
		if(prc.poll() is None):
			#print 'process still running'
			return True
		#else:
			#print 'process stoped running'
	return False

# wait nsec for processes in prcs to finish
# return true if all processes finished
# return false if a process is still running
def waitForProcesses(nsec, prcs):
	procrunning = True
	t = 0
	while(anyProcessRunning(prcs) and t < nsec):
		t += 1
		print '.waiting for Processes to finish'
		time.sleep(1)
	if(t >= nsec):
		return False
	else:
		return True

# convert all of the ubx files in to rinex files useing convbin in RTKLIB
def convertUBXfiles():

	print datetime.datetime.utcnow().strftime("%H:%M:%S.%f")+" "+'Converting UBX files'

	prcs = []

	for station in stations:
		call = 'convbin ' + station + '.ubx'

		#print call
		prcs.append(subprocess.Popen(call,stdout=subprocess.PIPE,stderr=subprocess.PIPE))

	#get output and err messages from call
	for prc in prcs:
		output, errors = prc.communicate()
		# do somthing with the output

	#wait for all of the processing to finish exit if not
	if not waitForProcesses(processtimeout,prcs):
		print 'convert process failed'

	print 'DONE Converting UBX files'

# post process all of the rinex files.
def postProcessdata(st,interval):

	print datetime.datetime.utcnow().strftime("%H:%M:%S.%f")+" "+'Post processing GPS data'
	prcs = []

	#make a string of all of the needed nav files
	navfiles = ""
	t = st - datetime.timedelta(days=1)
	#list all of the dayly nav files needed
	while(t <= st + datetime.timedelta(seconds=interval) + datetime.timedelta(seconds=3600)):
		gpsday = t.timetuple().tm_yday
		gpsyear = t.year
		downloadNavData(t)
		navfilename = 'brdc' + str(gpsday).zfill(3) + '0.' + str(gpsyear - 2000) + 'n'
		navfilepath = localnavroot + '\\' + str(gpsyear) + '\\' + str(gpsday).zfill(3) +'\\'+ navfilename
		#add the file if there is a file
		if(os.path.isfile(navfilepath) and not os.path.getsize(navfilepath) == 0):
			navfiles = navfiles +' "'+navfilepath+'"'
		t = t + datetime.timedelta(days=1)

	for station in stations:
		outfname = station+'.pos'
		call = 'rnx2rtkp '+rtkoptions+' '+baselocation+' -o '+ outfname +' '+station+'.obs '+basestation+'.obs '+ navfiles
		print call
		prcs.append(subprocess.Popen(call,stdout=subprocess.PIPE,stderr=subprocess.PIPE))
	#get output and err messages from call
	for prc in prcs:
		output, errors = prc.communicate()

	#wait for all of the processing to finish exit if not
	if not waitForProcesses(processtimeout,prcs):
		print 'post processing process failed'

	print 'DONE Post processing GPS data'

# insert the data from all of the POS files with a quality level of 2 or better
def insertDataFromPosFiles():

	print datetime.datetime.utcnow().strftime("%H:%M:%S.%f")+" "+'Inserting Post processed data'

	tid = 1
	rid = 1
	conn = pymysql.connect(host=host,port=port,user=user,passwd=passwd,db=solutionsdb)
	cur = conn.cursor()

	for station in stations:

		#quality 2 data from the file
		q2data = 0

		path = station+'.pos'
		posfile = open(path, 'r')
		lines = posfile.readlines()
		for l in lines:
			f = l.split()
			if( len(f) >= 15 and f[5] == '1'):
				d = f[0]
				t = f[1]
				e = f[2]
				n = f[3]
				u =  f[4]
				q = f[5]

				q2data = q2data + 1
				tablename = station
				timetag = d + " " + t
				j2ksec = datetimeToJ2k(datetime.datetime.strptime(timetag,"%Y/%m/%d %H:%M:%S.%f"))
				q = "REPLACE INTO "+tablename+" (j2ksec,E,N,U,tid,rid) VALUES("
				q = q +str(j2ksec)+","+str(e)+","+str(n)+","+str(u)+","+str(tid)+","+str(rid)+")"
				r = cur.execute(q)
				#print q
		print str(q2data) + ' quality 1 records added from ' + station
		print 'DONE Inserting Post processed data'

# cleans up the .pos, .obs, and .ubx files
def cleanup():

	print 'Removing files'

	#remove temp files
	os.system("del *.pos")
	os.system("del *.obs")
	os.system("del *.ubx")

#make fake files to keep the process from failing
def makeFakeFiles():
	for station in stations:
		obsfilename = station + '.obs'
		posfilename = station + '.pos'
		outobs = open(obsfilename, 'wb')
		outpos = open(posfilename, 'wb')
		outobs.close()
		outpos.close()

#MAIN
if __name__ == '__main__':
	# start time
	# start two days ago
	readConfigFile()
	st = datetime.datetime.utcnow() - datetime.timedelta(days=dayspast)
	#st = datetime.datetime(2014,12,6,0,0,0,0)
	# end time
	# end time (now)
	et = datetime.datetime.utcnow()
	#et = datetime.datetime(2014,12,12,0,0,0,0)
	# one day in sec = 86400
	#interval = 86400 * 7
	interval = 86400
	overlap = 0
	while(st < et):
		print "###################" + str(st) + "##################"
		print "FROM "+ st.strftime("%H:%M:%S.%f")+ " TO "+ et.strftime("%H:%M:%S.%f")
		print "###############################################################"
		getUBXfiles(st,interval+overlap)
		makeFakeFiles()
		convertUBXfiles()
		postProcessdata(st,interval+overlap)
		insertDataFromPosFiles()
		cleanup()
		st = st + datetime.timedelta(seconds=interval)
