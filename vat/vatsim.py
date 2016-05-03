#!/usr/local/bin python

import csv
import requests
import datetime
import math
import sys
import sqlite3
import os.path
from datetime import timedelta
from decimal import Decimal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "vatsim.db")

conn = sqlite3.connect(db_path)

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
        # csv.py doesn't do Unicode; encode temporarily as UTF-8:
        csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                                dialect=dialect, **kwargs)
        for row in csv_reader:
            #decode UTF-8 back to Unicode, cell by cell:
            yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
        for line in unicode_csv_data:
            yield line.encode('utf-8')

def getNmFromLatLon(lat1, lon1, lat2, lon2):
        # returns distances in nautical miles given 2 sets of lat/lon coordinates
        R = 3443.89849 # km
        theta1 = math.radians(lat1)
        theta2 = math.radians(lat2)
        dtheta = math.radians(lat2-lat1)
        dlon = math.radians(lon2-lon1)

        a = math.sin(dtheta/2) * math.sin(dtheta/2) + math.cos(theta1) * math.cos(theta2) * math.sin(dlon/2) * math.sin(dlon/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        d = R * c

        return round(d)


def pilotInsert(row, update_time):
    #Deals with inserting pilot/flight data into the database
    
    updateTime = update_time
    call_sign = row[0]
    pilotId = row[1]

    if (row[16] != ''):
        user_rating = int(row[16])
    else:
        user_rating = ''
    if (user_rating == 0):
        user_rating = 'Not Rated'
    elif (user_rating == 1):
        user_rating = 'VATSIM Online Pilot'
    elif (user_rating == 2):
        user_rating = 'VATSIM Airmanship Basics'
    elif (user_rating == 3):
        user_rating = 'VATSIM VFR Pilot'
    elif (user_rating == 4):
        user_rating = 'VATSIM IFR Pilot'
    elif (user_rating == 5):
        user_rating = 'VATSIM Advanced IFR Pilot'
    elif (user_rating == 6):
        user_rating = 'VATSIM International and Oceanic Pilot'
    elif (user_rating == 7):
        user_rating = 'Helicopter VFR and IFR Pilot'
    elif (user_rating == 8):
        user_rating = 'Military Special Operations Pilot'
    elif (user_rating == 9):
        user_rating = 'VATSIM Pilot Flight Instructor'

    #Turn these variables into blank strings if theyre missing from vatsim
    #Looks ugly but the else statements were necessary because of unicode/ascii
    #conversions
    if (row[6] != ''):
        lon = float(row[6])
    else:
        lon = ''

    if (row[5] != ''):
        lat = float(row[5])
    else:
        lat = ''

    if (row[8] != ''):
        ground_speed = int(row[8])
    else:
        ground_speed = ''

    if (row[17] != ''):
        transpond = int(row[17])
    else:
        transpond = ''

    if (row[38] != ''):
        flight_heading = int(row[38])
    else:
        flight_heading = ''

    if (row[10] != ''):
        tascruise = int(row[10])
    else:
        tascruise = ''

    if (row[7] != ''):
        altitude = int(row[7])
    else:
        altitude = ''

    real_name = row[2]
    date_time = updateTime
    just_date = date_time[0:10]
    flight_date = datetime.datetime.strptime(just_date, "%Y-%m-%d")
    flight_date = flight_date.date()
    client_type = "Pilot"
    serv = row[14]
    aircraft = row[9]
    depairport = row[11]
    destairport = row[13]

    deptime = row[22][0:2] + ":" + row[22][2:4]
    #Some of the deptimes come in in a format that gets screwy, so lets check for that
    try:
        fdeptime = datetime.datetime.strptime(deptime, "%H:%M")
        flight_duration = datetime.datetime.utcnow() - datetime.combine(date.today(), fdeptime)
        flight_duration = str(flight_duration)
    except Exception, e:
        deptime = None
        flight_duration = "00:00"


    actdeptime = row[23][0:2] + ":" + row[23][2:4]
    #same issue here, check for it again
    try:
        factdeptime = datetime.datetime.strptime(actdeptime, "%H:%M")
    except Exception, e:
        actdeptime = None


    altairport = row[28]
    flight_remarks= row[29]
    flight_route = row[30]
    Route_String = ";" + str(lat) + "," + str(lon) + ";"
    flightStatus = ""
    logon = row[37]
    timelogon = logon[0:4] + "-" + logon[4:6] + "-" + logon[6:8] + " " + logon[8:10] + ":" + logon[10:12] + ":" + logon[12:14] # + "+0000"

    #Deal with Personal Table (SQL)
    #########################
    #########################

    cursor = conn.execute("SELECT * FROM Personal WHERE cid = ?", (pilotId,))
    if (cursor.fetchone() is None):
        conn.execute("INSERT INTO Personal (cid, realname, pilot_rating) VALUES (?, ?, ?)", (pilotId, real_name, user_rating))
        conn.commit()
    else:
        conn.execute("UPDATE Personal SET pilot_rating = ? WHERE cid = ?", (user_rating, pilotId))
        conn.commit()

    #Deal with Flights Table
    #########################
    #########################

    cursor = conn.execute("SELECT * FROM Flights WHERE just_date = ? AND callsign = ? AND cid = ?", (flight_date, call_sign, pilotId))

    if (cursor.fetchone() is None):
        try:
            cursor = conn.execute("SELECT * FROM AIRPORTS WHERE icao = ?", (depairport,))
            for row in cursor:
                origLat = row[3]
                origLon = row[4]
            #calculate distance based on the last lat/lon that we have seen
            dist = getNmFromLatLon(lat, lon, origLat, origLon)
        except Exception, e:
            dist = 0

        if (ground_speed < 50):
            ramp = updateTime
        else:
            ramp = None

        if (ground_speed > 50):
            off = updateTime
        else:
            off = None

        conn.execute("INSERT INTO Flights (just_date, callsign, cid, planned_aircraft, planned_tascruise, planned_depairport, planned_altitude, planned_destairport, planned_deptime, planned_actdeptime, planned_altairport, planned_remarks, planned_route, Routestring, duration, total_distance, time_logon, outRamp, offGround) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (flight_date, call_sign, pilotId, aircraft, tascruise, depairport, altitude, destairport, deptime, actdeptime, altairport, flight_remarks, flight_route, Route_String, flight_duration, dist, timelogon, ramp, off))
        conn.commit()
    else:
        cursor = conn.execute("SELECT * FROM Flights WHERE just_date = ? AND callsign = ? AND cid = ?", (flight_date, call_sign, pilotId))

        #initializing these variables here
        rString = "" #routestring
        dur = datetime.datetime.min - datetime.datetime.min #duration
        td = "" #total distance
        outR = "" #out ramp
        offG = "" # off ground
        onG = "" # on ground
        gTime = "" #ground time

        for row in cursor:
            rString = row[14]
            td = row[16]
            outR = row[18]
            offG = row[19]
            onG = row[20]
            gTime = row[22]

        #Get last lat/lon
        colonCount = 0
        colon1 = 0
        colon2 = 0
        comma = 0
        try:
            for i in range(len(rString), 0, -1):
                if ((rString[i] == ";") and (colonCount == 0)):
                    colon1 = i
                    colonCount +=1
                elif ((rString[i] == ",") and (colonCount == 1)):
                    comma = i
                elif ((rString[i] == ";") and (colonCount == 1)):
                    colon2 = i
            prevLat = decimal(rString[colon2+1:comma])
            prevLon = decimal(rString[comma+1:colon1])

            #Update Total Distance

            td += getNmFromLatLon(lat, lon, prevLat, prevLon)

        except Exception, e:
            pass

        #Update Route String
        rString += str(Route_String)

        #if the plane doesnt have an outramp or offground time set and its ground speed is less than 50,
        #set out ramp time. This means we probably haven't seen the flight yet.
        if ((outR == None) and (ground_speed < 50) and (offG == None)):
            outR = updateTime
            flightStatus = "On The Ground"

        #if the plane doesnt have off ground set and its going faster than 50, consider it off the ground.
        #Planes never go faster than 50 on the ground.
        if ((offG == None) and (ground_speed > 50)):
            offG = updateTime
            flightStatus = "Airborne"




        #Get Destination Airport Coords
        cursor = conn.execute("SELECT * FROM Airports WHERE icao = ?", (destairport,))
        destLat = 0
        destlon = 0
        for row in cursor:
            destLat = row[3]
            destlon = row[4]

        #Here we decided that we want to consider a flight as "Arrived" if it is within 5 knots of its destination Airport
        #And has a ground speed of less than 50. Planes are rarely going less than 50 knots/hour unless they are on the ground.
        if ((onG == None) and (ground_speed< 50) and (getNmFromLatLon(lat, lon, destLat, destlon) < 5) and (offG != None)):
            onG = updateTime
            flightStatus = "Arrived"
            dur = datetime.datetime.utcnow() - datetime.datetime.min

         #Update Total Duration
        if ((offG != None) and (onG == None)):
            dtfoffGround = datetime.datetime.strptime(offG[:len(offG)-5], "%Y-%m-%d %H:%M:%S")
            dur = datetime.datetime.utcnow() - dtfoffGround


        rString = str(rString)
        min = datetime.datetime.min
        dur = min + dur

        if((offG != None) and (outR != None)):
            #get rid of timezones and converting the strings to datetime format
            offGx = datetime.datetime.strptime(offG[:len(offG)-5], "%Y-%m-%d %H:%M:%S")
            outRx = datetime.datetime.strptime(outR[:len(outR)-5], "%Y-%m-%d %H:%M:%S")
            gTime = offGx - outRx
            gTime = gTime.total_seconds()


        conn.execute("UPDATE Flights SET Routestring = ?, duration = ?, total_distance = ?, outRamp = ?, onGround = ?, offGround = ?, groundTime = ? WHERE just_date = ? AND callsign = ? AND cid = ?", (rString, dur, td, outR, onG, offG, gTime, flight_date, call_sign, pilotId))
        conn.commit() #DONE UP TO HERE

    #Deal with ActiveFlights Table
    #########################
    #########################

    cursor = conn.execute("SELECT * FROM ActiveFlights WHERE cid = ? AND callsign = ?", (pilotId, call_sign))

    #see if active flight exists for this
    if (cursor.fetchone() is not None):
        for row in cursor:
            if (flightStatus == ""):
                flightStatus = row[11]

        #insert new flight

    conn.execute("DELETE FROM ActiveFlights WHERE cid = ? AND callsign = ?", (pilotId, call_sign))
    conn.commit()

    conn.execute("INSERT INTO ActiveFlights (datetime, callsign, cid, clienttype, latitude, longitude, server, altitude, groundspeed, transponder, heading, flight_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (date_time, call_sign, pilotId, client_type, lat, lon, serv, altitude, ground_speed, transpond, flight_heading, flightStatus))
    conn.commit()

if __name__ == "__main__":

    client_rows = []
    r = requests.get('http://info.vroute.net/vatsim-data.txt')
    data = r.text.splitlines()
    update = ""
    newUpdate = True #our script runs every 2 minutes but the data sheet doesn't update EXACTLY every two minutes.
    #so we will check later on to see if we have inserted this data already. If we have we'll set
    #newUpdate to False
    updateTime = ""

    reader = unicode_csv_reader(data,delimiter=":")

    for row in reader:
        if (row != []):
            if "UPDATE = " in row[0]:
                    date = row[0]
                    update = date[9:]
                    updateTime = update[0:4] + "-" + update[4:6] + "-" + update[6:8] + " " + update[8:10] + ":" + update[10:12] + ":" + update[12:14] + "+0000"


                    cursor = conn.execute("SELECT * FROM ActiveFlights WHERE datetime = ?", (updateTime,))
                    if (cursor.fetchone() is not None): #making sure its not empty
                        newUpdate = False

            elif (row[0] == u'!CLIENTS'):
                for row in reader:
                    if (row[0] == ";"):
                        break
                    client_rows.append(row)
    if (newUpdate == True):
        for row in client_rows:
            if (row[3] == 'PILOT'):
                pilotInsert(row, updateTime)


    ## Delete flights that have been missing for an hour

    cursor = conn.execute("SELECT * FROM ActiveFlights WHERE datetime NOT IN (?)", (updateTime,))

    for row in cursor:
        newdate = row[0]
        newdate = datetime.datetime.strptime(newdate[:len(newdate)-5], "%Y-%m-%d %H:%M:%S")
        if (newdate + timedelta(hours=1) <= datetime.datetime.utcnow()):
            conn.execute("DELETE FROM ActiveFlights WHERE cid = ? AND datetime = ? AND callsign = ?", (row[2], row[0], row[1]))
            conn.commit()
