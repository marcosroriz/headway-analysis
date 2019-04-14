#!/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import click
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from postgis import register


def connectDB(db, dbuser, dbpass, line, spacing):
    # Connect to the PostgreSQL Database
    dbConnection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db, dbuser, dbpass))
    register(dbConnection)

    # Create a DB cursor and basic view for the script
    dbCursor = dbConnection.cursor()

    # DB Line View
    dbLineViewSQL = """
                    CREATE OR REPLACE VIEW LinhaInterpolada AS 
                    SELECT (ST_DumpPoints(ST_LineInterpolatePoints(wkb_geometry, {0}))).path[1],
                           (ST_DumpPoints(ST_LineInterpolatePoints(wkb_geometry, {0}))).geom
                    FROM linha{1};
                    """.format(spacing, line)
    dbCursor.execute(dbLineViewSQL)

    # DB MultiPoint Function
    dbMultiFunctionSQL = """
                         CREATE OR REPLACE FUNCTION ST_AsMultiPoint(geometry) RETURNS geometry AS
                         'SELECT ST_Union((d).geom) FROM ST_DumpPoints(ST_LineInterpolatePoints($1, {0})) AS d;'
                         LANGUAGE sql IMMUTABLE STRICT COST 10;
                         """.format(spacing)
    dbCursor.execute(dbMultiFunctionSQL)

    return (dbConnection, dbCursor)


def buildStopsFromFile(stopsFileName):
    """Build a dictionary containing bus stops information from a given file

    This function reads stopsFileName and creates a dictionary to hold information for each bus stop.
    Each bus stop is indexed by a given id (from column id in the file), and contains the following data:
    * the bus stop id
    * its position (latitude and longitude)
    * and travelled distance

    :param stopsFileName:
    :return: a dictionary containing bus stops information
    """
    stopsFile = open(stopsFileName)
    stopsReader = csv.DictReader(stopsFile)

    stops = dict()

    for aStop in stopsReader:
        index = int(aStop["id"])

        stops[index] = {}
        stops[index]["id"] = index
        stops[index]["lat"] = float(aStop["lat"])
        stops[index]["lng"] = float(aStop["lng"])
        stops[index]["dist"] = int(aStop["dist"])

    return stops



def getTravDistance(lat, lng, busLine, dbCursor):
    """ Returns the total travelled distance to position (lat, lng) in the given bus line
    :param lat: the bus's latitude position
    :param lng: the bus's longitude position
    :param busLine: the bus line
    :param dbCursor: a cursor to the database
    :return: total distanced travelled in meters to the position specified
    """
    # Map matching to discover closest point (projected)
    closestPointSQL = """
                      SELECT ST_ClosestPoint(ST_AsMultiPoint(linha.wkb_geometry), pt.geometry) AS ponto
                      FROM ST_GeomFromText('POINT({0} {1})', 4326) AS pt, 
                      linha{2} AS linha
                      """.format(lng, lat, busLine)
    dbCursor.execute(closestPointSQL)
    closestPoint = dbCursor.fetchone()

    # Get the index of map matched point
    pointIndexSQL = """
                    SELECT path, geom
                    FROM LinhaInterpolada
                    WHERE geom = %s
                    """
    dbCursor.execute(pointIndexSQL, (closestPoint))
    pointIndex = dbCursor.fetchone()

    # Get Travelled Distance
    distanceSQL = """
                  SELECT ST_Length(ST_MakeLine(linhainterpolada.geom), true)
                  FROM LinhaInterpolada AS linhainterpolada
                  WHERE path <= %s
                  """
    dbCursor.execute(distanceSQL, [pointIndex[0]])
    distance = dbCursor.fetchone()[0]

    return distance


def outlier(lat, lng, line, dbCursor):
    """ Check if a given latitude and longitude point is an outlier, i.e., it is not within the bus line buffer

    :param lat: latitude of the point
    :param lng: longitude of the point
    :param line: the busline id
    :param dbCursor: the database cursor
    :return: True if the point is over 100 meters of the line, False otherwise
    """
    pointDistanceSQL = """
                       SELECT ST_Distance(pt, linha.wkb_geometry, true)
                       FROM ST_GeomFromText('POINT({0} {1})', 4326) AS pt,
                            linha{2} AS linha
                       """.format(lng, lat, line)
    dbCursor.execute(pointDistanceSQL)
    pointDistance = dbCursor.fetchone()

    if pointDistance[0] >= 100:
        return True
    else:
        return False


def getLastBusStop(travDistance, busStops):
    lastStop = None

    for id, stop in busStops.items():
        if stop["dist"] <= travDistance:
            lastStop = stop
        elif stop["dist"] - 15 <= travDistance <= stop["dist"] + 15:
            # We consider a 15m error tolerance for travDistance (when it is close to a busStop)
            lastStop = stop

    return lastStop


def withinBusStop(travDistance, busStops):
    within = False

    for id, stop in busStops.items():
        if stop["dist"] - 15 <= travDistance <= stop["dist"] + 15:
            within = True

    return within


def processAVL(avlFileName, line, spacing, busStops, dbCursor):
    # List of raw headway at each stop
    rawHeadway = collections.defaultdict(list)

    # Last registered position of a given bus
    lastRegBusPosition = collections.defaultdict(dict)

    # Last registered bus stop
    lastRegBusStop = collections.defaultdict(dict)

    # Historical positions of a given bus stop
    historical = collections.defaultdict(list)
    lastPos = dict()

    # Read AVL file
    avlFile = open(avlFileName)
    avlReader = csv.DictReader(avlFile)

    # Process AVL data
    for avlData in avlReader:
        # Check if AVL's date and busline match the provided date and line period
        date = datetime.strptime(avlData["data"], "%Y-%m-%d %H:%M:%S")
        busLine = int(avlData["idlinha"])
        busID = int(avlData["idonibus"])
        busDirection = int(avlData["direcao"])
        lat = float(avlData["lat"])
        lng = float(avlData["lng"])
        letreiro = avlData["letreiro"]

        avl = {"date": date,
               "line": busLine,
               "busID": busID,
               "lat": lat,
               "lng": lng}

        if busLine == line and 12 <= date.hour < 14 and letreiro != "FORA DE SERVICO":
            print("PROCESSING AVL DATA AT: ", date.strftime("%c"))

            # Get Travelled Distance
            distance = getTravDistance(lat, lng, line, dbCursor)

            # Retrieve the last bus stop that this AVL has travelled
            lastBusStop = getLastBusStop(distance, busStops)

            # Check if AVL data is an outlier (going to garage, maintenance)
            if outlier(lat, lng, line, dbCursor):
                lastRegBusPosition[busID] = avl
                lastRegBusStop[busID] = lastBusStop
                continue

            # Retrieve the last registered bus stop that this AVL has travelled (that we registered)
            # Check if we have registered anything previously
            if not lastRegBusStop[busID]:
                # Ok, we have nothing
                # So, we register this bus stop
                lastRegBusPosition[busID] = avl
                lastRegBusStop[busID] = lastBusStop
                continue
            else:
                # Yes, we do have a previous record of this bus!
                # Let's get the data from the previous registered bus position
                prevDate = lastRegBusPosition[busID]["date"]
                prevLat = lastRegBusPosition[busID]["lat"]
                prevLng = lastRegBusPosition[busID]["lng"]
                prevDistance = getTravDistance(prevLat, prevLng, line, dbCursor)
                prevStopIndex = lastRegBusStop[busID]["id"]

                # Check if AVL is travelling in opposite direction (prev distance is bigger than current distance)
                if lastBusStop["id"] != 1 and (lastRegBusStop[busID]["id"] > lastBusStop["id"] or prevDistance > distance):
                    lastRegBusPosition[busID] = avl
                    lastRegBusStop[busID] = lastBusStop
                    continue

                # Get the number of travelled stops (diff between current and previous registered)
                numTravStops = int(lastBusStop["id"]) - int(lastRegBusStop[busID]["id"])

                if numTravStops > 0:
                    # We travelled through at least one bus stops

                    # Compute the velocity to get to the current position
                    deltaDistance = distance - prevDistance
                    deltaDate = (date - prevDate).total_seconds()
                    velocity = deltaDistance / deltaDate

                    # Compute the headway time for each travelled bus stop
                    for i in range(numTravStops):
                        passedStop = busStops[prevStopIndex + i + 1]
                        distancePassedBusStop = passedStop["dist"] - prevDistance
                        timePassedAtBusStop = prevDate + timedelta(seconds=(distancePassedBusStop / velocity))

                        rawHeadway[passedStop["id"]].append((busID, timePassedAtBusStop))
                        historical[busID].append(passedStop["id"])

                lastRegBusPosition[busID] = avl
                lastRegBusStop[busID] = lastBusStop

        if date.hour >= 14:
            break

    return rawHeadway


def deriveHeadway(rawHeadway):
    headway = dict()

    for busStopID in sorted(rawHeadway.keys()):
        rawHeadwaysAtStop = rawHeadway[busStopID]
        rawHeadwaysAtStop.sort(key=lambda x: x[1])
        headwayAtStopList = []

        # Get all time difference pairs at a given bus stop and do the pairwise headway computation
        for prev, next in list(zip(rawHeadwaysAtStop[:-1], rawHeadwaysAtStop[1:])):
            computedHeadway = (next[1] - prev[1]).total_seconds()
            headwayAtStopList.append(computedHeadway)

        # Generate Numpy Array
        headway[busStopID] = np.array(headwayAtStopList)

    return headway


@click.command()
@click.option("--avl",     default="dia.2019-02-18.csv",  help="AVL data")
@click.option("--line",    default=263,                   help="Bus Line")
@click.option("--stops",   default="data/263-pontos.csv", help="File containing Bus Stops")
@click.option("--spacing", default=0.0025,                help="Interpolation Spacing")
@click.option("--headway", default=930,                   help="Expected Scheduled Headway (in seconds)")
@click.option("--db",      default="highway",             help="PostGreSQL Database")
@click.option("--dbuser",  default="ufg",                 help="PostGreSQL User")
@click.option("--dbpass",  default="ufgufg",              help="PostGreSQL Password")
@click.option("--output",  default="saida.csv",           help="Output file")
def main(avl, line, stops, spacing, headway, db, dbuser, dbpass, output):
    # Create DB connection and get a cursor
    dbConnection, dbCursor = connectDB(db, dbuser, dbpass, line, spacing)

    # Parse Bus Stops
    busStops = buildStopsFromFile(stops)

    # Retrieve Raw Headways
    # Raw here means that we are just storing the datetime where a bus passes through the stop
    # We will calculate the headway (the difference between such occurrences) later
    rawHeadway = processAVL(avl, line, spacing, busStops, dbCursor)

    # Now, lets derive the Headway data for every bus stop from raw headway
    processedHeadway = deriveHeadway(rawHeadway)

    # Output processed headway
    
    # Output some statistics
    print("MEAN", "MIN", "MAX", "STDEV")
    for busStopID in sorted(processedHeadway.keys()):
        headwayAtStop = processedHeadway[busStopID]
        cvh = np.std(headwayAtStop - headway) / headway
        media = np.mean(headwayAtStop)
        min = np.min(headwayAtStop)
        max = np.max(headwayAtStop)
        print(busStopID, cvh, media, media/60, min, min/60, max, max/60)


if __name__ == "__main__":
    main()
