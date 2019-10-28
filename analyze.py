#!/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import click
import numpy as np
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.style as style

from matplotlib import cm
from matplotlib.dates import MINUTELY, DateFormatter, rrulewrapper, RRuleLocator, drange, MinuteLocator
from matplotlib.ticker import MaxNLocator, MultipleLocator, FormatStrFormatter, AutoMinorLocator
from datetime import datetime, timedelta
from postgis import register
from haversine import haversine, Unit


colors = ["#E58606","#5D69B1","#52BCA3","#99C945","#CC61B0","#24796C","#DAA51B","#2F8AC4","#764E9F","#ED645A","#CC3A8E","#A5AA99",
          "#88CCEE","#CC6677","#DDCC77","#117733","#332288","#AA4499","#44AA99","#999933","#882255","#661100","#6699CC","#888888"]
colordict = {"i": 0}


def getColor(busID, d):
    # if d.hour <= 8:
    #     return "#e07f05"
    # else:
    #     return "#50b28d"

    i = colordict["i"]
    if busID not in colordict:
        colordict[busID] = i
        colordict["i"] = (i + 1) % len(colors)
    else:
        i = colordict[busID]

    return colors[i]


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

    dbConnection.commit()
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
    stopsFile = open(stopsFileName, encoding='utf-8')
    stopsReader = csv.DictReader(stopsFile)

    stops = dict()

    for aStop in stopsReader:
        index = int(aStop["id"])

        stops[index] = {}
        stops[index]["term"] = bool(int(aStop["term"]))
        stops[index]["id"] = index
        stops[index]["lat"] = float(aStop["lat"])
        stops[index]["lng"] = float(aStop["lng"])
        stops[index]["dist"] = int(aStop["travdist"])
        # stops[index]["nome"] = str(aStop["nome"])

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

    if pointDistance[0] >= 250:
        return True
    else:
        return False


def getLastBusStop(lat, lng, travDistance, busStops):
    lastStop = busStops[1]
    within = False

    for id, stop in busStops.items():
        if stop["term"] and haversine((lat, lng), (stop["lat"], stop["lng"]), unit=Unit.METERS) <= 100:
            lastStop = stop
            within = True
            break
        elif not stop["term"] and haversine((lat, lng), (stop["lat"], stop["lng"]), unit=Unit.METERS) <= 15:
            lastStop = stop
            break
        elif stop["dist"] <= travDistance:
            lastStop = stop

    return lastStop, within


def getVelocityAndDistance(tx, ty, tp, line, dbCursor):
    lat, lng = tp[-1]
    prevLat, prevLng = tp[-2]

    distance = getTravDistance(lat, lng, line, dbCursor)
    prevDistance = getTravDistance(prevLat, prevLng, line, dbCursor)

    date = tx[-1]
    prevDate = tx[-2]

    # We travelled through at least one bus stops
    # Compute the velocity to get to the current position
    deltaDistance = distance - prevDistance
    deltaDate = (date - prevDate).total_seconds()
    velocity = deltaDistance / deltaDate

    return velocity, distance


def processAVL(avlFileName, line, spacing, start, end, busStops, dbCursor):
    # List of raw headway at each stop
    rawHeadway = collections.defaultdict(list)

    # Last registered position of a given bus
    lastRegBusPosition = collections.defaultdict(dict)

    # Last registered bus stop
    lastRegBusStop = collections.defaultdict(dict)

    # Historical position of vehicles
    historicalAVL = dict()

    # Headway Trips (by busID)
    trips = collections.defaultdict(dict)

    # Read AVL file
    avlFile = open(avlFileName)
    avlReader = csv.DictReader(avlFile)

    # Periodiciade e Numero de Dados
    periodicidade = []
    numdadosbrutos = 0
    numdadosfiltro = 0

    # Graphics Config
    style.use("seaborn-paper")
    fig, ax = plt.subplots()
    ax.xaxis.set_major_formatter(DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(MinuteLocator(byminute=[0,10,20,30,40,50]))
    ax.xaxis.set_minor_locator(MinuteLocator(interval=1))

    ax.xaxis.set_tick_params(rotation=90)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_minor_locator(MultipleLocator(1))
    ytick = [1, 5, 10, 15, 20, 25, 30, 35, 37]
    ylabel = ["T. Praça A", 5, 10, 15, "T. Praça da Bíblia", 25, 30, 35, "T. Praça A"]
    # for v in busStops.values():
    #     ytick.append(v["id"])
    #     ylabel.append(v["nome"])
    #
    plt.yticks(ytick, ylabel)

    # Process AVL data
    for avlData in avlReader:
        # Check if AVL's date and busline match the provided date and line period
        date = datetime.strptime(avlData["data"], "%Y-%m-%d %H:%M:%S")
        busLine = int(avlData["idlinha"])
        busID = int(avlData["idonibus"])
        lat = float(avlData["lat"])
        lng = float(avlData["lng"])
        letreiro = avlData["letreiro"]

        avl = {
            "date": date,
            "line": busLine,
            "busID": busID,
            "lat": lat,
            "lng": lng
        }

        if busLine == line and start <= date.hour < end and letreiro != "FORA DE SERVICO":
            print(avl)
            numdadosbrutos = numdadosbrutos + 1

            # Check if data is duplicate
            if lastRegBusPosition[busID]:
                if lastRegBusPosition[busID]["lat"] == lat and lastRegBusPosition[busID]["lng"] == lng:
                    print("POSIÇÃO IDÊNTICA")
                    continue

            # Check if AVL data is an outlier (going to garage, maintenance)
            if outlier(lat, lng, line, dbCursor):
                lastRegBusPosition[busID] = avl
                if busID in lastRegBusStop:
                    del lastRegBusStop[busID]
                continue

            numdadosfiltro = numdadosfiltro + 1

            # Ok, AVL is not an outlier
            # Get Travelled Distance
            distance = getTravDistance(lat, lng, line, dbCursor)

            # Retrieve the last bus stop that this AVL has travelled
            lastBusStop, within = getLastBusStop(lat, lng, distance, busStops)

            if busID == 20142:
                print("bugzao")

            if busID == 20142 and date.hour == 11 and date.minute == 15 and date.second == 42:
                print("tou aqui")

            # Retrieve the last registered bus stop that this AVL has travelled (that we registered)
            # Check if we have registered anything previously
            if not lastRegBusStop[busID]:
                # Ok, we have nothing
                # So, we register this bus stop
                lastRegBusPosition[busID] = avl
                lastRegBusStop[busID] = lastBusStop

                trips[busID]["x"] = [date]
                trips[busID]["d"] = [date]
                trips[busID]["y"] = [lastBusStop["id"]]
                trips[busID]["p"] = [(lat, lng)]
                print("INICIALIZANDO a lista do busID", busID)
                continue
            else:
                # Yes, we do have a previous record of this bus!
                # Let's get the data from the last registered bus position
                prevAVL = lastRegBusPosition[busID]
                prevDate = prevAVL["date"]
                prevLat = prevAVL["lat"]
                prevLng = prevAVL["lng"]
                prevDistance = getTravDistance(prevLat, prevLng, line, dbCursor)
                prevStopIndex = lastRegBusStop[busID]["id"]
                _, prevWithin = getLastBusStop(prevLat, prevLng, distance, busStops)

                # Salva a periodiciade de envio
                periodicidade.append((date - prevDate).total_seconds())

                # Data muito passada
                if (
                        (lastRegBusStop[busID]["term"]
                            and (date - trips[busID]["x"][-1]).total_seconds() > 120
                            and haversine((lat, lng), trips[busID]["p"][-1], unit=Unit.METERS) <= 100)
                    or
                        (lastRegBusStop[busID]["term"]
                            and (date - prevDate).total_seconds() > 300)
                    or
                        (distance <= prevDistance and lastRegBusStop[busID]["id"] != lastBusStop["id"]
                           and len(trips[busID]["x"]) < 5)
                ):
                    lastRegBusPosition[busID] = avl
                    lastRegBusStop[busID] = lastBusStop
                    trips[busID]["x"][-1] = date
                    trips[busID]["d"][-1] = date
                    trips[busID]["y"][-1] = lastBusStop["id"]
                    trips[busID]["p"][-1] = (lat, lng)
                    print("REINICIALIZANDO a lista do busID", busID)
                    continue

                # Check if AVL has finished the trip (went back to first stop)
                if (len(trips[busID]["x"]) <= 2 and
                    lastRegBusStop[busID]["id"] > lastBusStop["id"] and lastBusStop["id"] == 1):
                    lastRegBusPosition[busID] = prevAVL
                    lastRegBusStop[busID] = lastBusStop

                    trips[busID]["x"] = [prevDate]
                    trips[busID]["d"] = [prevDate]
                    trips[busID]["y"] = [lastBusStop["id"]]
                    trips[busID]["p"] = [(prevLat, prevLng)]
                    print("BUGGGG da lista do busID", busID)
                    continue
                elif lastRegBusStop[busID]["id"] > lastBusStop["id"] and lastBusStop["id"] <= 2:
                    if len(trips[busID]["x"]) > 36:
                        print("parou aqui q da merda")

                    lastRegBusPosition[busID] = avl
                    lastRegBusStop[busID] = lastBusStop

                    if len(trips[busID]["x"]):
                        trips[busID]["x"].append(date)
                        trips[busID]["d"].append(date)
                        trips[busID]["y"].append(37)
                        trips[busID]["p"].append((lat, lng))
                        plt.plot_date(trips[busID]["x"], trips[busID]["y"], "-", color = getColor(busID, trips[busID]["x"][0]), marker="o", markersize=5)

                        # rawHeadway[passedStop["id"]].append((busID, timePassedAtBusStop))
                        for i in range(len(trips[busID]["x"])):
                            rawHeadway[trips[busID]["y"][i]].append((busID, trips[busID]["x"][i]))

                        trips[busID]["x"] = []
                        trips[busID]["d"] = []
                        trips[busID]["y"] = []
                        trips[busID]["p"] = []
                        del lastRegBusStop[busID]

                    continue
                elif lastRegBusStop[busID]["id"] > lastBusStop["id"]:
                    lastRegBusPosition[busID] = avl
                    continue
                elif (lastBusStop["id"] == 35 or lastBusStop["id"] == 36) and lastRegBusStop[busID]["id"] == 1:
                    print("CURVINHA INICIAL")
                    lastRegBusPosition[busID] = avl
                    continue

                # Get the number of travelled stops (diff between current and previous registered)
                numTravStops = int(lastBusStop["id"]) - int(lastRegBusStop[busID]["id"])

                if numTravStops > 0:

                    if lastBusStop["id"] == 2 and lastRegBusStop[busID]["id"] == 33:
                        print("deu pau")

                    # We travelled through at least one bus stops
                    # Compute the velocity to get to the current position
                    deltaDistance = distance - prevDistance
                    deltaDate = (date - prevDate).total_seconds()
                    velocity = deltaDistance / deltaDate

                    # Compute the headway time for each travelled bus stop
                    for i in range(numTravStops):
                        passedStop = busStops[prevStopIndex + i + 1]
                        distancePassedBusStop = passedStop["dist"] - prevDistance
                        timePassedAtBusStop = prevDate
                        if velocity != 0:
                            timePassedAtBusStop = prevDate + timedelta(seconds=(distancePassedBusStop / velocity))

                        if trips[busID]["x"]:
                            if trips[busID]["x"][-1] >= timePassedAtBusStop:
                                print("WTF!!!!")

                        # rawHeadway[passedStop["id"]].append((busID, timePassedAtBusStop))
                        if (timePassedAtBusStop.hour == 1 and timePassedAtBusStop.minute == 30 and timePassedAtBusStop.second == 10):
                            print("WTF ")

                        trips[busID]["x"].append(timePassedAtBusStop)
                        trips[busID]["d"].append(date)
                        trips[busID]["y"].append(passedStop["id"])
                        trips[busID]["p"].append((lat, lng))


                lastRegBusPosition[busID] = avl
                lastRegBusStop[busID] = lastBusStop

        if date.hour >= end:
            break

    # Salva e plota dados que não completaram
    for busID in trips:
        if busID in trips:
            if "y" in trips[busID] and len(trips[busID]["y"]) > 3 and trips[busID]["x"][0].hour >= start :
                toFinishStop = 37 - trips[busID]["y"][-1]

                if toFinishStop < 3 and toFinishStop > 0:
                    velocity, prevDistance = getVelocityAndDistance(trips[busID]["x"], trips[busID]["y"], trips[busID]["p"], line, dbCursor)
                    prevDate = trips[busID]["x"][-1]
                    prevStopIndex = trips[busID]["y"][-1]

                    for i in range(toFinishStop):
                        distancePassedBusStop = 14525 - prevDistance
                        stopid = 37
                        if (prevStopIndex + i + 1) != 37:
                            passedStop = busStops[prevStopIndex + i + 1]
                            distancePassedBusStop = passedStop["dist"] - prevDistance
                            stopid = passedStop["id"]

                        timePassedAtBusStop = prevDate
                        if velocity != 0:
                            timePassedAtBusStop = prevDate + timedelta(seconds=(distancePassedBusStop / velocity))

                        trips[busID]["x"].append(timePassedAtBusStop)
                        trips[busID]["y"].append(stopid)
                        trips[busID]["p"].append((lat, lng))

                plt.plot_date(trips[busID]["x"], trips[busID]["y"], "-", color = getColor(busID, trips[busID]["x"][0]), marker="o", markersize=5)

                for i in range(len(trips[busID]["x"])):
                    rawHeadway[trips[busID]["y"][i]].append((busID, trips[busID]["x"][i]))

    plt.show()
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


def writeOutput(processedHeadway, output):
    for busStopID in sorted(processedHeadway.keys()):
        outputfilename = "out/ponto." + str(busStopID) + "." + output
        with open(outputfilename, 'a+', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=',')
            # writer.writerow(["headway"])
            for h in processedHeadway[busStopID]:
                writer.writerow([h])


@click.command()
@click.option("--avl",     default="avl/diasuteis/dia.2019-06-12",     help="AVL data")
@click.option("--line",    default=400,                                help="Bus Line")
@click.option("--stops",   default="data/400-pontos-corrigido.csv",    help="File containing Bus Stops")
@click.option("--spacing", default=0.00025,                            help="Interpolation Spacing")
@click.option("--start",   default=5,                                  help="Start time")
@click.option("--end",     default=12,                                 help="End time")
@click.option("--headway", default=1050,                               help="Expected Scheduled Headway (in seconds)")
@click.option("--db",      default="highway",                          help="PostGreSQL Database")
@click.option("--dbuser",  default="ufg",                              help="PostGreSQL User")
@click.option("--dbpass",  default="ufgufg",                           help="PostGreSQL Password")
@click.option("--output",  default="output.csv",                       help="Output file")
def main(avl, line, stops, spacing, start, end, headway, db, dbuser, dbpass, output):
    # Create DB connection and get a cursor
    dbConnection, dbCursor = connectDB(db, dbuser, dbpass, line, spacing)

    # Parse Bus Stops
    busStops = buildStopsFromFile(stops)

    # Retrieve Raw Headways
    # Raw here means that we are just storing the datetime where a bus passes through the stop
    # We will calculate the headway (the difference between such occurrences) later
    rawHeadway = processAVL(avl, line, spacing, start, end, busStops, dbCursor)

    # Now, lets derive the Headway data for every bus stop from raw headway
    processedHeadway = deriveHeadway(rawHeadway)

    # Output processed headway
    writeOutput(processedHeadway, output)

    # Output some statistics
    print("MEAN", "MIN", "MAX", "STDEV")
    for busStopID in sorted(processedHeadway.keys()):
        headwayAtStop = processedHeadway[busStopID]
        cvh = np.std(headwayAtStop - headway) / headway
        media = np.mean(headwayAtStop)
        min = np.min(headwayAtStop)
        max = np.max(headwayAtStop)
        print(busStopID, cvh, np.std(headwayAtStop), media, media/60, min, min/60, max, max/60)

    print("FINISHED PROCESSING")

if __name__ == "__main__":
    main()
