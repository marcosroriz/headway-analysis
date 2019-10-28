#!/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import click
import numpy as np
import psycopg2
import matplotlib.pyplot as plt
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

    dbConnection.commit()
    return (dbConnection, dbCursor)


def buildStopsFromFile(stopsFileName, line, dbCursor):
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
        stops[index]["term"] = bool(int(aStop["term"]))
        stops[index]["id"] = index
        stops[index]["lat"] = float(aStop["lat"])
        stops[index]["lng"] = float(aStop["lng"])
        stops[index]["dist"] = int(aStop["dist"])

        travDist = getTravDistance(aStop["lat"], aStop["lng"], line, dbCursor)
        print(aStop["term"], index, float(aStop["lat"]), float(aStop["lng"]), int(aStop["dist"]), int(travDist), sep=",")

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


@click.command()
@click.option("--line",    default=400,                                help="Bus Line")
@click.option("--stops",   default="data/400-pontos.csv",              help="File containing Bus Stops")
@click.option("--spacing", default=0.00025,                            help="Interpolation Spacing")
@click.option("--db",      default="highway",                          help="PostGreSQL Database")
@click.option("--dbuser",  default="ufg",                              help="PostGreSQL User")
@click.option("--dbpass",  default="ufgufg",                           help="PostGreSQL Password")
@click.option("--output",  default="output.csv",                       help="Output file")
def main(line, stops, spacing, db, dbuser, dbpass, output):
    # Create DB connection and get a cursor
    dbConnection, dbCursor = connectDB(db, dbuser, dbpass, line, spacing)

    # Parse Bus Stops
    busStops = buildStopsFromFile(stops, line, dbCursor)

    # Output processed headway
    # writeOutput(busStops, output)


if __name__ == "__main__":
    main()