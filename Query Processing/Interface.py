#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()
    #count the no of range tale partitions
    cursor.execute("select count(*) from rangeratingsmetadata")
    range_tables = cursor.fetchone()[0]
    delta_value = 5.0 / range_tables  # gives the delta value for each partition
    table_index = 0
    start_value = 0
    end_value = delta_value
    while (range_tables > 0):
        if not((start_value > ratingMaxValue) or (end_value < ratingMinValue)):
            cursor.execute('select userid, movieid, rating from rangeratingspart' + str(table_index)
                           + ' where rating >= ' + str(ratingMinValue) + ' and rating<= ' + str(ratingMaxValue)+';')
            rows = cursor.fetchall()
            with open('RangeQueryOut.txt', 'a') as f:
                for row in rows:
                    tup = ('RangeRatingsPart'+str(table_index) ,row[0],row[1], row[2] )
                    writeToFile('RangeQueryOut.txt',[tup])
        table_index = table_index + 1
        start_value = start_value + delta_value
        end_value = end_value + delta_value
        range_tables = range_tables - 1

    #roundrobin partition tables
    cursor.execute("select partitionnum from roundrobinratingsmetadata")
    round_tables = cursor.fetchone()[0]
    for i in range(0,round_tables):
        cursor.execute('select userid,movieid, rating from roundrobinratingspart' + str(i) + ' where rating >= ' + str(ratingMinValue) +
                                    ' and rating<= ' + str(ratingMaxValue))
        rows = cursor.fetchall()
        with open('RangeQueryOut.txt', 'a') as f:
            for row in rows:
                tup = ('RoundRobinRatingsPart' + str(i), row[0], row[1], row[2])
                writeToFile('RangeQueryOut.txt', [tup])


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("select count(*) from rangeratingsmetadata")
    range_tables = cursor.fetchone()[0]
    delta_value = 5.0 / range_tables  # gives the delta value for each partition
    table_index = 0
    start_value = 0
    end_value = delta_value
    while (range_tables > 0):
        if start_value == 0:
            if (table_index == 0 and (start_value <= ratingValue and end_value >= ratingValue)):
                cursor.execute('select userid,movieid, rating from rangeratingspart' + str(table_index) + ' where rating = ' + str(ratingValue)+';')
                rows = cursor.fetchall()
                with open('PointQueryOut.txt', 'a') as f:
                    for row in rows:
                        tup = ('RangeRatingsPart' + str(table_index), row[0], row[1], row[2])
                        writeToFile('PointQueryOut.txt', [tup])

                break
        else:
            if (table_index != 0 and (start_value < ratingValue and end_value >= ratingValue)):
                cursor.execute('select userid,movieid, rating from rangeratingspart' + str(table_index) + ' where rating = ' + str(ratingValue)+';')
                rows = cursor.fetchall()
                with open('PointQueryOut.txt', 'a') as f:
                    for row in rows:
                        tup = ('RangeRatingsPart' + str(table_index), row[0], row[1], row[2])
                        writeToFile('PointQueryOut.txt', [tup])
                break
        table_index = table_index + 1
        start_value = start_value + delta_value
        end_value = end_value + delta_value
        range_tables = range_tables - 1
    cursor.execute("select partitionnum from roundrobinratingsmetadata")
    round_tables = cursor.fetchone()[0]
    for i in range(0, round_tables):
        cursor.execute('select userid,movieid, rating from roundrobinratingspart' + str(i) + ' where rating = ' + str(ratingValue)+';')
        rows = cursor.fetchall()
        with open('PointQueryOut.txt', 'a') as f:
            for row in rows:
                tup = ('RoundRobinRatingsPart' + str(i), row[0], row[1], row[2])
                writeToFile('PointQueryOut.txt', [tup])

def writeToFile(filename, rows):
    f = open(filename, 'a+')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()