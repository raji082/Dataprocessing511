#!/usr/bin/python2.7
#
# Interface for the assignement
#
import csv
import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    #drop the table ratingstablename if it already exists
    cursor.execute('DROP TABLE IF EXISTS %s' %(ratingstablename))
    #create the table ratingstablename
    cursor.execute('CREATE TABLE %s (Userid int not null,temp1 varchar(10), MovieID int not null,temp2 varchar(10), '
                   'Rating float, temp3 varchar(10),timestamp bigint)' %ratingstablename )
    #read the ratingsfilepath and copy the values from the file to the table
    with open(ratingsfilepath, 'r') as f:
        cursor.copy_from(f, ratingstablename, sep=':')
    #alter the table by dropping the columns which have : column values
    cursor.execute('alter table %s drop column temp1' %ratingstablename)
    cursor.execute('alter table %s drop column temp2' % ratingstablename)
    cursor.execute('alter table %s drop column temp3' % ratingstablename)
    #commit all the changes and close the cursor
    openconnection.commit()
    cursor.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    #value at the end of each partition
    valueOfRangePartitions = 5.0/numberofpartitions
    #index value for range_part table
    rangePart = 0
    lower_value = 0
    higher_value= valueOfRangePartitions
    #iterate through the numberofpartitions
    while numberofpartitions > 0:
        #Drop the range_part if it already exists
        cursor.execute('DROP TABLE IF EXISTS range_part%s' %(str(rangePart)))
        #create the range_part with rating>= lower_value and rating<=higher_value if lower_value equals 0
        if lower_value == 0:
            cursor.execute('CREATE TABLE range_part%s as Select * from %s where rating>= %s and rating<= %s'
                            % (str(rangePart),ratingstablename, str(lower_value), str(higher_value)))
        # create the range_part with rating> lower_value and rating<=higher_value if lower_value not equals 0
        else:
            cursor.execute('CREATE TABLE range_part%s as Select * from %s where rating> %s and rating<= %s'
                           % (str(rangePart), ratingstablename, str(lower_value), str(higher_value)))
        #increment the lower_value to point to the next partition
        lower_value = lower_value + valueOfRangePartitions
        #increment the lower_value to point to the next partition
        higher_value = higher_value + valueOfRangePartitions
        #increment the index of range_part to point to the next table
        rangePart = rangePart+1
        #decrement the value of numberofpartitons
        numberofpartitions = numberofpartitions - 1
    # commit all the changes and close the cursor
    openconnection.commit()
    cursor.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    i=0
    #store the value of numberofpartitions in temp
    temp = numberofpartitions
    #iterate through the numberofpartitions
    while numberofpartitions > 0:
        #Drop the rrobin_part if it already exists
        cursor.execute('DROP TABLE IF EXISTS rrobin_part%s' %(str(i)))
        #create the table rrobin_part and insert into the table according to the modulus value
        cursor.execute('CREATE TABLE rrobin_part%s AS select r.userid, r.movieid,r.rating from (select row_number() OVER() as rowid,'
                    ' movieid, userid, rating from %s) as r where mod((r.rowid-1) ,%s )= %s' %(str(i), ratingstablename, str(temp), str(i)))
        #increment the index of range_part to point to the next table
        i = i+1
        # decrement the value of numberofpartitons
        numberofpartitions = numberofpartitions-1
    # commit all the changes and close the cursor
    openconnection.commit()
    cursor.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if(rating < 0 or rating >5 ):
        return
    cursor = openconnection.cursor()
    #First insert the userid, itemid,rating values into ratings table
    cursor.execute("insert into  %s (userid, movieid, rating) values (%s,%s,%s)" %(ratingstablename,str(userid),str(itemid),str(rating)))
    # calculate the value of number of tables which have the prefix rrobin_part and store it in numberofpartitions
    cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like'%rrobin_part%' ")
    numberofpartitions = cursor.fetchone()[0]
    #calculate the rowid of the last inserted record and store it in totalrows
    cursor.execute("select r.rowid from (select row_number() over () as rowid,%s.* from %s) as r where userid= %s and movieid = %s "
                   %(ratingstablename,ratingstablename,str(userid),str(itemid)))
    totalrows = cursor.fetchone()[0]
    #finding the index where we can insert the record and store it in last_index
    last_index = (totalrows - 1) % numberofpartitions
    #inserting the userid, itemid,rating values into rrobin_part of index= last_index
    cursor.execute("INSERT INTO rrobin_part%s (userid, movieid, rating)values (%s,%s,%s) " % (str(last_index), str(userid), str(itemid), str(rating)))
    # commit all the changes and close the cursor
    openconnection.commit()
    cursor.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating < 0 or rating > 5):
        return
    cursor = openconnection.cursor()
    # First insert the userid, itemid,rating values into ratings table
    cursor.execute("insert into %s (userid,movieid,rating) values (%s, %s , %s) " %(ratingstablename, str(userid), str(itemid), str(rating)))
    # calculate the value of number of tables which have the prefix range_part and store it in numberofpartitions
    cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '%range_part%'; ")
    numberofpartitions = cursor.fetchone()[0]
    # value at the end of each partition
    valueOfRangePartitions = 5.0 / numberofpartitions
    lower_value = 0
    higher_value = valueOfRangePartitions
    rangePart = 0
    # iterate through the numberofpartitions
    while numberofpartitions > 0:
        #check if the rating lies between lower_value and higher_value and insert in the index
        if(lower_value == 0):
            if(rangePart ==0 and rating >= lower_value and rating<= higher_value):
                cursor.execute("INSERT INTO range_part%s (userid, movieid, rating)values (%s,%s,%s) " %(str(rangePart), str(userid), str(itemid), str(rating)))
                break
        else:
            if(rangePart!=0 and rating > lower_value and rating<= higher_value):
                cursor.execute("INSERT INTO range_part%s (userid, movieid, rating)values (%s,%s,%s) " % (str(rangePart), str(userid), str(itemid), str(rating)))
                break
                # increment the lower_value to point to the next partition
        lower_value = lower_value + valueOfRangePartitions
            # increment the lower_value to point to the next partition
        higher_value = higher_value + valueOfRangePartitions
            # increment the index of range_part to point to the next table
        rangePart = rangePart + 1
            # decrement the value of numberofpartitons
        numberofpartitions = numberofpartitions - 1
    # commit all the changes and close the cursor
    openconnection.commit()
    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
