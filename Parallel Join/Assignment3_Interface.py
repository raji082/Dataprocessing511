#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cursor = openconnection.cursor()
    cursor.execute('select max('+SortingColumnName + ') from ' + InputTable)
    max_value = cursor.fetchone()[0]
    cursor.execute('select min(' + SortingColumnName + ') from ' + InputTable)
    min_value = cursor.fetchone()[0]
    noofThreads = 5
    range_tables = float(max_value - min_value)/noofThreads
    start_value = min_value
    end_value = start_value + range_tables
    parallelthreads = [0, 0, 0, 0, 0]
    for i in range(0, noofThreads):
        parallelthreads[i] = threading.Thread(target=sortIndividualTables, args=(InputTable, SortingColumnName, i, start_value, end_value, 'range_part', openconnection))
        start_value = start_value + range_tables
        end_value = end_value + range_tables
        parallelthreads[i].start()
    cursor.execute("Drop table if exists "+OutputTable)
    cursor.execute("Create table " + OutputTable + "( like " + InputTable + " including all)")
    for i in range(0, noofThreads):
        #print("Thread" + str(i))
        parallelthreads[i].join()
        cursor.execute("insert into " + OutputTable +" select * from range_part"+ str(i))
    # Below 3 lines commented are just for verification
    cursor.execute("copy " + OutputTable + " to '/Users/raji/Documents/DPS/Assignments/ParallelSortandJoin/sort_output.txt';")
    cursor.execute("SELECT * from " + "(SELECT * FROM " + InputTable + ") as st where st.movieid not in (select movieid from "+ OutputTable +");")
    print(cursor.fetchall())

def sortIndividualTables(InputTable, SortingColumnName, table_index, start_value, end_value,table_name,openconnection):
    cursor = openconnection.cursor()
    range_table_name = table_name + str(table_index)
    cursor.execute('Drop table if exists '+ range_table_name)
    if table_index == 0:
        cursor.execute("create table " + range_table_name + " as select * from " + InputTable + " where "+ SortingColumnName +" >= "
                       + str(start_value) + " and " + SortingColumnName + "<=" + str(end_value) +" order by " + SortingColumnName +" asc" )
    else:
        cursor.execute("create table " + range_table_name + " as select * from " + InputTable + " where " + SortingColumnName + " > "
                       + str(start_value) + " and " + SortingColumnName + "<=" + str(end_value) + " order by " + SortingColumnName +" asc" )


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cursor = openconnection.cursor()
    cursor.execute("select max(" + Table1JoinColumn + ") from " + InputTable1)
    max_table1 = cursor.fetchone()[0]
    cursor.execute("select min(" + Table1JoinColumn + ") from " + InputTable1)
    min_table1 = cursor.fetchone()[0]
    cursor.execute("select max(" + Table2JoinColumn + ") from " + InputTable2)
    max_table2 = cursor.fetchone()[0]
    cursor.execute("select min(" + Table2JoinColumn + ") from " + InputTable2)
    min_table2 = cursor.fetchone()[0]
    min_t = min(min_table1, min_table2)
    max_t = max(max_table1, max_table2)
    noofThreads = 5
    range_table = float(max_t - min_t) / noofThreads
    parallel_threads = [0, 0, 0, 0, 0]
    start_value = min_t
    end_value = start_value + range_table
    for i in range(0, noofThreads):
        parallel_threads[i] = threading.Thread(target=joinTables, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,'temp_table1','temp_table2',
                                                                      i, start_value, end_value,'output_table', openconnection))
        start_value = start_value + range_table
        end_value =  end_value + range_table
        parallel_threads[i].start()
    cursor.execute("DROP table if exists " + OutputTable)
    cursor.execute("CREATE TABLE " + OutputTable + " (like " + InputTable1 + " including ALL)")
    cursor.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';")
    tab2 = cursor.fetchall()
    for i in range(len(tab2)):
        cursor.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + tab2[i][0] + " " + tab2[i][1])
    for i in range(0,noofThreads):
        parallel_threads[i].join()
        cursor.execute("insert into " + OutputTable + " select * from output_table" + str(i))
    cursor.execute("copy " + OutputTable + " to '/Users/raji/Documents/DPS/Assignments/ParallelSortandJoin/join_output.txt';")
    cursor.execute("SELECT * from " + "(SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + ") as jt where jt.movieid not in (select movieid from "+ OutputTable +");")
    print(cursor.fetchall())



def joinTables(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,temp1,temp2,
                                                                    table_index, start_value,end_value,output_table,openconnection):
    table1 = temp1 + str(table_index)
    table2 = temp2 + str(table_index)
    output_table = output_table + str(table_index)
    cursor = openconnection.cursor()
    cursor.execute("DROP table if exists " + table1)
    cursor.execute("CREATE table " + table1 + "(like " + InputTable1 + " including all)" )
    cursor.execute("DROP table if exists " + table2)
    cursor.execute("CREATE table " + table2 + "(like " + InputTable2 + " including all)")
    if table_index == 0:
        cursor.execute("insert into " + table1 + " select * from " + InputTable1 + " where " + Table1JoinColumn + ">= " + str(start_value) + " and " +
                           Table1JoinColumn + "<= " +str(end_value))
        cursor.execute("insert into " + table2 + " select * from " + InputTable2 + " where " + Table2JoinColumn + ">= " + str(start_value) + " and " +
                           Table2JoinColumn + "<= " +str(end_value))
    else:
        cursor.execute("insert into " + table1 + " select * from " + InputTable1 + " where " + Table1JoinColumn + "> " + str(start_value) + " and " +
                           Table1JoinColumn + "<= " + str(end_value))
        cursor.execute("insert into " + table2 + " select * from " + InputTable2 + " where " + Table2JoinColumn + "> " + str(start_value) + " and " +
                           Table2JoinColumn + "<= " + str(end_value))

    cursor.execute("DROP table if exists " + output_table)
    cursor.execute("CREATE TABLE " + output_table + " (like " + InputTable1 + " including ALL)")
    cursor.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';")
    tab2 = cursor.fetchall()
    for i in range(len(tab2)):
        cursor.execute("ALTER TABLE " + output_table + " ADD COLUMN " + tab2[i][0] + " " + tab2[i][1])
    cursor.execute("insert into " + output_table + " select * from " + table1 + ' inner join ' + table2 + ' on ' + table1 +'.' +Table1JoinColumn + ' = ' +table2
                   + '.' +Table2JoinColumn)
