# -*- coding: utf8 -*-
import pyodbc
import sys

### 10.8.5.90 or 10.8.3.90
# con = pyodbc.connect('Driver={SQL Server};Server=10.8.2.90;Database=CareerPages_Dev;uid=automation;pwd=automation')
# con = pyodbc.connect('Driver={SQL Server};Server=autodb;Database=CareerPages;uid=automation;pwd=automation')
con = pyodbc.connect('Driver={SQL Server};Server=10.8.196.65;Database=CareerPages;uid=automation;pwd=JHsUJ<97$jgn')

print("connected")

# INSERT SOME VALUES INTO tableName
def insertRow(tableName, colsTuple, valuesTuple):
    with con:
        cur = con.cursor()
        SQLCommand = "INSERT INTO {} ({}) VALUES ({})".format(tableName, colsTuple, valuesTuple)
        print(SQLCommand)
        res = cur.execute(SQLCommand)
        return res.rowcount


# RETRIEVE TABLE ROWS
def retrieveTable(tableName, param, value):
    with con:
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM {} where {} = '{}'".format(tableName, param, value))
        res = cur.fetchall()
        return res


# DELETE ROW
def deleteRow(tableName, param, value):
    with con:
        cur = con.cursor()
        cur.execute("DELETE FROM {} WHERE {} = '{}'".format(tableName, param, value))
        print("Number of rows deleted:", cur.rowcount)
        return cur.rowcount


# SET UP THE CONNECTION
def retrieveTableColumn(tableName, columnName):
    with con:
        cur = con.cursor()
        cur.execute("SELECT {} FROM {} ".format(columnName, tableName))
        res = cur.fetchall()
        return res


# SET UP THE CONNECTION
def updateTableColumn(tableName, columnName, val, wherecolumn, whereval):
    with con:
        cur = con.cursor()
        qry = "update {} set {} = '{}'' where {}= '{}'".format(tableName, columnName, val, wherecolumn, whereval)
        print(qry)
        cur.execute("update {} set {} = '{}' where {}= '{}'".format(tableName, columnName, val, wherecolumn, whereval))
        return cur.rowcount


def insertRows(query):
    with con:
        cur = con.cursor()
        cur.execute(query)
        return cur.rowcount


def retrieveTabledata(selquery):
    with con:
        cur = con.cursor()
        cur.execute(selquery)
        res = cur.fetchall()
        return res


def updateRows(query):
    with con:
        cur = con.cursor()
        cur.execute(query)
        return cur.rowcount



