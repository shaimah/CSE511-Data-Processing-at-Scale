#!/usr/bin/python2.7
#
# Assignment2 Interface
#

#!/usr/bin/python2.7
#
# Assignment1 Library
#

import psycopg2
import os
import sys

header = [['PartitionName','UserID','MovieID','Rating']]



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	
	cur = openconnection.cursor()
		
	cur.execute("select PartitionNum from RangeRatingsMetadata where MinRating >= %d  order by PartitionNum asc" %(ratingMinValue))

	records = cur.fetchmany(1)
	for row in records:
		N1 = row[0]	
	cur.execute("select PartitionNum from RangeRatingsMetadata where MaxRating >= {0} order by PartitionNum desc".format(ratingMaxValue))
	records = cur.fetchmany(1)

	N2 = findpart(records)



		
	cur.execute("select * from RangeRatingsMetadata")
	records = cur.fetchall()

	cur.execute("select * from RoundRobinRatingsMetadata")
	records = cur.fetchall()
	for row in records:
		N3 = row[0]

	#finding the number of partitions
	cur.execute("drop table if exists frange3")
	cur.execute("create table frange3 (PartitionName text,UserID int, MovieID int, Rating float)")
	i = 0
	while i < N3:
		cur.execute("drop table if exists name")
		cur.execute("create table name (PartitionName text)")
		cur.execute("insert into name(PartitionName) values ('RoundRobinRatingsPart%s')"%(i))
		cur.execute("insert into frange3(PartitionName, UserID, MovieID, Rating) select name.*, RoundRobinRatingsPart{0}.UserID, RoundRobinRatingsPart{0}.MovieID, RoundRobinRatingsPart{0}.Rating from name,RoundRobinRatingsPart{0} where RoundRobinRatingsPart{0}.Rating >= {1} and RoundRobinRatingsPart{0}.Rating <= {2}".format(i,ratingMinValue, ratingMaxValue)) 

 		i += 1
	
	
	if N1 == N2:
		cur.execute("drop table if exists frange")		
		cur.execute("create table frange (PartitionName text)")
		cur.execute ("insert into frange values('RangeRatingsPart%s')" %(N1))
		
		cur.execute("select frange.*,RangeRatingsPart{0}.UserID, RangeRatingsPart{0}.MovieID,RangeRatingsPart{0}.Rating from frange, RangeRatingsPart{0} where RangeRatingsPart{0}.Rating >= {1} and RangeRatingsPart{0}.Rating <= {2} union select * from frange3".format(N1,ratingMinValue,ratingMaxValue))
		frecords = cur.fetchall()

	
	else:
		
		cur.execute("drop table if exists frange2")
		cur.execute("create table frange2 (PartitionName text,UserID int, MovieID int, Rating float)")
		
		i = 0
		while i <= N2:

			cur.execute("drop table if exists name")
			cur.execute("create table name (PartitionName text)")
	
			cur.execute("insert into name(PartitionName) values ('RangeRatingsPart%s')"%(i))
			cur.execute("insert into frange2(PartitionName, UserID, MovieID, Rating) select name.*, RangeRatingsPart{0}.UserID, RangeRatingsPart{0}.MovieID, RangeRatingsPart{0}.Rating from name,RangeRatingsPart{0} where RangeRatingsPart{0}.Rating >= {1} and RangeRatingsPart{0}.Rating <= {2}".format(i,ratingMinValue, ratingMaxValue)) 

			i += 1
	

		cur.execute("select * from frange2 union select * from frange3")


		frecords = cur.fetchall()
	

	writeToFile('RangeQueryOut.txt', frecords)

def PointQuery(ratingsTableName, ratingValue, openconnection):
	cur = openconnection.cursor()
	
	#counting partitions
	cur.execute("select * from RoundRobinRatingsMetadata")
	records = cur.fetchall()
	N = findpart(records)

	cur.execute("select PartitionNum from RangeRatingsMetadata where MaxRating >= {0} and MinRating <= {0}".format(float(ratingValue)))
	records = cur.fetchmany(1)
	N4 = findpart(records)


	cur.execute("drop table if exists name")
	cur.execute("create table name (PartitionName text)")
	cur.execute("insert into name(PartitionName) values ('RangeRatingsPart%s')"%N4)
	cur.execute("drop table if exists prange1")
	cur.execute("create table prange1 (PartitionName text,UserID int, MovieID int, Rating float)")
	
	cur.execute("insert into prange1(PartitionName, UserID, MovieID, Rating) select name.*, RangeRatingsPart{0}.UserID, RangeRatingsPart{0}.MovieID, RangeRatingsPart{0}.Rating from name,RangeRatingsPart{0} where RangeRatingsPart{0}.Rating = {1}".format(N4,float(ratingValue))) 
	
	
	

	#RoundRobin
	

	
	cur.execute("drop table if exists prange2")
	cur.execute("create table prange2 (PartitionName text,UserID int, MovieID int, Rating float)")
	
	i = 0
	while i < N:
		cur.execute("drop table if exists name")
		cur.execute("create table name (PartitionName text)")
		cur.execute("insert into name(PartitionName) values ('RoundRobinRatingsPart%s')"%(i))
		cur.execute("insert into prange2(PartitionName, UserID, MovieID, Rating) select name.*, RoundRobinRatingsPart{0}.UserID, RoundRobinRatingsPart{0}.MovieID, RoundRobinRatingsPart{0}.Rating from name,RoundRobinRatingsPart{0} where RoundRobinRatingsPart{0}.Rating = {1}".format(i,float(ratingValue))) 
 		i += 1
	

	cur.execute("select * from prange1 union select * from prange2")

	frecords = cur.fetchall()
	

	writeToFile('PointQueryOut.txt', frecords)





def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()

def findpart(records):
	for row in records:
		return row[0]
		
	
