import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt

plt.style.use('ggplot')
conn = sqlite3.connect('vatsim.db')

def commonAirport(flights, airports):
	#gets the most common airport pair

	airportpairs = flights[(flights['planned_depairport'] != '') & (flights['planned_destairport'] != '')]

	x = airportpairs.apply(airportPair, axis=1).mode()
	a = airports[(airports['icao'] == x[0][0])]
	b = airports[(airports['icao'] == x[0][1])]

	print 'The most common airport pair is: ' + a['name'][int(a.index[0])] + ', ' + b['name'][int(b.index[0])]



def avgGroundTime(flights):
	#gets the average ground time for all airports

	groundtime = flights[(flights['outRamp'].notnull()) & (flights['offGround'].notnull())]
	x = groundtime.apply(groundDifference, axis=1).mean()
	print "Average ground time: " + str(x)


def miseryMap(airports, flights):
	groundtime = flights[(flights['groundTime'].notnull()) & (flights['planned_depairport'] != "") & (flights['groundTime'] < 7200)]

	#calculate the average ground times for each airport and count how many flights for each airport at the same time
	x = groundtime.groupby('planned_depairport').agg({'groundTime': np.mean, 'planned_depairport': np.size})
	coor = airports[['icao', 'lat','lon']]
	misery = pd.merge(coor, x, left_on = 'icao', right_index = 'planned_depairport')


	airports['z'] = pd.Series(0, index=airports.index)

	maxNum = max(x['planned_depairport'])

	#make a logarithmic ratio so that the smallest airports show up and the most popular are always the same size
	size = 250.0
	ratio = maxNum/size
	ratio2 = size/np.log(size)

	cm = plt.cm.get_cmap('coolwarm')
	cmx = plt.cm.get_cmap('Greys')
	#print airports
	
	ax = airports.plot(kind='scatter',x='lon',y='lat', c='z', cmap = cmx, alpha = .5, lw=0, colorbar=False)
	ax2 = misery.plot(kind='scatter',x='lon',y='lat', c = 'groundTime', cmap=cm, s = np.log(misery['planned_depairport']/ratio) * ratio2, colorbar = False, alpha = .85, vmin = 0, vmax = 3600, ax=ax)
	

	misery = misery.sort_values(by='groundTime', ascending = 0)
	
	
	for i in range(0, 25):
		ax2.annotate(misery.iloc[i,0], (misery.iloc[i,2], misery.iloc[i,1]))
	

def miseryGraph(flights):

	#green are flights with good ground time. red are flights with bad
	#x and y aggs are there just so theyre dataframes instead of series
	green = flights[(flights['groundTime'].notnull()) & (flights['planned_depairport'] != "") & (flights['groundTime'] < 3000)]
	x = green.groupby('just_date').agg({'cid': np.size})
	red = flights[(flights['groundTime'].notnull()) & (flights['planned_depairport'] != "") & (flights['groundTime'] > 3000) & (flights['groundTime'] < 7200)]
	y = red.groupby('just_date').agg({'cid': np.size})

	misery = pd.concat([x,y],axis=1)

	my_colors = ['b', 'r']
	ax3 = misery.plot(kind = 'area', legend = False, color = my_colors)
	ax3.set_xlabel("Date")
	ax3.set_ylabel("# of Flights")
	plt.show()

#*******************************************#

def airportPair(x): #Helper function for commonAirport
	if (x[5] < x[7]):
		return (x[5], x[7])
	else:
		return (x[7], x[5])

def groundDifference(x): #Helper function for avgGroundTime
	return pd.to_datetime(x[19]) - pd.to_datetime(x[18])

def main(conn):	
	flights = pd.read_sql_query("SELECT * from Flights", conn)
	activeflights = pd.read_sql_query("SELECT * from ActiveFlights", conn)
	airports = pd.read_sql_query("SELECT * from Airports", conn)

	miseryMap(airports, flights)
	miseryGraph(flights)

	#other functions we made 
	commonAirport(flights, airports)

main(conn)
