#!/usr/bin/python
#  1. Unique Flight ID                 	# Unique per record Not significant
#  2. Year			  	# Irrelevant
#  3. Month
#  4. Day of Month (1-31)
#  5. Day of Week (1-7) 
#  6. Scheduled Departure Time (HHMM)
#  7. Scheduled Airline time (HHMM)     # Irrelevant
#  8. Airline
#  9. Flight Number			# Not Significant
# 10. Tail number			
# 11. Plane Model           	
# 12. Seat Configuration		
# 13. Departure Delay			# Irrelevant
# 14. Origin airport
# 15. Destination Airport
# 16. Distance travelled (miles)        
# 17. Taxi time (minutes) 		# Not in scheduled
# 18. Taxi time out (minutes) 		# Not in scheduled
# 19. Whether the flight was cancelled 	# Not in scheduled
# 20. Cancellation code 		# Not in scheduled

# 
# This file takes in the future scheduled flights, and formats a file to be used by Spark for predictions. 
from __future__ import division
import sys
import os
sys.path.append('.')
# use some dictionaries previously created
import airport_dict as ac
import airline_dict as alc
import seat_config_dict as scd
import plane_dict as pd

airport = os.environ["AIRPORT"]
# SVM format file:
# feature 1   Scheduled departure hour
# feature 2   Month
# feature 3   Day of Month
# feature 4   Day of Week
# feature 5   Airline (from Dictionary)
# feature 6   Destination airport (from Dictionary)
# feature 7   Type of plane
# feature 8   Seat Configuration
# feature 9   Distance
for data in sys.stdin:
	(flightID,year,month,dom,dow,sched_dep_time,sched_arr_time,airline,flight_no,tail_no,plane_model,seat_configuration,dep_delay,origin,dest,distance,taxi_time_in,taxi_time_out,cancelled,cancelled_code) = data.split(',')
	dep_hour = int(sched_dep_time)/100    # Convert scheduled departure time to a float

	if origin == airport:
		print '%s 1:%f 2:%f 3:%f 4:%f 5:%f 6:%f 7:%f 8:%f 9:%f' %  \
                	(flightID \
                	,dep_hour \
                	,int(month) \
                	,int(dom) \
                	,int(dow) \
                	,alc.airline_dict[airline] \
                	,ac.airport_dict[dest]  \
                	,pd.plane_dict[plane_model] \
                	,scd.seat_dict[seat_configuration] \
                	,float(distance)
                	)


