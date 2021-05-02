# Functionality-5

hive -e "SELECT * FROM 
		(SELECT gPlusPlaceId, COUNT(*) COUNT FROM ANALYSES.REVIEWS
			GROUP BY gPlusPlaceId) A
		WHERE COUNT > 3"
