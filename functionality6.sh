# Functionality-6

hive -e "SELECT U.gPlusUserId USER_ID, R.REVIEWTEXT, U.JOBS FROM
		ANALYSES.USERS U JOIN ANALYSES.REVIEWS R ON U.gPlusUserId = R.gPlusUserId
		WHERE U.JOBS = 'IT Specialist'"
