# Input: Pass the argument as the rating you want
# Argument-1: The ratings count you want

hive -e "SELECT COUNT(*) COUNT FROM ANALYSES.REVIEWS WHERE RATING = $1" 


