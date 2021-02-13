SELECT 

ccd.SCH_NAME
,ccd.LZIP
,tst.*

FROM school_statistics.school_ccd_data_18 ccd

INNER JOIN school_statistics.test_score_history tst
	ON tst.NCESSCH = ccd.NCESSCH

WHERE ccd.ST = 'NY'
AND ccd.SCH_NAME LIKE '%Sarah Anderson%'
AND tst.dem_grp = 'ALL'
#AND ccd.LZIP = '55906'
AND tst.schl_yr = '1819'

ORDER BY tst.schl_yr, tst.gradelevel;

