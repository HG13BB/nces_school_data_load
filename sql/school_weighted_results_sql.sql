SELECT 
 ccd.ST
,tst.schl_yr
,tst.dem_grp
,tst.testtype
,sum(tst.numvalid) AS numstudents
,sum(tst.numvalid * tst.pctprof)  AS pctprof_weight
,sum(tst.numvalid * tst.pctprof) / sum(tst.numvalid) AS pctprof_weighted
FROM school_statistics.school_ccd_data_18 ccd

INNER JOIN school_statistics.test_score_history tst
	ON tst.NCESSCH = ccd.NCESSCH

WHERE 1=1
#AND ccd.ST = 'NY'
#AND ccd.SCH_NAME LIKE '%Sarah Anderson%'
AND tst.dem_grp = 'ALL'
AND tst.gradelevel = '00'
#AND tst.schl_yr = '1819'
#AND tst.testtype = 'MTH'
#ORDER BY tst.schl_yr, tst.gradelevel

GROUP BY 
 ccd.ST
,tst.schl_yr
,tst.dem_grp
,tst.testtype


ORDER BY 
 ccd.ST
,tst.schl_yr
,tst.testtype 


