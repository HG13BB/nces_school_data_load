#get weighted avg proficiency results by state for all demographic groups

SELECT 
 ccd.ST
,tst.dem_grp
,tst.schl_yr
,tst.testtype
,sum(tst.numvalid) AS numstudents
,sum(tst.numvalid * tst.pctprof)  AS pctprof_weight
,sum(tst.numvalid * tst.pctprof) / sum(tst.numvalid) AS pctprof_weighted
FROM school_statistics.school_ccd_data_18 ccd

INNER JOIN school_statistics.test_score_history tst
	ON tst.NCESSCH = ccd.NCESSCH

WHERE 1=1
AND tst.gradelevel = '00'
AND tst.numvalid > 0
#AND tst.dem_grp = 'ALL'
#AND ccd.ST = 'NY'
#AND ccd.SCH_NAME LIKE '%Sarah Anderson%'
#AND tst.schl_yr = '1819'
#AND tst.testtype = 'MTH'
#ORDER BY tst.schl_yr, tst.gradelevel

GROUP BY 
 ccd.ST
,tst.dem_grp
,tst.schl_yr
,tst.testtype


ORDER BY 
 tst.testtype
,ccd.ST
,tst.schl_yr
,tst.dem_grp

