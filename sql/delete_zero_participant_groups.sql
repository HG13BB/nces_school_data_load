#delete test groups with no participants

DELETE 

FROM school_statistics.test_score_history

WHERE metrictype = 'NUMVALID'
AND result_value = '0.0';
 

