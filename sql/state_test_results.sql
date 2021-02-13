SELECT * 

FROM school_statistics.test_score_history tst
INNER JOIN school_statistics.school_id schid ON schid.NCESSCH = tst.NCESSCH

WHERE schid.STNAM = 'NEW YORK';