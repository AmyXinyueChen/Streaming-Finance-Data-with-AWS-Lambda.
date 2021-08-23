SELECT B.name, B.Hour, A.ts,B.HOUR_HIGHEST
FROM sta9760s2021_project03_s3 A
INNER JOIN

(SELECT name, SUBSTRING(ts,12,2) as Hour, MAX(high) AS HOUR_HIGHEST
FROM sta9760s2021_project03_s3 
GROUP BY name,SUBSTRING(ts,12,2)) B

ON A.name=B.name and A.high=B.HOUR_HIGHEST
ORDER BY B.name, B.Hour
