CREATE TABLE reviews (Id INTEGER,ProductId STRING,UserId STRING,ProfileName STRING,HelpfulnessNumerator FLOAT,HelpfulnessDenominator FLOAT,Score FLOAT,Tempo INTEGER,Summary STRING,Text STRING,TimeCreation STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '${hiveconf:inputpath}'
				OVERWRITE INTO TABLE reviews;

CREATE TABLE Classjob2 (UserId FLOAT, Score FLOAT);

INSERT INTO Classjob2 SELECT UserId, COALESCE(SUM(HelpfulnessNumerator/HelpfulnessDenominator),0)/COUNT(*) AS avg_ratio FROM (SELECT UserId, HelpfulnessNumerator, CASE WHEN HelpfulnessDenominator=0 THEN 0 ELSE HelpfulnessDenominator END AS HelpfulnessDenominator FROM reviews) subquery GROUP BY UserId ORDER BY avg_ratio DESC;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM your_table;

DROP TABLE reviews;
DROP TABLE Classjob2;