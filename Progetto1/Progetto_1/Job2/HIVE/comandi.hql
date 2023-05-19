CREATE TABLE reviews (Id INTEGER,ProductId STRING,UserId STRING,ProfileName STRING,HelpfulnessNumerator FLOAT,HelpfulnessDenominator FLOAT,Score FLOAT,Time INTEGER,Summary STRING,Text STRING,TimeCreatio)

LOAD DATA LOCAL INPATH '/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Dataset/TestDataset/Test_k_01.csv'
                                OVERWRITE INTO TABLE reviews;

CREATE TABLE Classjob2 (UserId FLOAT, Score FLOAT);


INSERT INTO Classjob2 SELECT UserId, COALESCE(SUM(HelpfulnessNumerator/HelpfulnessDenominator),0)/COUNT(*) AS avg_ratio FROM (SELECT UserId, HelpfulnessNumerator, CASE WHEN HelpfulnessDenominator=0 THEN 0 ELSE HelpfulnessDenominator END AS HelpfulnessDenominator FROM reviews) subquery GROUP BY UserId ORDER BY avg_ratio DESC;

DROP TABLE reviews;
DROP TABLE Classjob2;