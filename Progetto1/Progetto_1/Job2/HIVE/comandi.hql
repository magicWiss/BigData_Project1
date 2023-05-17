
#Creazione della tabella iniziale
Colonne tabella originale->Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text,TimeCreation

Id->integer
ProductId->string
UserId->string
ProfileName->string
HelpfulnessNumerator->float
HelpfulnessDenominator->float
Score->float
time->int
Summary->string
Text->string
TimeCreation->string

->ISTRUZIONE
CREATE TABLE reviews (Id INTEGER,ProductId STRING,UserId STRING,ProfileName STRING,HelpfulnessNumerator FLOAT,HelpfulnessDenominator FLOAT,Score FLOAT,Time INTEGER,Summary STRING,Text STRING,TimeCreation STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Dataset/Rev_Parsed.csv'
				OVERWRITE INTO TABLE reviews;

CREATE TABLE ClassificaJob2 (UserId FLOAT, Score FLOAT);
Query

INSERT INTO Classjob2 SELECT UserId, COALESCE(SUM(HelpfulnessNumerator/HelpfulnessDenominator),0)/COUNT(*) AS avg_ratio FROM (SELECT UserId, HelpfulnessNumerator, CASE WHEN HelpfulnessDenominator=0 THEN 0 ELSE HelpfulnessDenominator END AS HelpfulnessDenominator FROM reviews) subquery GROUP BY UserId ORDER BY avg_ratio DESC;

INSERT OVERWRITE LOCAL DIRECTORY '/home/matteowissel/Universita/Bigdata/Progetto1_git/BigData_Project1/Progetto1/Progetto_1/Job2/HIVE/output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM your_table;