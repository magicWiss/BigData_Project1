DROP TABLE reviews;
DROP TABLE reviews_with_year;
DROP TABLE words_product_per_year;
DROP TABLE top_reviewed_products_per_year;
DROP TABLE top_words_per_product_per_year;

CREATE TABLE reviews (Id STRING, ProductId STRING, UserId STRING, ProfileName STRING, HelpfulnessNumerator STRING, HelpfulnessDenominator STRING, Score STRING, Time INT, Summary STRING, Text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'BD/BigData_Project1/Progetto1/Progetto_1/Dataset/Rev_Parsed.csv'
							OVERWRITE INTO TABLE reviews;

CREATE TABLE reviews_with_year AS (
    SELECT ProductId, from_unixtime(Time, 'yyyy') as year, Text
    FROM reviews);

CREATE TABLE words_product_per_year AS (
    SELECT word, ProductId, year
    FROM reviews_with_year
    LATERAL VIEW explode(split(Text, ' ')) words_table as word
    WHERE LENGTH(word) >= 4
);

CREATE TABLE top_reviewed_products_per_year AS (SELECT year, ProductId, cont_prods FROM (
    SELECT year, ProductId, COUNT(ProductId) as cont_prods, ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) AS row_num
    FROM reviews_with_year
    GROUP BY year, ProductId
) x WHERE row_num <= 10);

-- filtro gli id dei prodotti grazie alla tabella creata prima così da evitare salvataggi inutili
CREATE TABLE top_words_per_product_per_year AS (SELECT year, ProductId, word, cont_words FROM (
    SELECT year, ProductId, word, COUNT(word) as cont_words, ROW_NUMBER() OVER (PARTITION BY year, ProductId ORDER BY COUNT(*) DESC) AS row_num
    FROM words_product_per_year
    WHERE ProductId IN (SELECT ProductId FROM top_reviewed_products_per_year)
    GROUP BY year, ProductId, word
) x WHERE row_num <= 5);

SELECT tp.year, tp.ProductId, tw.word, tw.cont_words
FROM top_reviewed_products_per_year tp JOIN top_words_per_product_per_year tw
ON tp.year == tw.year AND tp.ProductId ==tw.ProductId;


DROP TABLE reviews;
DROP TABLE reviews_with_year;
DROP TABLE words_product_per_year;
DROP TABLE top_reviewed_products_per_year;
DROP TABLE top_words_per_product_per_year;