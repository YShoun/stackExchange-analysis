-- Hive initialization
-- if one was created before
DROP DATABASE IF EXISTS STACKEXCH CASCADE;

-- Create database STACKEXCH and external table in STACKEXCH called data
-- It has the same column as the file exported from pig
CREATE DATABASE STACKEXCH;
CREATE EXTERNAL TABLE IF NOT EXISTS STACKEXCH.data (
Id INT,
PostTypeId INT,
AcceptedAnswerId INT,
Score INT,
ViewCount INT,
Body VARCHAR(255),
OwnerUserId INT,
Title VARCHAR(255),
Tags VARCHAR(255),
AnswerCount INT,
CommentCount INT,
FavoriteCount INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/assignment1/pig_output1';

-- We allow hive to print the name of the columns on outputs
SET hive.cli.print.header=true;

-- The top 10 posts by score
-- We only output the Id, Tile, and Score columns
SELECT Id, Title, Score
FROM STACKEXCH.data
ORDER BY Score DESC LIMIT 10;

-- The top 10 user by post score without NULL OwnerUserId
-- We only output OwnerUserId and the total_score it has
SELECT OwnerUserId, SUM(Score) AS total_score
FROM STACKEXCH.data
WHERE OwnerUserId IS NOT NULL
GROUP BY OwnerUserId
ORDER BY total_score DESC LIMIT 10;

--The number of distinct users, who used the word “Hadoop” in one of their posts
-- We count for distinct users (which is not null) which have the word 'hadoop' that appeared in Body, Title, or Tag column
SELECT COUNT(DISTINCT OwnerUserId) AS number_of_users
FROM STACKEXCH.data
WHERE OwnerUserId IS NOT NULL AND (Tags LIKE '%hadoop%' OR Body LIKE '%hadoop%' OR Title LIKE '%hadoop%');

-- Using Mapreduce calculate the per user TF IDF (just submit the top 10 terms for each of the top 10 users from Query 3.II)
---- MapReduce style code should be used, various code online can be used, the main target of the task is to let you understand
---- how MapReduce programming model works rather than how to get the results.

-- We are going to use java code to create the code. See java-TFIDF folder.

-- before we run the java code, we create input_dir, tmp_dir but not output tmp_dir
-- we get pig data that has gone through ETL from hdfs to local
hadoop fs -get /assignment1/pig_output1/part-r-00000 /home/yong_pho2/

-- we execute our first jar file that will merge body, title and tag columns for a given user from the top 10
java -jar PhaseOne.jar
-- we put the result into hdfs
hadoop fs -put /home/yong_pho2/user_terms.txt /assignment1/input_dir/
-- we run our second jar file that will do the wordcounting and set up the top 10 words as output. It takes the input directory and the output directory as arguments
hadoop jar CountAndTop.jar /assignment1/input_dir /assignment1/output_dir/
hadoop fs -cat /assignment1/output_dir/part-r-00000

-- we remove the old result so we can process the other user
rm user_terms.txt
hadoop fs -rm -r /assignment1/output_dir/
hadoop fs -rm -r /assignment1/temp_dir/
hadoop fs -mkdir /assignment1/temp_dir
hadoop fs -rm /assignment1/input_dir/user_terms.txt
