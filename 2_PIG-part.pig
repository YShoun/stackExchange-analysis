/*
 * We extract the data from the 4 CSVs files generated through Stack Exchange query page and put them into HDFS.
 */
hadoop fs -put /home/yong_pho2/assignment1/0QueryResults.csv /assignment1/0QueryResults.csv
hadoop fs -put /home/yong_pho2/assignment1/1QueryResults.csv /assignment1/1QueryResults.csv
hadoop fs -put /home/yong_pho2/assignment1/2QueryResults.csv /assignment1/2QueryResults.csv
hadoop fs -put /home/yong_pho2/assignment1/3QueryResults.csv /assignment1/3QueryResults.csv

/*
 * Extract
 */
REGISTER '/usr/lib/pig/piggybank.jar';
/* Using piggybank library, we extract each CSVs correctly by setting up multiline support.
 * This is used to counter spearator problems because of the body column of the data : YES_MULTILINE
 * Piggybank is also used to not read the first line of the file known as the header: SKIP_INPUT_HEADER
 * We set up at the same time the schema for each csv */
data0  = LOAD '/assignment1/0QueryResults.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
Id: INT,
PostTypeId: INT,
AcceptedAnswerId: INT,
ParentId: INT,
CreationDate: CHARARRAY,
DeletionDate: CHARARRAY,
Score: INT,
ViewCount: INT,
Body: CHARARRAY,
OwnerUserId: INT,
OwnerDisplayName: CHARARRAY,
LastEditorUserId: INT,
LastEditorDisplayName: CHARARRAY,
LastEditDate: CHARARRAY,
LastActivityDate: CHARARRAY,
Title: CHARARRAY,
Tags: CHARARRAY,
AnswerCount: INT,
CommentCount: INT,
FavoriteCount: INT,
ClosedDate: CHARARRAY,
CommunityOwnedDate: CHARARRAY);

data1  = LOAD '/assignment1/1QueryResults.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
Id: INT,
PostTypeId: INT,
AcceptedAnswerId: INT,
ParentId: INT,
CreationDate: CHARARRAY,
DeletionDate: CHARARRAY,
Score: INT,
ViewCount: INT,
Body: CHARARRAY,
OwnerUserId: INT,
OwnerDisplayName: CHARARRAY,
LastEditorUserId: INT,
LastEditorDisplayName: CHARARRAY,
LastEditDate: CHARARRAY,
LastActivityDate: CHARARRAY,
Title: CHARARRAY,
Tags: CHARARRAY,
AnswerCount: INT,
CommentCount: INT,
FavoriteCount: INT,
ClosedDate: CHARARRAY,
CommunityOwnedDate: CHARARRAY);

data2  = LOAD '/assignment1/2QueryResults.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
Id: INT,
PostTypeId: INT,
AcceptedAnswerId: INT,
ParentId: INT,
CreationDate: CHARARRAY,
DeletionDate: CHARARRAY,
Score: INT,
ViewCount: INT,
Body: CHARARRAY,
OwnerUserId: INT,
OwnerDisplayName: CHARARRAY,
LastEditorUserId: INT,
LastEditorDisplayName: CHARARRAY,
LastEditDate: CHARARRAY,
LastActivityDate: CHARARRAY,
Title: CHARARRAY,
Tags: CHARARRAY,
AnswerCount: INT,
CommentCount: INT,
FavoriteCount: INT,
ClosedDate: CHARARRAY,
CommunityOwnedDate: CHARARRAY);

data3  = LOAD '/assignment1/3QueryResults.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
Id: INT,
PostTypeId: INT,
AcceptedAnswerId: INT,
ParentId: INT,
CreationDate: CHARARRAY,
DeletionDate: CHARARRAY,
Score: INT,
ViewCount: INT,
Body: CHARARRAY,
OwnerUserId: INT,
OwnerDisplayName: CHARARRAY,
LastEditorUserId: INT,
LastEditorDisplayName: CHARARRAY,
LastEditDate: CHARARRAY,
LastActivityDate: CHARARRAY,
Title: CHARARRAY,
Tags: CHARARRAY,
AnswerCount: INT,
CommentCount: INT,
FavoriteCount: INT,
ClosedDate: CHARARRAY,
CommunityOwnedDate: CHARARRAY);

/*
* Transform
*/
-- We use UNION function to merge the 4 csvs into one table
data = UNION data0,data1,data2,data3;

-- check the data by creating a sample and describing it
data_sample = LIMIT data 2;
DUMP data_sample;
DESCRIBE data;

-- checking the number of rows is 200000
count_rows = FOREACH (GROUP data ALL) GENERATE COUNT(data);
DUMP count_rows;

-- Check if all the id rows are distinct by its id.
-- The result shows us that 199998 thas distinct id
data_id = FOREACH data GENERATE Id;
distinct_id = DISTINCT data_id;
distinct_id_group = GROUP distinct_id ALL;
distinct_id_count = FOREACH distinct_id_group  GENERATE COUNT(distinct_id);
-- DUMP shows 199998 rows has unique Id
DUMP distinct_id_count;

-- We remove the duplicate Id
-- We group data by Id and create a nested foreach that will order the data by Id and only keep 1 rows
data1 = FOREACH (GROUP data BY Id) {
  ordening = ORDER data BY Id;
  limit_rows = LIMIT ordening 1;
  GENERATE FLATTEN(limit_rows);
};

-- We check that we have now 199998 rows
count_rows1 = FOREACH (GROUP data1 ALL) GENERATE COUNT(data1);
DUMP count_rows1;

-- Clean useless columns. We keep 12 columns that can be useful for our HIVE part.
data_rm_col = FOREACH data1 GENERATE Id, PostTypeId, AcceptedAnswerId, Score, ViewCount, Body, OwnerUserId, Title, Tags, AnswerCount, CommentCount, FavoriteCount;
-- We check everything has been done correctly by looking at a sample of the result
DESCRIBE data_rm_col;
data_rm_col_sample = LIMIT data_rm_col 3;
DUMP data_rm_col_sample;

-- Remove special character on body, tag and Title by using REPLACE function on the specific column.
-- Replace function will use regex coding for removing undesired character.
-- On the columns 'Body', 'Title' and 'Tag', we replace everything with whitespace except characters and single space
data_rm_special = FOREACH data_rm_col GENERATE Id, PostTypeId, AcceptedAnswerId, Score, ViewCount, REPLACE(Body,'([^a-zA-Z0-9 \\s]+)',' '), OwnerUserId, REPLACE(Title,'([^a-zA-Z0-9 \\s]+)',' '), REPLACE(Tags,'([^a-zA-Z0-9 \\s]+)',' '), AnswerCount, CommentCount, FavoriteCount;
-- We check if everything is working well by checking a sample
data_rm_special_sample = LIMIT data_rm_special 3;
DUMP data_rm_special_sample;

-- Remove multiline on Body
-- To avoid further complication on data loading and reading, we replace multispace generated by the multiline into single whitespace in the column 'Body'
data_rm_multiline = FOREACH data_rm_special GENERATE Id, PostTypeId, AcceptedAnswerId, Score, ViewCount,REPLACE($5,'(\\s+)',' '), OwnerUserId, $7, $8, AnswerCount, CommentCount, FavoriteCount;
data_rm_multiline_sample = LIMIT data_rm_multiline 3;
DUMP data_rm_multiline_sample;

-- decapitalize the data in title and tags columns for next questions on recognizing hadoop word
data_lower_case = FOREACH data_rm_multiline GENERATE Id, PostTypeId, AcceptedAnswerId, Score, ViewCount, LOWER($5), OwnerUserId, LOWER($7), LOWER($8), AnswerCount, CommentCount, FavoriteCount;
-- We check everything has been done by DUMP a sample
data_lower_case_sample = LIMIT data_lower_case 3;
DUMP data_lower_case_sample;

-- Check if data still has same number of column
count_rows = FOREACH (GROUP data_lower_case ALL) GENERATE COUNT(data_lower_case);
DUMP count_rows;

/*
* Load
*/
-- Export latest modified data into a file that can be read by hive
STORE data_lower_case INTO '/assignment1/pig_output1' USING PigStorage (',');
