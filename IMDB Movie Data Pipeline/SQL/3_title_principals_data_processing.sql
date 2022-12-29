/*============================================================================*/
/*========== STAGING_TITLE_PRINCIPALS - DATA PROCESSING ======================*/
/*============================================================================*/
/* Duplicate data exists with an ordering number to make it 'unique'
*  429 duplicate rows found 
*
*  Job - this field has some arbitrary text that may or may not be related to what would normally
*  be thought of as a job. Doesn't appear to be very useful information but we'll see how it goes.
*
*  characters - text field formated as double qouted string in square brackets e.g. ["Mario"],
*  can be up to a maximum of 1300+ characters. Comma separated quoted strings for persons who
*  play multiple characters are common e.g. ["Bandit","Shot Passenger","Tenderfoot Dancer"]
*/
/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS Principal_Job;
DROP TABLE IF EXISTS Principal_Category;
DROP TABLE IF EXISTS Principal_Character;
DROP TABLE IF EXISTS Title_Principals;
DROP TABLE IF EXISTS TEMP_STAGING_TITLE_PRINCIPALS;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE Principal_Job
(
  job_id        INTEGER
, job_name      TEXT
, PRIMARY KEY(job_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Principal_Category
(
  category_id         INTEGER
, category_name       TEXT
, PRIMARY KEY(category_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Principal_Character
(
  title_id      INTEGER
, person_id     INTEGER
, character     TEXT
, PRIMARY KEY(title_id,person_id,character)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
, FOREIGN KEY(person_id) REFERENCES Person(person_id)
) 
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Title_Principals
(
  title_id INTEGER
, person_id INTEGER
, category_id INTEGER
, job_id INTEGER
, PRIMARY KEY(title_id,person_id,category_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
, FOREIGN KEY(person_id) REFERENCES Person(person_id)
, FOREIGN KEY(category_id) REFERENCES Principal_Category(category_id)
, FOREIGN KEY(job_id) REFERENCES Principal_Job(job_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE TEMP_STAGING_TITLE_PRINCIPALS
(
  title_id      INTEGER
, ordering      INTEGER
, person_id     INTEGER
, category      TEXT
, job           TEXT
, PRIMARY KEY(title_id,ordering,person_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

/*========== Principal_Category ==============================================*/

/* Generate category id */
INSERT INTO Principal_Category
SELECT  SUM(1) OVER (ORDER BY category_count DESC, category_name ASC) AS category_id
,       category_name AS category_name
FROM    (
        SELECT  category AS category_name
        ,       COUNT(*) AS category_count
        FROM
                STAGING_TITLE_PRINCIPALS
        GROUP BY 
                1
        ORDER BY
                2 DESC
        ) AS dt1
;

ANALYZE Principal_Category;

/*========== Principal_Job ===================================================*/

/* Generate our job_id */
/* This job data is terrible, example:
*       written for the screen by
*       written for the screen
*       written for screen
*       writer: screen
*       the screenplay by
*       the screen play written by
*       etc. etc. 
* these rows are all basically the same thing but are listed as separate 'jobs'
* The source file is full of examples like this, too many to fix for this project. 
*/
INSERT INTO Principal_Job
SELECT  SUM(1) OVER (ORDER BY occurrences DESC, job ASC) AS job_id
,       job AS job_name
FROM    (
        SELECT job
        ,      COUNT(*) AS occurrences
        FROM
                STAGING_TITLE_PRINCIPALS
        WHERE
                job != '\N'
        GROUP BY 
                1
        ORDER BY 
                2 DESC
        ) dt1
;

ANALYZE Principal_Job;

/*========== Principal_Character =============================================*/

/* These next queries need additional resources to finish on a human timescale ;) */
PRAGMA temp_store = 2; -- MEMORY, default = FILE
PRAGMA cache_size = -512000; -- 512MB, default = 2MB

/* Use recursive CTE to split up delimted string of characters and convert to rows
Replace "," with | and split on the pipe symbol to try and preserve commas within strings, approx 180k entries
e.g 'Bridget, the Cook' & 'Tom Ripley, a Cowboy' 
*/
WITH RECURSIVE RCTE_PRINCIPALS_CHARACTERS AS
(
SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
,       CAST(substring(person_id,3) AS INTEGER) AS person_id
,       NULL AS character
,       REPLACE(REPLACE(REPLACE(character,'","','|'),'["',''),'"]','') as characters_remainder
FROM
        STAGING_TITLE_PRINCIPALS
WHERE
        character != '\N'
UNION ALL
SELECT  title_id
,       person_id
,       CASE    WHEN instr(characters_remainder,'|') = 0
                THEN characters_remainder
                ELSE substring(characters_remainder,0,instr(characters_remainder,'|'))
        END AS character
,       CASE    WHEN instr(characters_remainder,'|') = 0
                THEN NULL
                ELSE substring(characters_remainder,instr(characters_remainder,'|')+1)
        END AS characters_remainder
FROM
        RCTE_PRINCIPALS_CHARACTERS
WHERE
        characters_remainder != ''
AND     characters_remainder IS NOT NULL
)
INSERT INTO Principal_Character
SELECT  DISTINCT title_id
,       person_id
,       character
FROM
        RCTE_PRINCIPALS_CHARACTERS
WHERE   
        character IS NOT NULL
ORDER BY
        1,2,3
;

ANALYZE Principal_Character;

/*========== Title_Principals ================================================*/

/* Use our TEMP table to remove rows where title_id,person_id,category and job are the same i.e. duplicates 
It looks like the 'ordering' column has been added to make the rows unique instead of fixing the data integrity issue!
While we are scanning the whole table, CAST all our ids to INTEGER and make our pseudo nulls actually NULL
This is a 53 million row table so lets use some more memory to help it along. See PRAGMA statements. */

INSERT INTO TEMP_STAGING_TITLE_PRINCIPALS
SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
,       MIN(ordering)
,       CAST(substring(person_id,3) AS INTEGER) AS person_id
,       category
,       CASE WHEN job = '\N' THEN NULL ELSE job END AS job
FROM    
        STAGING_TITLE_PRINCIPALS
GROUP BY
        1,3,4,5
;

/* Some temp indexes to assist with the next query */
CREATE INDEX IDX_TMP_PRIN_JOB ON Principal_Job(job_name);
CREATE INDEX IDX_TMP_PRIN_CAT ON Principal_Category(category_name);

PRAGMA temp_store = 2; -- MEMORY, default = FILE
PRAGMA cache_size = -512000; -- 512MB, default = 2MB

INSERT INTO Title_Principals
SELECT  tstp.title_id
,       tstp.person_id
,       pcat.category_id
,       pj.job_id
FROM    TEMP_STAGING_TITLE_PRINCIPALS AS tstp
        LEFT JOIN
        Principal_Job AS pj
ON
        tstp.job = pj.job_name
        LEFT JOIN
        Principal_Category AS pcat
ON
        tstp.category = pcat.category_name
GROUP BY 1,2,3,4 
;

ANALYZE Title_Principals;

/*============================================================================*/
/*========== DROP TEMP AND STAGING DATABASE OBJECTS ==========================*/
/*============================================================================*/

DROP INDEX IF EXISTS IDX_TMP_PRIN_JOB;
DROP INDEX IF EXISTS IDX_TMP_PRIN_CAT;
DROP TABLE IF EXISTS TEMP_STAGING_TITLE_PRINCIPALS;
-- DROP TABLE IF EXISTS STAGING_TITLE_PRINCIPALS;

/*============================================================================*/
/*========== END STAGING_TITLE_PRINCIPALS - DATA PROCESSING ==================*/
/*============================================================================*/