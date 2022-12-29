/*============================================================================*/
/*========== STAGING_TITLE_BASICS - DATA PROCESSING ==========================*/
/*============================================================================*/
/* Normalize Genres and Title Types into Reference/Lookup Tables              */

/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/
 
DROP TABLE IF EXISTS Title; 
DROP TABLE IF EXISTS Genre; 
DROP TABLE IF EXISTS Title_Genre; 
DROP TABLE IF EXISTS Title_Type; 
DROP TABLE IF EXISTS TEMP_GENRE;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE Title (
  title_id          INTEGER
, type_id           INTEGER
, primary_title     TEXT
, original_title    TEXT
, is_adult          INTEGER CHECK(is_adult IN (0,1)) -- Have INTEGER act like Boolean data type 
, start_year        INTEGER
, end_year          INTEGER
, run_time_minutes  INTEGER
, PRIMARY KEY(title_id)
, FOREIGN KEY(type_id) REFERENCES Title_Type(type_id)
) 
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Genre
(
  genre_id      INTEGER
, genre_name    TEXT    NOT NULL
, PRIMARY KEY (genre_id)
) 
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Title_Genre
(
  title_id      INTEGER
, genre_id      INTEGER
, PRIMARY KEY(title_id,genre_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
, FOREIGN KEY(genre_id) REFERENCES Genre(genre_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Title_Type
(
  type_id         INTEGER
, type_name       TEXT    NOT NULL
, PRIMARY KEY(type_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

/*========== Genre ===========================================================*/

/* genres - comma delimited string has a maximum of 3 values so no recursive CTE required,
*  some nested substrings will get the job done. Possibly a little more confusing than a recursive CTE
*  but shows a different technique. See Title_Crew or Title_Principals processing for recursive CTE examples. 
*/
CREATE TABLE TEMP_GENRE AS

        SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
        ,       CASE    WHEN instr(genres,',') = 0
                        THEN genres
                        ELSE substring(genres,0,instr(genres,','))
                END AS genre_name
        FROM
                STAGING_TITLE_BASICS
        UNION
        SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
        ,       CASE    WHEN instr(substring(genres,instr(genres,',') + 1),',') = 0
                        THEN substring(genres,instr(genres,',') + 1)
                        ELSE substring(
                                substring(genres,instr(genres,',') + 1),0
                        ,       instr(substring(genres,instr(genres,',') + 1),',')
                        )
                END AS genre_name
        FROM
                STAGING_TITLE_BASICS
        UNION
        SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
        ,       CASE    WHEN instr(substring(genres,instr(genres,',') + 1),',') = 0
                        THEN substring(genres,instr(genres,',') + 1)
                        ELSE substring(
                                substring(genres,instr(genres,',') + 1)
                        ,   instr(substring(genres,instr(genres,',') + 1),',') + 1
                        )
                END AS genre_name
        FROM
                STAGING_TITLE_BASICS
        ORDER BY 1
;

/* Generate genre_id */
INSERT INTO Genre
SELECT  SUM(1) OVER (ORDER BY genre_name ASC) AS genre_id
,       genre_name
FROM    (
        SELECT  DISTINCT(genre_name) as genre_name
        FROM
                TEMP_GENRE
        WHERE
                genre_name != '\N'
        ORDER BY 
                genre_name ASC
        ) AS dt1
;

/* Create index to help next query complete faster, delete after use */
CREATE UNIQUE INDEX IDX_TMP_GENRE__GENRE_NAME ON GENRE(genre_name);

ANALYZE Genre; 

/*========== Title_Genre =====================================================*/
/* Intermediate table */ 

INSERT INTO Title_Genre
SELECT  tg.title_id
,       g.genre_id
FROM
        TEMP_GENRE      tg
        LEFT JOIN
        Genre           g
ON
        tg.genre_name = g.genre_name
WHERE
        tg.genre_name != '\N'
ORDER BY
        tg.title_id ASC
,       g.genre_id ASC
;

ANALYZE Title_Genre; 

/*========== Title_Type ======================================================*/

/* Generate type_id */
INSERT INTO Title_Type
SELECT  SUM(1) OVER (ORDER BY type_name) AS type_id
,       type_name
FROM    (
        SELECT  DISTINCT(title_type) AS type_name
        FROM    
                STAGING_TITLE_BASICS
        ORDER BY
                title_type
        ) AS dt1
;

/* Create index to help next query complete faster, delete after use */
CREATE UNIQUE INDEX IDX_TMP_TITLE_TYPE__TYPE_NAME ON TITLE_TYPE(type_name);

ANALYZE Title_Type;

/*========== Title ===========================================================*/

/* Populate our Title table from STAGING_TITLE_BASICS 
*    - Dropping genres column, we have Title_Genre to reference Title to Genre 
*    - Replacing title_type with type_id 
*    - Replacing pseudo null '\N' with real NULL 
*    - CAST values to INTEGERs (title_id, start_year, end_year & run_time_minutes)
*/
INSERT INTO Title
SELECT  CAST(substring(tb.title_id,3) AS INTEGER) AS title_id
,       tt.type_id
,       tb.primary_title
,       tb.original_title
,       tb.is_adult
,       CAST(tb.start_year AS INTEGER)
,       CAST(CASE WHEN tb.end_year = '\N' THEN NULL ELSE tb.end_year END AS INTEGER)
,       CAST(CASE WHEN tb.run_time_minutes = '\N' THEN NULL ELSE tb.run_time_minutes END AS INTEGER)
FROM
        STAGING_TITLE_BASICS    tb 
        LEFT JOIN
        Title_Type      tt
ON 
        tb.title_type = tt.type_name
ORDER BY
        tb.title_id
,       tt.type_id
;

ANALYZE Title;

/*============================================================================*/
/*========== DROP TEMP AND STAGING DATABASE OBJECTS ==========================*/
/*============================================================================*/

DROP INDEX IF EXISTS IDX_TMP_GENRE__GENRE_NAME;
DROP INDEX IF EXISTS IDX_TMP_TITLE_TYPE__TYPE_NAME;
DROP TABLE IF EXISTS TEMP_GENRE;
-- DROP TABLE IF EXISTS STAGING_TITLE_BASICS;

/*============================================================================*/
/*========== END STAGING_TITLE_BASICS - DATA PROCESSING ======================*/
/*============================================================================*/