/*============================================================================*/
/*========== STAGING_NAME_BASICS - DATA PROCESSING ===========================*/
/*============================================================================*/
/* Normalize Professions and 'Known for' Titles into Reference/Lookup Tables 
*  Person_Profession intermediate table enables one to many relationship */

/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS Person;
DROP TABLE IF EXISTS Profession;
DROP TABLE IF EXISTS Person_Profession;
DROP TABLE IF EXISTS Person_Title_Known_For;
DROP TABLE IF EXISTS TEMP_PERSON_PROFESSION; 
DROP TABLE IF EXISTS TEMP_PERSON_PROFESSION_2;
DROP TABLE IF EXISTS TEMP_PERSON_KNOWN_FOR; 
DROP TABLE IF EXISTS TEMP_PERSON_KNOWN_FOR_2;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE Person 
(
  person_id     INTEGER
, person_name   TEXT    NOT NULL
, birth_year    INTEGER
, death_year    INTEGER
, PRIMARY KEY(person_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Profession 
(
  profession_id     INTEGER
, profession_name   TEXT
, PRIMARY KEY(profession_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Person_Profession
(
  person_id             INTEGER
, profession_id         INTEGER
, profession_rank       INTEGER
, PRIMARY KEY(person_id,profession_id,profession_rank)
, FOREIGN KEY(person_id) REFERENCES Person(person_id)
, FOREIGN KEY(profession_id) REFERENCES Profession(profession_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE Person_Title_Known_For
(
  person_id             INTEGER
, title_id              INTEGER
, PRIMARY KEY(person_id,title_id)
, FOREIGN KEY(person_id) REFERENCES Person(person_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

/*========== Profession ======================================================*/

/* primary_profession, comma delimited string of enumerated values that needs splitting up.
*  Nested derived tables split off the first value in the string and return the remainder to the outer query 
*/

CREATE TABLE IF NOT EXISTS TEMP_PERSON_PROFESSION AS
SELECT  person_id
,       profession_rank_1
,       profession_rank_2
,       CASE    WHEN instr(primary_profession,',') = 0
                THEN primary_profession
                ELSE substring(primary_profession,0,instr(primary_profession,','))
        END as profession_rank_3
FROM    (
        SELECT  person_id
        ,       profession_rank_1
        ,       CASE    WHEN instr(primary_profession,',') = 0
                        THEN primary_profession
                        ELSE substring(primary_profession,0,instr(primary_profession,','))
                END AS profession_rank_2
        ,       CASE    WHEN instr(primary_profession,',') = 0
                        THEN NULL
                        ELSE substring(primary_profession,instr(primary_profession,',')+1) 
                END AS primary_profession
        FROM    (
                SELECT  CAST(substring(person_id,3) AS INTEGER) AS person_id
                ,       CASE    WHEN instr(primary_profession,',') = 0
                                THEN primary_profession
                                ELSE substring(primary_profession,0,instr(primary_profession,','))
                        END AS profession_rank_1
                ,       CASE    WHEN instr(primary_profession,',') = 0
                                THEN NULL
                                ELSE substring(primary_profession,instr(primary_profession,',')+1) 
                        END AS primary_profession
                FROM
                        STAGING_NAME_BASICS
                WHERE
                        primary_profession != '' -- > 50% of the data!
                ORDER BY
                        person_id ASC
                ) AS dt1
        ) AS dt2
;

/* Turn our profession columns into ranked rows */
CREATE TABLE IF NOT EXISTS TEMP_PERSON_PROFESSION_2 AS
SELECT  person_id
,       profession_rank_1 AS profession
,       1 AS profession_rank
FROM
        TEMP_PERSON_PROFESSION
WHERE
        profession_rank_1 IS NOT NULL
UNION
SELECT  person_id
,       profession_rank_2 AS profession
,       2 AS profession_rank
FROM
        TEMP_PERSON_PROFESSION
WHERE
        profession_rank_2 IS NOT NULL
UNION
SELECT  person_id
,       profession_rank_3 AS profession
,       3 AS profession_rank
FROM
        TEMP_PERSON_PROFESSION
WHERE
        profession_rank_3 IS NOT NULL
ORDER BY
        person_id ASC
,       profession_rank ASC
;

/* Generate profession_id */
INSERT INTO Profession
SELECT  SUM(1) OVER (ORDER BY profession_count DESC, profession ASC) AS profession_id
,       profession AS profession_name
FROM    (
        SELECT  profession
        ,       COUNT(*) AS profession_count
        FROM
                TEMP_PERSON_PROFESSION_2
        GROUP BY
                1
        ) AS dt1
;

/* Create index to help the next query, we will delete it when we are done with it */
CREATE UNIQUE INDEX IDX_TMP_PROFESSION__PROFESSION_NAME ON PROFESSION(profession_name);

ANALYZE Profession;

/*========== Person_Profession ===============================================*/
/* intermediate table  */

INSERT INTO Person_Profession
SELECT  tpp2.person_id
,       p.profession_id
,       tpp2.profession_rank
FROM    
        TEMP_PERSON_PROFESSION_2 AS tpp2
        LEFT JOIN
        Profession AS p
ON
        tpp2.profession = p.profession_name
ORDER BY
        tpp2.person_id ASC
,       tpp2.profession_rank ASC
;

ANALYZE Person_Profession;

/*========== Person ==========================================================*/

/* Populate our Person table, 
    - drop column primary_profession we can reference that through Person_Profession -> Profession 
    - drop column known_for_titles, next step we will populate Person_Title_Known_For
      we need to Populate Person first because of the foreign key on Person_Title_Known_For 
*/

INSERT INTO Person
SELECT  CAST(substring(person_id,3) AS INTEGER) AS person_id
,       primary_name AS person_name
,       CASE WHEN birth_year = '\N' THEN NULL ELSE birth_year END AS birth_year
,       CASE WHEN death_year = '\N' THEN NULL ELSE death_year END AS death_year
FROM
        STAGING_NAME_BASICS
ORDER BY
        person_id ASC
;

ANALYZE Person;

/*========== Person_Title_Known_For ==========================================*/

/* known_for_titles, comma delimited string of values that needs splitting up.
*  Nested derived tables split off the first value in the string and return the remainder to the outer query */

CREATE TABLE IF NOT EXISTS TEMP_PERSON_KNOWN_FOR AS
SELECT  person_id
,       title_1
,       title_2
,       title_3
,       CASE    WHEN instr(known_for_titles,',') = 0
                THEN known_for_titles
                ELSE substring(known_for_titles,0,instr(known_for_titles,','))
        END AS title_4
FROM    (
        SELECT  person_id
        ,       title_1
        ,       title_2
        ,       CASE    WHEN instr(known_for_titles,',') = 0
                        THEN known_for_titles
                        ELSE substring(known_for_titles,0,instr(known_for_titles,','))
                END AS title_3
        ,       CASE    WHEN instr(known_for_titles,',') = 0
                        THEN NULL
                        ELSE substring(known_for_titles,instr(known_for_titles,',')+1) 
                END AS known_for_titles 
        FROM    (
                SELECT  person_id
                ,       title_1
                ,       CASE    WHEN instr(known_for_titles,',') = 0
                                THEN known_for_titles
                                ELSE substring(known_for_titles,0,instr(known_for_titles,','))
                        END AS title_2
                ,       CASE    WHEN instr(known_for_titles,',') = 0
                                THEN NULL
                                ELSE substring(known_for_titles,instr(known_for_titles,',')+1) 
                        END AS known_for_titles 
                FROM    (
                        SELECT  CAST(substring(person_id,3) AS INTEGER) AS person_id
                        ,       CASE    WHEN instr(known_for_titles,',') = 0
                                        THEN known_for_titles
                                        ELSE substring(known_for_titles,0,instr(known_for_titles,','))
                                END AS title_1
                        ,       CASE    WHEN instr(known_for_titles,',') = 0
                                        THEN NULL
                                        ELSE substring(known_for_titles,instr(known_for_titles,',')+1) 
                                END AS known_for_titles               
                        FROM
                                STAGING_NAME_BASICS
                        WHERE
                                known_for_titles != '\N' 
                        ) AS dt1
                ) AS dt2
        ) AS dt3
;

/* Turn our title columns into rows */

CREATE TABLE IF NOT EXISTS TEMP_PERSON_KNOWN_FOR_2 AS
SELECT  person_id
,       title_1 AS title_id
FROM
        TEMP_PERSON_KNOWN_FOR
WHERE
        title_1 IS NOT NULL
UNION
SELECT  person_id
,       title_2 AS title_id
FROM
        TEMP_PERSON_KNOWN_FOR
WHERE
        title_2 IS NOT NULL
UNION
SELECT  person_id
,       title_3 AS title_id
FROM
        TEMP_PERSON_KNOWN_FOR
WHERE
        title_3 IS NOT NULL
UNION
SELECT  person_id
,       title_4 AS title_id
FROM
        TEMP_PERSON_KNOWN_FOR
WHERE
        title_4 IS NOT NULL
ORDER BY
        person_id ASC
;

INSERT INTO Person_Title_Known_For
SELECT  person_id
,       CAST(substring(title_id,3) AS INT) AS title_id
FROM
        TEMP_PERSON_KNOWN_FOR_2
ORDER BY
        person_id ASC
;

ANALYZE Person_Title_Known_For;

/*============================================================================*/
/*========== DROP TEMP AND STAGING DATABASE OBJECTS ==========================*/
/*============================================================================*/

DROP INDEX IDX_TMP_PROFESSION__PROFESSION_NAME;
DROP TABLE IF EXISTS TEMP_PERSON_PROFESSION; 
DROP TABLE IF EXISTS TEMP_PERSON_PROFESSION_2;
DROP TABLE IF EXISTS TEMP_PERSON_KNOWN_FOR; 
DROP TABLE IF EXISTS TEMP_PERSON_KNOWN_FOR_2;
-- DROP TABLE IF EXISTS STAGING_NAME_BASICS;

/*============================================================================*/
/*========== END STAGING_NAME_BASICS - DATA PROCESSING =======================*/
/*============================================================================*/