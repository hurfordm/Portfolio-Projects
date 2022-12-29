/*============================================================================*/
/*========== STAGING_TITLE_CREW - DATA PROCESSING ============================*/
/*============================================================================*/
/*
* Writers and Directors fields contain comma separated values, 
* sometimes hundreds of person_id's for a tv series with many episodes.
*
* 'Recursive Common Table Expression' used to normalize the data so we can actually query it later
*/
/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS Title_Crew;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE Title_Crew
(
  title_id  INTEGER
, person_id INTEGER
, profession_id INTEGER
, PRIMARY KEY(title_id,person_id,profession_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
, FOREIGN KEY(person_id) REFERENCES Person(person_id)
, FOREIGN KEY(profession_id) REFERENCES Profession(profession_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

/*========== Title_Crew ======================================================*/

WITH RECURSIVE RCTE(title_id,writer,writers_remainder,director,directors_remainder) AS
(
SELECT  title_id
,       NULL AS writer
,       writers AS writers_remainder
,       NULL AS director
,       directors AS directors_remainder
FROM
        STAGING_TITLE_CREW
UNION ALL
SELECT  title_id
,       CASE    WHEN instr(writers_remainder,',') > 0
                THEN substring(writers_remainder,0,instr(writers_remainder,',')) 
                ELSE writers_remainder
        END AS writer
,       CASE    WHEN instr(writers_remainder,',') > 0
                THEN substring(writers_remainder,instr(writers_remainder,',')+1) 
                ELSE NULL
        END AS writers_remainder 
,       CASE    WHEN instr(directors_remainder,',') > 0
                THEN substring(directors_remainder,0,instr(directors_remainder,',')) 
                ELSE directors_remainder
        END AS director
,       CASE    WHEN instr(directors_remainder,',') > 0
                THEN substring(directors_remainder,instr(directors_remainder,',')+1) 
                ELSE NULL
        END AS directors_remainder
FROM
        RCTE
WHERE
        writers_remainder != '' AND
        directors_remainder != ''
)
INSERT INTO Title_Crew
SELECT  CAST(substring(cte.title_id,3) AS INTEGER) AS title_id
,       CAST(substring(cte.writer,  3) AS INTEGER) AS person_id
,       p.profession_id
FROM
        RCTE AS cte
        CROSS JOIN
        Profession AS p
WHERE
        p.profession_name = 'writer'
AND     cte.writer IS NOT NULL
AND     cte.writer != '\N'
UNION ALL
SELECT  CAST(substring(cte.title_id,3) AS INTEGER) AS title_id
,       CAST(substring(cte.director,3) AS INTEGER) AS person_id
,       p.profession_id
FROM
        RCTE AS cte
        CROSS JOIN
        Profession AS p
WHERE
        p.profession_name = 'director'
AND     cte.director IS NOT NULL
AND     cte.director != '\N'
ORDER BY
        1 ASC,3 ASC,2 ASC
;

ANALYZE Title_Crew;

/*============================================================================*/
/*========== DROP STAGING TABLE ==============================================*/
/*============================================================================*/

-- DROP TABLE STAGING_TITLE_CREW;

/*============================================================================*/
/*========== END STAGING_TITLE_CREW - DATA PROCESSING ========================*/
/*============================================================================*/