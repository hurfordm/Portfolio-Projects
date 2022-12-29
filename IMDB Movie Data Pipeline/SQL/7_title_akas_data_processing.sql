/*============================================================================*/
/*========== STAGING_TITLE_AKAS - DATA PROCESSING ============================*/
/*============================================================================*/

/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS TEMP_AKAS;
DROP TABLE IF EXISTS AKAS_Region;
DROP TABLE IF EXISTS AKAS_Language;
DROP TABLE IF EXISTS AKAS_Title;
DROP TABLE IF EXISTS AKAS_Title_Type;
DROP TABLE IF EXISTS AKAS_Type;
DROP TABLE IF EXISTS AKAS_Attribute;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE AKAS_Title
(
  akas_id           INTEGER
, title_id          INTEGER
, akas_title        TEXT
, region_id         INTEGER
, language_id       INTEGER
, attribute_id      INTEGER
, is_original_title INTEGER CHECK(is_original_title IN (0,1))
, PRIMARY KEY(akas_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
, FOREIGN KEY(region_id) REFERENCES AKAS_Region(region_id)
, FOREIGN KEY(language_id) REFERENCES AKAS_Language(language_id)
, FOREIGN KEY(attribute_id) REFERENCES AKAS_Attribute(attribute_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE AKAS_Type -- AKAS: (A)lso (K)nown (AS)
(
  type_id         INTEGER
, type_name       TEXT    NOT NULL
, PRIMARY KEY(type_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE AKAS_Title_Type
(
  akas_id        INTEGER
, type_id         INTEGER
, PRIMARY KEY(akas_id,type_id)
, FOREIGN KEY(akas_id) REFERENCES AKAS_Title(akas_id)
, FOREIGN KEY(type_id) REFERENCES AKAS_Type(type_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE AKAS_Attribute
(
  attribute_id      INTEGER
, attribute_name    TEXT
, PRIMARY KEY(attribute_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE AKAS_Region
(
  region_id     INTEGER
, region_name   TEXT
, PRIMARY KEY(region_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/

CREATE TABLE AKAS_Language
(
  language_id     INTEGER
, language_name   TEXT
, PRIMARY KEY(language_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

/*========== TEMP_AKAS =======================================================*/

/* Let's clean up our raw data
*
*  1. Replace pseudo null '\N' with actual NULL values (region, language, types and attributes fields)
*  2. Replace weird control character U+0002 -> '' where there should be a space!? (types and attributes fields)
*  3. Split out type values, maximum of 2 types exist in this field separated by a space
*  4. Convert our title_id into an integer by chopping off the (unnecessary) first 2 alpha characters and CASTing
*  5. Use aggregate windows function to generate unique alternative title id. Amazon provides an order_id within title_id but that suggests a meaning to the order which doesn't exist
*/

/* Performance Optimizations
*
*  1. Use temp_store PRAGMA to change location of temporary working tables and indexes to memory instead of the default file. Memory access is way faster than disk I/O.
*  2. Use cache_size PRAGMA to increase cache memory allocation. Increased from the default of 2MB to 512MB. Makes a huge difference in query run times.
*  3. Create indexes on our table to assist with later queries. 
*/

PRAGMA temp_store = 2; -- MEMORY, default = FILE
PRAGMA cache_size = -512000; -- 512MB, default = 2MB
CREATE TABLE TEMP_AKAS AS
SELECT  SUM(1) OVER (ORDER BY   title_id ASC
                        ,       is_original_title DESC
                        ,       title ASC
                        ,       region ASC
                        ,       language ASC
                        ,       type_1 ASC
                        ,       type_2 ASC
                        ,       attribute ASC
                        ) AS akas_id
,       title_id
,       title
,       region
,       language
,       type_1
,       type_2
,       attribute
,       is_original_title
FROM    (
        SELECT  DISTINCT
                CAST(substring(title_id,3) AS INTEGER) AS title_id
        ,       title
        ,       CASE WHEN region = '\N' THEN NULL ELSE region END AS region
        ,       CASE WHEN language = '\N' THEN NULL ELSE language END AS language
        ,       CASE    WHEN instr(replace(trim(types),'',' '),' ') > 0
                        THEN substring(replace(trim(types),'',' '),0,instr(replace(trim(types),'',' '),' '))
                        ELSE CASE   WHEN types = '\N'
                                THEN NULL
                                ELSE types
                        END
                END AS type_1
        ,       CASE    WHEN instr(replace(trim(types),'',' '),' ') > 0
                        THEN substring(replace(trim(types),'',' '),instr(replace(trim(types),'',' '),' ')+1)
                        ELSE NULL
                END AS type_2
        ,       replace(trim(attributes),'',' ') AS attribute
        ,       is_original_title
        FROM
                STAGING_TITLE_AKAS
        ORDER BY
                title_id ASC
        ,       is_original_title DESC
        ,       title ASC
        ,       region ASC
        ,       language ASC
        ,       type_1 ASC
        ,       type_2 ASC
        ,       attribute ASC
        ) dt1
;

CREATE UNIQUE INDEX PK_TEMP_AKAS__AKAS_ID ON TEMP_AKAS(akas_id);
CREATE INDEX IDX_TEMP_AKAS__REGION ON TEMP_AKAS(region);
CREATE INDEX IDX_TEMP_AKAS__LANGUAGE ON TEMP_AKAS(language);
CREATE INDEX IDX_TEMP_AKAS__TYPE_1 ON TEMP_AKAS(type_1);
CREATE INDEX IDX_TEMP_AKAS__TYPE_2 ON TEMP_AKAS(type_2);

ANALYZE TEMP_AKAS;

/*========== AKAS_Region ======================================================*/

INSERT INTO AKAS_Region
SELECT  SUM(1) OVER (ORDER BY region_name ASC) AS region_id
,       region_name
FROM    (
        SELECT  DISTINCT region AS region_name
        FROM
                TEMP_AKAS
        WHERE
                region IS NOT NULL
        ) AS dt1
;

ANALYZE AKAS_Region;

/*========== AKAS_Language ====================================================*/

INSERT INTO AKAS_Language
SELECT  SUM(1) OVER (ORDER BY language_name) AS language_id
,       language_name
FROM    (
        SELECT  DISTINCT language AS language_name
        FROM
                TEMP_AKAS
        WHERE
                language IS NOT NULL
        ) AS dt1
;

ANALYZE AKAS_Language;

/*========== AKAS_Type ========================================================*/

INSERT INTO AKAS_Type
SELECT  SUM(1) OVER (ORDER BY akas_type)
,       akas_type
FROM    (
        SELECT  DISTINCT type_1 AS akas_type
        FROM
                TEMP_AKAS
        WHERE   
                type_1 IS NOT NULL
        UNION
        SELECT  DISTINCT type_2 AS akas_type
        FROM
                TEMP_AKAS
        WHERE
                type_2 IS NOT NULL
        ) dt1
;

ANALYZE AKAS_Type;

/*========== AKAS_Attribute =============================================*/

INSERT INTO AKAS_Attribute 
SELECT  SUM(1) OVER (ORDER BY attribute_count DESC, attribute ASC) AS attribute_id
,       attribute AS attribute_name
FROM    (
        SELECT  attribute
        ,       COUNT(*) AS attribute_count
        FROM
                TEMP_AKAS
        WHERE
                attribute != '\N'
        AND     attribute IS NOT NULL
        GROUP BY
                1
        ) AS dt1
;

ANALYZE AKAS_Attribute;

/*========== Title_AKAS =================================================*/

/* 
* Create temporary indexes to help this query
* (we will manually delete them later)
* Structuring our query as nested derived tables 'helps' the query planner use the indexes
*/
CREATE INDEX IDX_TMP_REGION ON AKAS_Region(region_name);
CREATE INDEX IDX_TMP_LANG ON AKAS_Language(language_name);
CREATE INDEX IDX_TMP_ATTR ON AKAS_Attribute(attribute_name);

-- PRAGMA temp_store = 2; -- MEMORY, default = FILE
-- PRAGMA cache_size = -512000; -- 512MB, default = 2MB
INSERT INTO AKAS_Title
SELECT  dt2.akas_id
,       dt2.title_id          
,       dt2.akas_title        
,       dt2.region_id            
,       dt2.language_id 
,       akatrr.attribute_id
,       dt2.is_original_title 
FROM    (
        SELECT  dt1.akas_id
        ,       dt1.title_id          
        ,       dt1.akas_title        
        ,       dt1.region_id            
        ,       lang.language_id 
        ,       dt1.attribute
        ,       dt1.is_original_title 
        FROM    (
                SELECT  DISTINCT
                        aka.akas_id 
                ,       aka.title_id          
                ,       aka.title AS akas_title        
                ,       reg.region_id            
                ,       aka.language 
                ,       aka.attribute    
                ,       aka.is_original_title  
                FROM
                        TEMP_AKAS AS aka
                        LEFT JOIN
                        AKAS_Region AS reg
                ON
                        aka.region = reg.region_name
                AND     aka.is_original_title = 0
                ) AS dt1
                LEFT JOIN
                AKAS_Language AS lang
        ON
                dt1.language = lang.language_name
        ) as dt2
        LEFT JOIN
        AKAS_Attribute AS akatrr
ON
        dt2.attribute = akatrr.attribute_name        
;

ANALYZE AKAS_Title;

/*========== AKAS_Title_Type =================================================*/

INSERT INTO AKAS_Title_Type
SELECT  ta.akas_id
,       tat.type_id    
FROM
        TEMP_AKAS AS ta
        LEFT JOIN
        AKAS_Type AS tat
ON
        ta.type_1 = tat.type_name
WHERE   
        ta.type_1 IS NOT NULL
UNION
SELECT  ta2.akas_id
,       tat2.type_id
FROM
        TEMP_AKAS AS ta2
        LEFT JOIN
        AKAS_Type AS tat2
ON
        ta2.type_2 = tat2.type_name
WHERE   
        ta2.type_2 IS NOT NULL
ORDER BY
        1,2
;

ANALYZE AKAS_Title_Type;

/*============================================================================*/
/*========== DROP TEMP AND STAGING DATABASE OBJECTS ==========================*/
/*============================================================================*/

DROP INDEX IDX_TMP_REGION;
DROP INDEX IDX_TMP_LANG;
DROP INDEX IDX_TMP_ATTR;
DROP TABLE TEMP_AKAS;
-- DROP TABLE STAGING_TITLE_AKAS;

/*============================================================================*/
/*========== END STAGING_TITLE_AKAS - DATA PROCESSING ========================*/
/*============================================================================*/