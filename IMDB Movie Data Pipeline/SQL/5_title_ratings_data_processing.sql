/*============================================================================*/
/*========== STAGING_TITLE_RATINGS - DATA PROCESSING =========================*/
/*============================================================================*/
/* No problems with this data. 
*  We just need a bit of substring, CAST and FORMAT to get the data types correct  
*/
/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS Title_Rating;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE IF NOT EXISTS Title_Rating
(
  title_id          INTEGER
, average_rating    REAL
, number_of_votes   INTEGER
, PRIMARY KEY(title_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
)
WITHOUT ROWID, STRICT
;

/*========== Title_Rating ====================================================*/

INSERT INTO Title_Rating
SELECT  CAST(substring(title_id,3) AS INTEGER) AS title_id
,       FORMAT('%.1f',CAST(average_rating AS REAL)) AS average_rating
,       CAST(number_of_votes AS INTEGER) AS number_of_votes
FROM
        STAGING_TITLE_RATINGS
;

ANALYZE Title_Rating;

/*============================================================================*/
/*========== DROP STAGING TABLE ==============================================*/
/*============================================================================*/

-- DROP TABLE STAGING_TITLE_RATINGS;

/*============================================================================*/
/*========== END STAGING_TITLE_RATINGS - DATA PROCESSING =====================*/
/*============================================================================*/