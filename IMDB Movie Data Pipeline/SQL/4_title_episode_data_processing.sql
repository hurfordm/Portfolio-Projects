/*============================================================================*/
/*========== STAGING_TITLE_EPISODE - DATA PROCESSING =========================*/
/*============================================================================*/
/* No problems with this data. 
*  We just need to substring and CAST our ids to INTEGER and NULL our '\N's   
*/

/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS Title_Episode;

/*============================================================================*/
/*========== CREATE TABLES ===================================================*/
/*============================================================================*/

CREATE TABLE Title_Episode
(
  title_episode_id  INTEGER
, title_id          INTEGER
, season_number     INTEGER
, episode_number    INTEGER
, PRIMARY KEY(title_episode_id)
, FOREIGN KEY(title_id) REFERENCES Title(title_id)
)
WITHOUT ROWID, STRICT
;

/*============================================================================*/
/*========== POPULATE TABLES =================================================*/
/*============================================================================*/

INSERT INTO Title_Episode
SELECT  CAST(substring(episode_id,3) AS INTEGER) AS title_episode_id
,       CAST(substring(parent_title_id,3) AS INTEGER) AS title_id
,       CAST(CASE WHEN season_number = '\N' THEN NULL ELSE season_number END AS INTEGER) AS season_number
,       CAST(CASE WHEN episode_number = '\N' THEN NULL ELSE episode_number END AS INTEGER) AS episode_number
FROM
        STAGING_TITLE_EPISODE
ORDER BY
        2 ASC
,       3 ASC
,       4 ASC
;

ANALYZE Title_Episode;

/*============================================================================*/
/*========== DROP STAGING TABLE ==============================================*/
/*============================================================================*/

-- DROP TABLE IF EXISTS STAGING_TITLE_EPISODE;

/*============================================================================*/
/*========== END STAGING_TITLE_EPISODE - DATA PROCESSING =====================*/
/*============================================================================*/