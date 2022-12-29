/*============================================================================*/
/*========== CREATE DATA STAGING TABLES ======================================*/
/*============================================================================*/
/* 
* Tables defined from information provided in imdb-datasets-interface.md
* This data will need to be cleaned and organized to make it easier to query 
*/
/*============================================================================*/
/*========== DROP TABLES =====================================================*/
/*============================================================================*/

DROP TABLE IF EXISTS STAGING_TITLE_AKAS;
DROP TABLE IF EXISTS STAGING_TITLE_BASICS;
DROP TABLE IF EXISTS STAGING_TITLE_CREW;
DROP TABLE IF EXISTS STAGING_TITLE_EPISODE;
DROP TABLE IF EXISTS STAGING_TITLE_PRINCIPALS;
DROP TABLE IF EXISTS STAGING_TITLE_RATINGS;
DROP TABLE IF EXISTS STAGING_NAME_BASICS;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_AKAS 
(
  title_id          TEXT                                      -- "alphanumeric unique identifier of the title"
, ordering          INTEGER                                   -- "a number to uniquely identify rows for a given title_id"
, title             TEXT                                      -- "the localized title"
, region            TEXT                                      -- "the region for this version of the title"
, language          TEXT                                      -- "the language of the title"
, types             TEXT                                      -- "Enumerated set of attributes for this alternative title. One or more of the following: 'alternative', 'dvd', 'festival', 'tv', 'video', 'working', 'original', 'imdbDisplay'"
, attributes        TEXT                                      -- "Additional terms to describe this alternative title, not enumerated"
, is_original_title INTEGER CHECK(is_original_title IN (0,1)) -- "0: not original title; 1: original title"
, PRIMARY KEY(title_id, ordering)
) 
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_BASICS 
(
  title_id          TEXT                              -- "alphanumeric unique identifier of the title"
, title_type        TEXT                              -- "the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)"
, primary_title     TEXT                              -- "the more popular title / the title used by the filmmakers on promotional materials at the point of release"
, original_title    TEXT                              -- "original title, in the original language"
, is_adult          INTEGER CHECK(is_adult IN (0,1))  -- "0: non-adult title; 1: adult title"
, start_year        INTEGER                           -- "represents the release year of a title. In the case of TV Series, it is the series start year"
, end_year          INTEGER                           -- "TV Series end year. '\N' for all other title types"
, run_time_minutes  INTEGER                           -- "primary runtime of the title, in minutes"
, genres            TEXT                              -- "includes up to three genres associated with the title"
, PRIMARY KEY(title_id)
) 
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_CREW 
(
  title_id          TEXT -- "alphanumeric unique identifier of the title"
, directors         TEXT -- "array of person_id - director(s) of the given title"
, writers           TEXT -- "array of person_id - writer(s) of the given title"
, PRIMARY KEY(title_id)
)
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_EPISODE 
(
  episode_id        TEXT    -- "alphanumeric identifier of episode"
, parent_title_id   TEXT    -- "alphanumeric identifier of the parent TV Series"
, season_number     INTEGER -- "season number the episode belongs to"
, episode_number    INTEGER -- "episode number of the tconst in the TV series"
, PRIMARY KEY(episode_id)
)
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_PRINCIPALS 
(
  title_id          TEXT    -- "alphanumeric unique identifier of the title"
, ordering          INTEGER -- "a number to uniquely identify rows for a given title_id"
, person_id         INTEGER -- "alphanumeric unique identifier of the name/person"
, category          TEXT    -- "the category of job that person was in"
, job               TEXT    -- "the specific job title if applicable, else '\N'"
, character         TEXT    -- "the name of the character played if applicable, else '\N'"
, PRIMARY KEY(title_id,ordering)
)
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_TITLE_RATINGS 
(
  title_id          TEXT    -- "alphanumeric unique identifier of the title"
, average_rating    REAL    -- "weighted average of all the individual user ratings"
, number_of_votes   INTEGER -- "number of votes the title has received"
, PRIMARY KEY(title_id)
)
WITHOUT ROWID
;

/*============================================================================*/

CREATE TABLE IF NOT EXISTS STAGING_NAME_BASICS 
(
  person_id             TEXT    -- "alphanumeric unique identifier of the name/person"
, primary_name          TEXT    -- "name by which the person is most often credited"
, birth_year            INTEGER -- "in YYYY format"
, death_year            INTEGER -- "in YYYY format if applicable, else '\N'"
, primary_profession    TEXT    -- "the top-3 professions of the person"
, known_for_titles      TEXT    -- "titles the person is known for"
, PRIMARY KEY(person_id)
)
WITHOUT ROWID
;

/*============================================================================*/
/*========== END CREATE DATA STAGING TABLES ==================================*/
/*============================================================================*/
