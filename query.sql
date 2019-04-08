/*
Using WHERE reduces the amount of data scanned / quota used
*/
#standardSQL
SELECT
  stories.score AS score,
  stories.descendants AS descendants,
  stories.year AS year,
  stories.title AS title,
  CASE
    WHEN STRPOS(stories.title, "Ask HN: ") > 0 THEN CASE
    WHEN stories.author = 'whoishiring'
  OR stories.author = "_whoishiring" THEN "whoishiring"
    ELSE "ask"
  END WHEN STRPOS(stories.title, "Show HN: ") > 0 THEN "show"
    ELSE "story"
  END AS type
FROM (
  SELECT
    stories.score,
    stories.descendants,
    stories.title,
    stories.author,
    EXTRACT(YEAR
    FROM
      DATE(stories.time_ts)) AS year,
    ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM DATE(stories.time_ts))
    ORDER BY
      stories.score DESC) AS rn
  FROM
    `bigquery-public-data.hacker_news.stories` AS stories ) AS stories
WHERE
  stories.rn <= 100
  AND NOT (score IS NULL)
  AND NOT (descendants IS NULL
    OR descendants <= 0)
ORDER BY
  year DESC,
  score DESC
