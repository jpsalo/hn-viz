/*
Using WHERE reduces the amount of data scanned / quota used
*/
SELECT
  stories.score AS score,
  stories.descendants AS descendants,
  EXTRACT(YEAR FROM DATE(stories.time_ts)) AS year,
  stories.title AS title,
  CASE
    WHEN STRPOS(title, "Ask HN: ") > 0 THEN
      CASE
        WHEN author = "whoishiring" OR author = "_whoishiring" THEN "whoishiring"
        ELSE "ask"
      END
    WHEN STRPOS(title, "Show HN: ") > 0 THEN "show"
    ELSE "story"
  END AS type
FROM
  `bigquery-public-data.hacker_news.stories` AS stories
WHERE
  NOT (score IS NULL) AND
  NOT (descendants IS NULL OR descendants = 0)
ORDER BY
  descendants DESC
LIMIT
  100
