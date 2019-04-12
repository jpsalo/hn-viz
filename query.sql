SELECT
  uni.score,
  uni.descendants,
  EXTRACT(YEAR
  FROM
    DATE(uni.timestamp)) AS year,
  uni.title,
  CASE
    WHEN STRPOS(uni.title, "Ask HN: ") > 0 THEN CASE
    WHEN uni.author = 'whoishiring'
  OR uni.author = "_whoishiring" THEN "whoishiring"
    ELSE "ask"
  END WHEN STRPOS(uni.title, "Show HN: ") > 0 THEN "show"
    ELSE "story"
  END AS type
FROM ((
    SELECT
      stories.score AS score,
      stories.descendants AS descendants,
      stories.timestamp AS timestamp,
      stories.BY AS author,
      stories.title AS title
    FROM (
      SELECT
        stories.score,
        stories.descendants,
        stories.title,
        stories.BY,
        stories.timestamp,
        ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM DATE(stories.timestamp))
        ORDER BY
          stories.score DESC) AS rn
      FROM
        `bigquery-public-data.hacker_news.full` AS stories
      WHERE
        stories.type = 'story') AS stories
    WHERE
      stories.rn <= 100
      AND NOT (stories.score IS NULL
        OR stories.score <= 0))
  UNION DISTINCT (
    SELECT
      stories.score AS score,
      stories.descendants AS descendants,
      stories.timestamp AS timestamp,
      stories.BY AS author,
      stories.title AS title
    FROM (
      SELECT
        stories.score,
        stories.descendants,
        stories.title,
        stories.BY,
        stories.timestamp,
        ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM DATE(stories.timestamp))
        ORDER BY
          stories.descendants DESC) AS rn
      FROM
        `bigquery-public-data.hacker_news.full` AS stories
      WHERE
        stories.type = 'story') AS stories
    WHERE
      stories.rn <= 100
      AND NOT (stories.descendants IS NULL
        OR stories.descendants <= 0))) AS uni
ORDER BY
  year DESC,
  descendants DESC
