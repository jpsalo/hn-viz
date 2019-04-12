WITH
  sub_stories AS (
  SELECT
    uni.id,
    uni.title,
    uni.score,
    uni.descendants,
    EXTRACT(YEAR
    FROM
      DATE(uni.timestamp)) AS year,
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
        stories.id AS id,
        stories.title AS title,
        stories.score AS score,
        stories.descendants AS descendants,
        stories.BY AS author,
        stories.timestamp AS timestamp
      FROM (
        SELECT
          stories.id,
          stories.title,
          stories.score,
          stories.descendants,
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
        stories.id AS id,
        stories.title AS title,
        stories.score AS score,
        stories.descendants AS descendants,
        stories.BY AS author,
        stories.timestamp AS timestamp
      FROM (
        SELECT
          stories.id,
          stories.title,
          stories.score,
          stories.descendants,
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
    descendants DESC ),
  sub_threads AS (
  SELECT
    MAX(sub_stories.title) AS title,
    MAX(sub_stories.type) AS type,
    MAX(sub_stories.score) AS score,
    MAX(sub_stories.descendants) AS descendants,
    COUNT(DISTINCT CAST(comments.timestamp AS DATE)) AS days,
    MAX(sub_stories.year) AS year,
    comments.parent AS threadId
  FROM
    `bigquery-public-data.hacker_news.full` AS comments
  INNER JOIN
    sub_stories
  ON
    comments.parent = sub_stories.id
  WHERE
    comments.dead IS NULL
    AND comments.deleted IS NULL
  GROUP BY
    comments.parent )
SELECT
  sub_threads.title,
  sub_threads.type,
  sub_threads.score,
  sub_threads.descendants,
  sub_threads.days,
  sub_threads.year,
  sub_threads.threadId
FROM
  sub_threads
ORDER BY
  sub_threads.year
