/* We take the top 50000 posts by ViewCount.
We can remove TOP 50000 from the query as stackExcahnge only give 50000 lines per query */
SELECT TOP 50000 *
FROM posts
ORDER BY posts.ViewCount DESC;

/* The FETCH NEXT will give the next row after the 50000 offset,
meaning we get the next 50000 rows after the first 50000 rows.

The OFFSET with FETCH NEXT ROW ONLY will give the next row after the 50000 offsets,
meaning we exclude the first set of 50000 records and take the next 50000 rows */
SELECT *
FROM posts
ORDER BY posts.ViewCount DESC
OFFSET 50000 ROWS
FETCH NEXT 50000 ROW ONLY;

/* The FETCH NEXT will give the next row after the 100000 offset,
meaning we get the next 50000 rows after the first 100000 rows */
SELECT *
FROM posts
ORDER BY posts.ViewCount DESC
OFFSET 100000 ROWS
FETCH NEXT 50000 ROW ONLY;

/* The FETCH NEXT will give the next row after the 150000 offset,
meaning we get the next 50000 rows after the first 150000 rows */
SELECT *
FROM posts
ORDER BY posts.ViewCount DESC
OFFSET 150000 ROWS
FETCH NEXT 50000 ROW ONLY;
