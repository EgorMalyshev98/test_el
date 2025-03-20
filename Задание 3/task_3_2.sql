--1. Напишите SQL-запрос, который возвращает ТОП-5 самых продаваемых курсов по месяцам.
SELECT "month", course, sales_count
FROM 
	(SELECT 
	    date_trunc('month', "date") AS "month",
	    oc.course,
	    COUNT(*) AS sales_count,
	    ROW_NUMBER() OVER (PARTITION BY date_trunc('month', "date") ORDER BY count(*) DESC) AS rn
	FROM orders o
	JOIN order_courses oc ON o.id = oc.order_id
	WHERE oc.course IS NOT null
	GROUP BY 1, 2) t
WHERE rn <=5

-- 2. Напишите SQL-запрос, который возвращает ТОП-3 самых популярных пакетов по предметам.

SELECT subject, package, package_count
FROM 
	(SELECT 
		subject, 
		package,
		count(*) package_count,
		ROW_NUMBER() OVER(PARTITION BY subject ORDER BY count(*) DESC) rn
		
	FROM  order_subjects os
	WHERE package IS NOT null
	GROUP BY subject, package) t
WHERE rn <= 3
