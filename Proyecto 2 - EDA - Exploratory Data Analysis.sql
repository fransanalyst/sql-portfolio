-- EDA - Análisis de Datos Exploratorio

/* En este análisis utilizaremos la base de datos que trabajamos en el proyecto de Data Cleaning */



SELECT * FROM layoffs_mod2;

-- Observamos los máximos para encontrar las cantidades de despidos y los porcentajes más altos

SELECT MAX(total_laid_off), MAX(percentage_laid_off) 
FROM layoffs_mod2;

/* Aquí buscamos los registros que hayan tenido un porcentaje de despido del 100%, que significa que
prácticamente el 100% de la compañía fue despedida.
Este tipo de despidos "layoffs" se da por contexto económico o situación de la empresa */

SELECT * FROM layoffs_mod2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

/* Ahora vemos la cantidad total de despido por compañía y las ordenamos de mayor a menor,
esto nos permite ver a las compañías con mayor cantidad de despidos */

SELECT company, SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY company
ORDER BY 2 DESC;

/* Aquí vemos el rango de fechas entre los que se realizaron estos despidos,
entre marzo de 2020 y marzo de 2023, en la época más fuerte de la pandemia */

SELECT MIN(date), MAX(date)
FROM layoffs_mod2;

-- Los países con mayor cantidad de despidos durante esta época

SELECT country, SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY country
ORDER BY 2 DESC;

SELECT * FROM layoffs_mod2;

-- Aquí podemos observar la cantidad de despidos por año

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY YEAR(date)
ORDER BY 1 DESC;

SELECT * FROM layoffs_mod2;

-- Total de despidos según la etapa o momento del ciclo de vida de la compañía

SELECT stage, SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY stage
ORDER BY 2 DESC;

-- Aquí podemos ver la cantidad de despidos por cada mes ordenados hacia la fecha más reciente

SELECT SUBSTRING(date,1,7) AS Month, SUM(total_laid_off)
FROM layoffs_mod2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY Month
ORDER BY 1 ASC;  

-- Aquí calcularemos el total acumulado según van avanzando los meses

WITH rolling_total AS
(
SELECT SUBSTRING(date,1,7) AS Month, SUM(total_laid_off) AS total_off
FROM layoffs_mod2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY Month
ORDER BY 1 ASC
)
SELECT Month, total_off, SUM(total_off)
OVER(ORDER BY Month) AS rolling_total
FROM rolling_total;

-- Aquí vemos el total de despidos por compañía ordenadas desde la que tuvo más despidos hasta la que menos

SELECT company, SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY company
ORDER BY 2 DESC;

-- Aquí la cantidad de despidos por compañía por año ordenados de mayor a menor

SELECT company, year(date), SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY company, year(date)
ORDER BY 3 DESC;

-- Aquí buscaremos un ranking de las 5 primeras compañías con mayor cantidad de despidos por cada año 

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, year(date), SUM(total_laid_off)
FROM layoffs_mod2
GROUP BY company, year(date)
), company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT * FROM company_year_rank
WHERE ranking <= 5;


