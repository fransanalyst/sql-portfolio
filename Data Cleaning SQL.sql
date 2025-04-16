-- Limpieza de Datos con SQL

-- Fuente de datos: https://www.kaggle.com/datasets/swaptr/layoffs-2022

/* Datos de despidos por reducción en empresas durante la época de pandemia
de Marzo de 2020 a Marzo de 2023 */


SELECT * FROM layoffs;

-- Proceso de Limpieza de Datos
-- 1. Remover Duplicados
-- 2. Estandarizar los Datos
-- 3. Corregir Datos nulos o vacíos
-- 4. Remover Filas o Columnas (sólo para este proyecto)

-- Creamos una tabla modificable para no cambiar la tabla original y trabajaremos en la modificable

CREATE TABLE layoffs_mod
LIKE layoffs;

SELECT * FROM layoffs_mod;

INSERT layoffs_mod
SELECT * FROM layoffs;

SELECT * FROM layoffs_mod;


-- 1. Remover Duplicados

/* Buscamos duplicados -> Los registros que arrojen resultado de row_num mayor a 1,
será porque tienen datos idénticos (por lo tanto, duplicados) */

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_mod;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_mod
)
SELECT * FROM duplicates_cte
WHERE row_num > 1;

-- Creamos una nueva tabla para añadir la columna de row_num y hacer la eliminación más sencilla

CREATE TABLE `layoffs_mod2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Ahora insertamos todos los datos incluyendo row_num en la nueva tabla

SELECT * FROM layoffs_mod2;

INSERT INTO layoffs_mod2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_mod;

-- Ahora sólo eliminamos los registros que tengan un row_num mayor a 1

SELECT * FROM layoffs_mod2
WHERE row_num > 1;

DELETE FROM layoffs_mod2
WHERE row_num > 1;

-- Verificamos la eliminación de los datos y ya está lista la eliminación de duplicados

SELECT * FROM layoffs_mod2
WHERE row_num > 1;


-- 2. Estandarizar los Datos

/* Al observar la tabla notamos que hay nombres de compañías con espacios en blanco antes de la palabra,
así que quitamos los espacios y actualizamos la columna */

SELECT company, TRIM(company)
FROM layoffs_mod2;

UPDATE layoffs_mod2
SET company = TRIM(company);

/* Ahora buscamos errores en la columna industry y encontramos que la indutria Crypto está escrita de tres
maneras distintas, así que la estandarizaremos con 'Crypto' que es la forma en que se escribe en la mayoría
de las compañías de la tabla */

SELECT DISTINCT industry
FROM layoffs_mod2
ORDER BY industry;

SELECT * FROM layoffs_mod2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_mod2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

/* Ahora buscamos errores en Locación y encontramos que 'United States' tiene un error en algunas compañías,
contiene un punto al final del nombre, así que quitaremos ese punto para que quede estandarizado */

SELECT DISTINCT location
FROM layoffs_mod2
ORDER BY location;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_mod2
ORDER BY country;

UPDATE layoffs_mod2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

/* Ahora el error está en la columna de fecha, encontramos que las fechas están definidas como tipo de dato string,
así que la actualizaremos para tener el tipo correcto 'date' */

SELECT date, STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_mod2;

UPDATE layoffs_mod2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

SELECT date
FROM layoffs_mod2;

ALTER TABLE layoffs_mod2
MODIFY COLUMN date DATE;


-- 3. Corregir Datos nulos o vacíos

/* Aquí buscamos datos nulos o vacíos, encontramos que algunos registros de ciertas
compañías (como Airbnb) tienen vacío el dato de industry, así que completaremos
esos datos con los registros de las mismas compañías que sí lo tienen */

SELECT * FROM layoffs_mod2
WHERE industry IS NULL
OR industry = '';

SELECT * FROM layoffs_mod2
WHERE company = 'Airbnb';

UPDATE layoffs_mod2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_mod2 t1
JOIN layoffs_mod2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_mod2 t1
JOIN layoffs_mod2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_mod2;


-- 4. Remover Filas o Columnas (sólo para este proyecto)

/* En esta tabla encontramos datos que no son utilizables, así que no nos serían útiles en el análisis,
las eliminamos para tener una tabla más limpia */

SELECT * FROM layoffs_mod2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_mod2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_mod2;

-- Finalmente eliminamos la columna que creamos al inicio para eliminar duplicados

ALTER TABLE layoffs_mod2
DROP COLUMN row_num;

SELECT * FROM layoffs_mod2;

/* Ahora ya tenemos una tabla limpia con la que podemos realizar nuestro análisis 
de una manera más eficiente */