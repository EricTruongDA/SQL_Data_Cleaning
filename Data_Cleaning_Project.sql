-- Data Cleaning 
-- I converted the raw data from csv to JSON in order to import the data into MySQL
 /* the goal is to 
	1. remove duplicates
    2. standardize the data
		Fix spellings 
		Create consistency throughout the data 
	3. Figure what to do with null values / blank values
    4. Remove columns and rows 
		Remove any data that doesnt make any sense */
USE world_layoffs;

CREATE TABLE layoffs_copy
LIKE layoffs; 

-- Making sure all the column names were copied 
SELECT * 
FROM layoffs_copy;

-- Inserting the raw data into the data we will manipulate 
INSERT layoffs_copy
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_copy;

--  row number partition on everysingle column 
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_copy;

-- create a CTE to find the duplicates. The duplicates would be row_num > 1

WITH duplicate_CTE AS(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_copy
)
SELECT *
FROM duplicate_CTE
WHERE row_num >1;


DELETE FROM duplicate_CTE
WHERE row_num > 1; -- cannot delete from here, so we will create a new table and
				   -- insert what we need for the columns. The reason is because there is no
				   -- no primary key in this data set. 



-- run this so the table will have row_num
CREATE TABLE `layoffs_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- seeing how will our new table look like and if it has all the table we went, espicially row_num
SELECT * FROM layoffs_copy2;

INSERT INTO layoffs_copy2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_copy;

SELECT * 
FROM layoffs_copy2
WHERE row_num > 1;

-- Checking to see if our queries pull what we needed
SELECT  * 
FROM layoffs_copy2
WHERE company = 'Casper';

-- Once we confirm, we wil delete the duplicates 
DELETE 
FROM layoffs_copy2
WHERE row_num > 1;

-- Double check with the query if they're gone
SELECT * 
FROM layoffs_copy2
WHERE row_num > 1;

-- this new table that we got now will not have any duplicates 
SELECT  * 
FROM layoffs_copy2;

-- let's do the next step with standarding data 
SELECT company, TRIM(company)
FROM layoffs_copy2; 

-- Need to update the table with our trim
UPDATE layoffs_copy2 
SET company = TRIM(company);

-- Look into indsutry
SELECT DISTINCT(industry)
FROM layoffs_copy2
ORDER BY 1;

-- update all the crypto currency to be the same industry
SELECT *
FROM layoffs_copy2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_copy2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

-- let's check if we did update all crypto industries
SELECT *
FROM layoffs_copy2
WHERE industry LIKE 'Crypto%'; 

SELECT DISTINCT(industry)
FROM layoffs_copy2
ORDER BY 1;

-- let's check on location (important to manually go through data)
SELECT DISTINCT(location)
FROM layoffs_copy2
ORDER BY 1;

-- see if there's any errors on countries
SELECT DISTINCT(country)
FROM layoffs_copy2
ORDER BY 1; -- in this case we found United States with a period, we gnna want to fix that

SELECT 
	DISTINCT country,
    TRIM(TRAILING '.' FROM country)
FROM layoffs_copy2
ORDER BY 1; 

UPDATE layoffs_copy2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT * 
FROM layoffs_copy2;

-- converting the text date to actual date format, we also have to give the date format we want 
SELECT
	`date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_copy2;

-- There was a date name null, IS NULL was not working so i had to do = 'NULL'
DELETE FROM layoffs_copy2
WHERE `date` = 'NULL';

UPDATE layoffs_copy2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_copy2;

-- changing the format to date for our column `date`
ALTER TABLE layoffs_copy2
MODIFY COLUMN `date` DATE;


SELECT * 
FROM layoffs_copy2
WHERE total_laid_off = 'NULL' 
AND percentage_laid_off = 'NULL';

-- there's a missing value and a NULL after doing the query below
SELECT DISTINCT industry
FROM layoffs_copy2; 

SELECT *
FROM layoffs_copy2
WHERE industry = 'NULL' 
OR industry = ''; 

-- can populate data like airbnb for industry 
SELECT *
FROM layoffs_copy2
WHERE company = 'Airbnb';

-- we want to fill the missing industry with companies that have another row such as airbnb
-- companies like airbnb had multiple lay offs so we can choose what was populated for it orginally
SELECT *
FROM layoffs_copy2 t1
	JOIN layoffs_copy2 t2
		ON t1.company = t2.company 
WHERE t1.industry = '' AND t2.industry IS NOT NULL;

-- make it look a little easier, with side by side table 
SELECT t1.industry, t2.industry
FROM layoffs_copy2 t1
	JOIN layoffs_copy2 t2
		ON t1.company = t2.company 
WHERE t1.industry = '' AND t2.industry IS NOT NULL;

-- we need to change the blanks into NULL
UPDATE layoffs_copy2
SET industry = NULL 
WHERE industry = '';

-- we want to update the table

UPDATE layoffs_copy2 t1
JOIN layoffs_copy2 t2
		ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- double check everything
SELECT *
FROM layoffs_copy2
WHERE industry = 'NULL' 
OR industry = ''; 

-- delete data that will not be useful and there's no way in finding those information 
DELETE FROM layoffs_copy2
WHERE total_laid_off = 'NULL' 
AND percentage_laid_off = 'NULL';

-- look at the table we have 
SELECT *
FROM layoffs_copy2;

-- to have our final clean data, we need to drop one of the column 
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;

-- I do want to change the 'NULL' to actual 'NULL', may have been error converting CSV to JSON
UPDATE layoffs_copy2
SET total_laid_off = NULL 
WHERE total_laid_off = 'NULL';

-- lets do it for percentage_laid_off also
UPDATE layoffs_copy2
SET percentage_laid_off = NULL 
WHERE percentage_laid_off = 'NULL';

-- do it for funds_raised_millions also 
UPDATE layoffs_copy2
SET funds_raised_millions = NULL 
WHERE funds_raised_millions = 'NULL';

-- final clean data 
SELECT *
FROM layoffs_copy2;
