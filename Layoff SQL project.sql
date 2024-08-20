-- Data cleaning
SELECT *
FROM layoffs;


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date` ) AS row_num
FROM layoffs_staging;
-- identify duplicates
WITH dublicate_cte AS
( SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions ) AS row_num
FROM layoffs_staging
 )
SELECT *
FROM dublicate_cte
WHERE row_num > 1;

-- handel duplicates
create table `layoffs_staging3` (
`company` text,
`location` text,
`industry` text,
`total_laid_off` int default null,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int default null,
`row_num` int
) engine =InnoDB default charset = utf8mb4 collate = utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging3
where row_num > 1;

insert into layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions ) AS row_num
FROM layoffs_staging;

delete
FROM layoffs_staging3
where row_num > 1;

SELECT *
FROM layoffs_staging3;

-- Standrizing data
SELECT company, trim(company)
from layoffs_staging3;

update layoffs_staging3
set company = trim(company);

SELECT distinct industry
from layoffs_staging3
ORDER BY 1;
SELECT *
from layoffs_staging3
WHERE industry like 'Crypto%';

update layoffs_staging3
set industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT distinct location
from layoffs_staging3
ORDER BY 1;
SELECT distinct country
from layoffs_staging3
ORDER BY 1;
SELECT *
from layoffs_staging3
WHERE country like 'United States%';

SELECT distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging3
ORDER BY 1;

update layoffs_staging3
set country = trim(TRAILING '.' FROM country)
WHERE country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging3;

update layoffs_staging3
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging3;

ALTER table layoffs_staging3
modify column `date` DATE;

-- handel nulls
select *
from layoffs_staging3
where total_laid_off is null 
AND percentage_laid_off IS NULL;

select *
from layoffs_staging3
where industry is null 
OR industry = '';

update layoffs_staging3
set industry = null
WHERE industry = '';

select *
from layoffs_staging3
where company LIKE 'Bally%';

select t1.industry, t2.industry
from layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
where t1.industry is null 
and t2.industry is not null;

update layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


select *
from layoffs_staging3
where total_laid_off is null 
AND percentage_laid_off IS NULL;

delete
from layoffs_staging3
where total_laid_off is null 
AND percentage_laid_off IS NULL;

select *
from layoffs_staging3;

ALTER table layoffs_staging3
drop column row_num;

-- Exploratory Data Analysis
SELECT *
FROM layoffs_staging3;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging3;

SELECT *
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY  funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) 
FROM layoffs_staging3
group by company
order by 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging3;

SELECT industry, SUM(total_laid_off) 
FROM layoffs_staging3
group by industry
order by 2 DESC;

SELECT country, SUM(total_laid_off) 
FROM layoffs_staging3
group by country
order by 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging3
group by YEAR(`date`)
order by 1 DESC;

SELECT stage, SUM(total_laid_off) 
FROM layoffs_staging3
group by stage
order by 2 DESC;

SELECT substring(`date`, 1,7) AS `Month`, SUM(total_laid_off) 
FROM layoffs_staging3
where substring(`date`, 1,7) IS NOT NULL
group by `Month`
order by 1 ASC;

WITH Rolling_Total AS
(
SELECT substring(`date`, 1,7) AS `Month`, SUM(total_laid_off) AS Total_off
FROM layoffs_staging3
where substring(`date`, 1,7) IS NOT NULL
group by `Month`
order by 1 ASC
)
SELECT `Month`, Total_off
,SUM(Total_off) OVER(order by `Month`) AS rolling_total
FROM Rolling_Total;



SELECT company, Year(`date`), SUM(total_laid_off) 
FROM layoffs_staging3
group by company, Year(`date`)
order by 3 DESC;

WITH Company_Year (Company, Years, total_laid_off) AS
(
SELECT company, Year(`date`), SUM(total_laid_off) 
FROM layoffs_staging3
group by company, Year(`date`)
order by 3 DESC
), Company_Year_Rank AS
(
SELECT *, 
dense_rank() OVER(partition by Years order by total_laid_off desc) AS Ranking
FROM Company_Year
WHERE Years IS NOT NULL

)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5 ;


