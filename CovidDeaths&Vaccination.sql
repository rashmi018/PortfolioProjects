select * from PortfolioProject1..CovidDeaths$
order by 3,4

-- 1. select * 
--from PortfolioProject1..CovidVaccinations$
--order by 3,4

-- 2. Select Data that we are going to be using

select location, date, total_cases, new_cases, total_deaths, population
from PortfolioProject1..CovidDeaths$
order by 1,2

-- 3. Looking at Total Cases Vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
from PortfolioProject1..CovidDeaths$
where location like '%states%'
order by 1,2

-- 4. Looking at Total Cases Vs Population
select location, date, total_cases, population, (total_cases/population)*100 as PercentPopulationInfected
from PortfolioProject1..CovidDeaths$
where location like '%states%'
order by 1,2

-- 5. Looking at data for India

select * from PortfolioProject1..CovidDeaths$
where location like 'India'

--6. Looking at Countries with Highest Infection Rate compared to Population

select location, Population, MAX(total_cases) as HighestInfectionCount, Max((total_cases/population))*100 as PercentPopulationInfected
from PortfolioProject1..CovidDeaths$
group by location, population
order by PercentPopulationInfected desc

--7. Showing Countries with Highest Death Count Per Population

select location, Max(cast(total_deaths as int)) as TotalDeathCount --total deaths is coverted from nvarchar to int for better results using cast
from PortfolioProject1..CovidDeaths$
where continent is not null --we don't want continents with null values
group by location
order by TotalDeathCount desc

-- 8. Let's break things down by Continent
-- Showing continents with the highest death count

select continent, max(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject1..CovidDeaths$
where continent is not null
group by continent
order by TotalDeathCount desc

--9. Global Numbers

select Date, sum(new_cases) as TotalNewCases, sum(cast(new_deaths as int)) as TotalNewDeaths, -- TotalNewCases, TotalNewDeaths & DeathPercentage By Date
sum(cast(new_deaths as int))/sum(new_cases)*100 as DeathPercentage
from PortfolioProject1..CovidDeaths$
where continent is not null
group by Date
order by 1,2 

--10. Total New Cases World

select sum(new_cases) as TotalNewCases, sum(cast(new_deaths as int)) as TotalNewDeaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as DeathPercentage
from PortfolioProject1..CovidDeaths$
where Continent is not null
order by 1,2

-- 11. Join Deaths & Vaccination table

/* Selects all columns from both the CovidDeaths and CovidVaccinations tables.
Joins these two tables based on two conditions:The location values in both tables must be the same.The date values in both tables must be the same */

select *
from PortfolioProject1..CovidDeaths$ dea
join PortfolioProject1..CovidVaccinations$ vac
on dea.location = vac.location
and dea.date = vac.date

--12. Looking at Total Population Vs Vaccination
--This code joins data from the CovidDeaths and CovidVaccinations tables, selecting relevant columns (continent, location, date, population, and new vaccinations).
--It then filters out rows without a continent value and sorts the results by location and date, enabling analysis of the relationship between total population and 
--new vaccinations across different continents and locations.

select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
from PortfolioProject1..CovidDeaths$ dea
join PortfolioProject1..CovidVaccinations$ vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null 
order by 2,3	--This refers to the second and third columns in the select statement, which are dea.location and dea.date, respectively. 
--The results will be sorted by dea.location first, and then by dea.date within each location.

-- 13. Creating a CTE for a temporary table. USE Common Table Expression, CTE should've same columns as the original table

--utilizing a Common Table Expression (CTE) to calculate a rolling sum of vaccinated people and then use that to analyze the percentage of the population vaccinated.
--With: This keyword introduces a Common Table Expression (CTE). CTEs are temporary named result sets that are defined within a query and can be referenced 
--within the same query.as: This keyword separates the CTE definition from its query.
--PopvsVac: This is the name of the CTE, which will hold the intermediate data.

-- sum(convert(int,vac.new_vaccinations)): This calculates the sum of new_vaccinations for each location.
--over (Partition by dea.location order by dea.location, dea.date): This specifies a window function that calculates the cumulative sum:
--Partition by dea.location: This groups the data by location.
--order by dea.location, dea.date: This orders the data within each location group by location and then by date. The rolling sum is calculated based on this order.

With PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
as 
(
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
sum(convert(int,vac.new_vaccinations)) over (Partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
from PortfolioProject1..CovidDeaths$ dea
join PortfolioProject1..CovidVaccinations$ vac
on dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
)
select *, (RollingPeopleVaccinated/Population)*100 as PercentagePopulationVaccinated
from PopvsVac

--select: This selects all columns from the CTE (PopvsVac) and a new calculated column.
-- *: This represents all columns from the CTE.
-- (RollingPeopleVaccinated/Population)*100: This calculates the percentage of the population vaccinated for each location and date:
-- RollingPeopleVaccinated/Population: Divides the rolling sum of vaccinated people by the population.
--*100: Multiplies the result by 100 to express it as a percentage.

--This code does the following:
--Defines a CTE named PopvsVac that calculates the cumulative sum of vaccinated people (RollingPeopleVaccinated) for each location over time.
--Uses the CTE to select all columns from the CTE along with a new column that calculates the percentage of the population vaccinated.
--This results in a dataset that shows the total population, number of new vaccinations, rolling sum of vaccinated people, and the percentage of the 
--population vaccinated for each location and date, providing valuable insights into vaccination progress.

--14. Temp Table

-- Using Temp Table to perform Calculation on Partition By in previous query
--This code joins the CovidDeaths and CovidVaccinations tables, calculates the cumulative sum of vaccinations (RollingPeopleVaccinated) for each location using a
--window function, and then calculates the percentage of the population vaccinated. It stores the results in a temporary table #PercentPopulationVaccinated 
--for further analysis or display.



DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
    Continent nvarchar(255),
    Location nvarchar(255),
    Date datetime,
    Population numeric,
    New_vaccinations numeric,
    RollingPeopleVaccinated numeric
)

-- Correctly calculate RollingPeopleVaccinated
INSERT INTO #PercentPopulationVaccinated
SELECT 
    dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    CAST(vac.new_vaccinations AS numeric) AS New_vaccinations, -- Use numeric here
    SUM(CAST(vac.new_vaccinations AS numeric)) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS RollingPeopleVaccinated
	--Sums vac.new_vaccinations values for each location; Over Partition By is a window function that calculates the cumulative sum (RollingPeopleVaccinated) 
	--for each location, ordered by date. In PartionBy the sum is calculated separately for each dea.location
	--ORDER BY dea.location, dea.Date: The cumulative sum is calculated in order of date within each location.


FROM 
    PortfolioProject1..CovidDeaths$ dea
JOIN 
    PortfolioProject1..CovidVaccinations$ vac ON dea.location = vac.location AND dea.date = vac.date
-- WHERE dea.continent IS NOT NULL -- You might want to re-add this if you need to filter

-- Calculate the percentage and select everything
SELECT 
    *, 
    (RollingPeopleVaccinated/Population)*100 AS PercentPopulationVaccinated
FROM 
    #PercentPopulationVaccinated


--15. Create view

CREATE VIEW PercentPopulationVaccinated2 AS
SELECT 
    dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    CAST(vac.new_vaccinations AS numeric) AS New_vaccinations, -- Use numeric here
    SUM(CAST(vac.new_vaccinations AS numeric)) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS RollingPeopleVaccinated
FROM 
    PortfolioProject1..CovidDeaths$ dea
JOIN 
    PortfolioProject1..CovidVaccinations$ vac ON dea.location = vac.location AND dea.date = vac.date
WHERE dea.continent IS NOT NULL 

SELECT * FROM PercentPopulationVaccinated2;
 
