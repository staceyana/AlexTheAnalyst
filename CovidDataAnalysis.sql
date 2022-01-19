select *
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
order by 3,4

--select *
--from `alextheanalyst.CovidAnalysis.CovidVacs`
--where continent is not null
--order by 3,4

--Select Data we'll be using
select location, date, total_deaths, new_cases, total_deaths, population
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
order by 1,2

--total deaths vs total cases
select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathRate
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
and location = 'United States'
order by 1,2

--total cases vs total population
select location, date, total_cases, population, (total_cases/population)*100 as InfectionRate
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
--and location = 'United States'
order by 1,2

--what country has the highest infection rate
select location, population, max(total_cases) as HighestInfectionCount, max((total_cases/population)*100) as InfectionRate
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
group by location, population
order by 4 desc

--what country has the highest death count
--might need to cast as integer if not already
select location, max(total_deaths) as TotalDeath
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
group by location
order by 2 desc

--COMPARE CONTINENTS

--CORRECT WAY   
--don't run by continent as numbers are incorrect
--take out income
select location, max(total_deaths) as TotalDeath
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is null 
and location not like '%income%'
group by location
order by 2 desc

--INCORRECT NUMBERS, BUT GOOD FOR TABLEAU
--for tableau: numbers are incorrect but will use that to get to continent groupings
select continent, max(total_deaths) as TotalDeath
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
group by continent
order by 2 desc

--GLOBAL NUMBERS

--do aggregate functions (can't do sum(max), as 2 aggregates)
--might need to cast deaths as int or convert int

select date, sum(new_cases) as total_cases, sum(new_deaths) as total_deaths, sum(new_deaths)/sum(new_cases)*100  as DeathRate
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
group by date
order by 1,2

select sum(new_cases) as total_cases, sum(new_deaths) as total_deaths, sum(new_deaths)/sum(new_cases)*100  as DeathRate
from `alextheanalyst.CovidAnalysis.CovidDeaths`
where continent is not null
order by 1,2

--Vaxs table

select *
from `alextheanalyst.CovidAnalysis.CovidVacs`
where continent is not null
order by 3,4

--JOIN tables

select *
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date


--Vaccintation rate

select dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations
, sum(vax.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_vaxs
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date
where dea.continent is not null
order by 2,3

--CTE
--note that syntax is different in BigQuery vs MS SQL

with PopVsVax as 
(
select dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations
, sum(vax.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_vaxs
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date
where dea.continent is not null
)

select *, (rolling_vaxs/population)*100 as PercVax
from PopVsVax

--TEMP TABLES
--BigQUery has different syntax, not letting do INSERT in free version, DML 

drop table if exists PercentPopulationVaccinated

BEGIN 
create temp table PercentPopulationVaccinated
(
    Continent string, 
    location string,
    date datetime, 
    population numeric,
    new_vaccinations numeric, 
    rolling_vaxs numeric
);
end; 

insert into PercentPopulationVaccinated
select dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations
, sum(vax.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_vaxs
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date
where dea.continent is not null

select *, (rolling_vaxs/population)*100 as PercVax
from PercentPopulationVaccinated


--OG code for MS SQL
drop table if exists #PercentPopulationVaccinated
create table #PercentPopulationVaccinated
(
    Continent nvarchar(255), 
    location nvarchar(255),
    date datetime, 
    population numeric,
    new_vaccinations numeric, 
    rolling_vaxs numeric
)

insert into #PercentPopulationVaccinated
select dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations
, sum(vax.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_vaxs
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date
where dea.continent is not null

select *, (rolling_vaxs/population)*100 as PercVax
from #PercentPopulationVaccinated

--CREATE VIEWS to store for VIz
--BigQuery supports but requires to specify project, etc see line 1 (183)

create view `alextheanalyst.CovidAnalysis.PercVax` as 
select dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations
, sum(vax.new_vaccinations) over (partition by dea.location order by dea.location, dea.date) as rolling_vaxs
from `alextheanalyst.CovidAnalysis.CovidDeaths` dea
join `alextheanalyst.CovidAnalysis.CovidVacs` vax
    on dea.location = vax.location
    and dea.date = vax.date
where dea.continent is not null

select *
from `alextheanalyst.CovidAnalysis.PercVax`


