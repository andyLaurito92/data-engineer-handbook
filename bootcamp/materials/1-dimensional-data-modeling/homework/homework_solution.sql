--- 1 . DDL for actors

do $$ 
begin
	create type film as (
		name text,
		votes integer,
		rating real,
		filmid text
		
	);

	create type quality_class as enum('star', 'good', 'average', 'bad');
exception
	when duplicate_object then null;
end $$;


drop table actors;
create table if not exists actors (
	actorid text,
	actor text,
	films film[],
	quality_class quality_class,
	is_active boolean,
	current_year integer,
	primary key(actorid, current_year)
);


--- 2. Cumulative query generation
--- min(year) = 1970
insert into actors
with yesterday as (
	select *
	from actors 
	where current_year = 1969
),
today as (
	select 
		actor, 
		actorid, 
		year, 
		ARRAY_AGG(ROW(film, votes, rating, filmid)::film) AS films,
		avg(rating) as avg_rating
	from actor_films af1
	where af1.year = 1970
	group by actor, actorid, year
)
	select
	coalesce(y.actorid, t.actorid) as actorid,
	coalesce(y.actor, t.actor) as actor,
	case
		when y.films is null then t.films
		else y.films || t.films
	end as films,
	case 
		when t.avg_rating > 8 then 'star'::quality_class
		when t.avg_rating > 7 then 'good'::quality_class
		when t.avg_rating > 6 then 'average'::quality_class
		else 'bad'::quality_class
	end as quality_class,
	case 
		when t.year is null then false
		else true
	end as is_active,
	1970 as current_year
from yesterday as y 
full outer join today as t 
on y.actorid = t.actorid;


select * from actors;
select * from actors where actor = 'Alain Delon' and current_year = 1970;


--- 3. DDL for actors_history_scd

drop table actors_history_scd;
create table actors_history_scd (
	actorid text,
	actor text,
	is_active bool,
	quality_class quality_class,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	
	primary key (actorid, start_year)
);


--- 4. Incremental query for actors_hisotry_scd
insert into actors_history_scd
with previous_dimension_values as (
	select 
		*,
		lag(is_active, 1) over (partition by actorid order by current_year) as prev_is_active,
		lag(quality_class, 1) over (partition by actorid order by current_year) prev_quality_class
	from actors
	where current_year <= 1976
),
changed_records_indicators as (
	select *,
		case 
			when prev_is_active <> is_active then 1 
			when quality_class <> prev_quality_class then 1
			else 0 
		end as changed_indicator
	from previous_dimension_values 
),
streak_identifier as (
	select 
		*,
		sum(changed_indicator) over (partition by actorid order by current_year) as streak
	from changed_records_indicators
), 
changed_record as (
	select 
		actorid,
		actor,
		is_active,
		quality_class,
		min(current_year) as start_year,
		max(current_year) as end_year,
		1976 as current_year
	from streak_identifier
	group by actorid, actor, is_active, quality_class, streak
	order by actor, start_year
)

select * from changed_record;

-- Select last value of dimensions per each actor
select * from actors_history_scd 
where end_year = 2000

--- 5. Incremental query for actors_history_scd
