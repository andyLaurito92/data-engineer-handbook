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
