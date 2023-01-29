## Week 1 Homework
Done by : Abdelrahman Amer

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string` &#x2611;
- `--idimage string` 
- `--idfile string`

```sh
docker run --help
```


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3 &#x2611;
- 7

```sh
Docker run -it —entrypoint=bash python:3.9
pip list
```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530 &#x2611;
- 17630
- 21090

```sql
SELECT count(lpep_pickup_datetime) FROM public.green_taxi_trips t
where date(lpep_pickup_datetime) = date'2019-01-15' and date(lpep_dropoff_datetime) = date'2019-01-15'
```

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15 &#x2611;
- 2019-01-10

```sql
SELECT lpep_pickup_datetime as "pickup",lpep_dropoff_datetime dropoff,* FROM public.green_taxi_trips t
order by trip_distance desc
limit 1;
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254 &#x2611;
- 2: 1282 ; 3: 274
## code
```sql
SELECT lpep_pickup_datetime as "pickup",lpep_dropoff_datetime dropoff, passenger_count,* FROM public.green_taxi_trips t
where date(lpep_pickup_datetime) = date'2019-01-1' and (passenger_count = 2 );


SELECT lpep_pickup_datetime as "pickup",lpep_dropoff_datetime dropoff, passenger_count,* FROM public.green_taxi_trips t
where date(lpep_pickup_datetime) = date'2019-01-1' and (passenger_count = 3 )
```


## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza

```sql
select trips."PULocationID",tip_amount,trips."DOLocationID" ,zones."Zone" as "pickup",zdo."Zone" as "dropoff" from public.green_taxi_trips trips 
left join zones 
on "PULocationID" = zones."LocationID"
left join zones zdo
on "DOLocationID" = zdo."LocationID"
where zones."Zone" = 'Astoria'
order by tip_amount desc
limit 1;
```


