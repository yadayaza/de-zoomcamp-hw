## Module 1 Homework for https://github.com/DataTalksClub/data-engineering-zoomcamp

Original file - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/01-docker-terraform/homework.md

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- --delete
- --rc
- --rmc
- `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.

```docker run -it --entrypoint=bash python:3.9```

Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- `0.42.0`
- 1.0.0
- 23.0.1
- 58.1.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

1. `docker-compose.yaml` file contains postgres database and pgadmin services declaration.
2. `ingest_data.py` file contains trips and zones tables creation and filling

````
user@lptp:~$ docker compose up
user@lptp:~$ URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
user@lptp:~$ ZONES_URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
user@lptp:~$ python3 ingest_data.py \
> --user={POSTGRES_USER} \
> --password={POSTGRES_PASSWORD} \
> --host=localhost \
> --port=5432 \
> --db=ny_taxi \
> --table_name=green_taxi_trips \
> --url=${URL} \
> --zones_url=${ZONES_URL}
````


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

There are no "trash" records with day 18 but other month:

```
SELECT 
  *
FROM 
	green_taxi_trips 
WHERE
    EXTRACT(MONTH FROM lpep_pickup_datetime) != 9 AND
	EXTRACT(DAY FROM lpep_pickup_datetime) = 18;
```
So we could count just by using day part of timestamp.
```
SELECT 
  COUNT(1)
FROM 
    green_taxi_trips 
WHERE
    EXTRACT(DAY FROM lpep_pickup_datetime) = 18 AND
    EXTRACT(DAY FROM lpep_dropoff_datetime) = 18;
```

- 15767
- `15612`
- 15859
- 89009

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance. 

```
SELECT 
	CAST(lpep_pickup_datetime AS DATE) as "day"
FROM 
	green_taxi_trips
WHERE
	trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips);
```

- 2019-09-18
- 2019-09-16
- `2019-09-26`
- 2019-09-21


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

```
SELECT 
	"Borough",
	SUM(total_amount) as sum_total
FROM 
	green_taxi_trips t LEFT JOIN zones zpu 
	  ON t."PULocationID" = zpu."LocationID" 
WHERE
	EXTRACT(DAY FROM lpep_pickup_datetime) = 18
GROUP BY 
	"Borough"
ORDER BY 
	sum_total DESC
```
```
"Brooklyn"	96333.23999999923
"Manhattan"	92271.29999999896
"Queens"	78671.70999999886
"Bronx"	32830.089999999924
"Unknown"	728.75
"Staten Island"	342.59000000000003
```
 
- `"Brooklyn" "Manhattan" "Queens"`
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

```
SELECT 
	zdo."Zone" as dropoff_zone,
	MAX(t.tip_amount) as max_tip
FROM 
	green_taxi_trips t LEFT JOIN zones zpu
	ON t."PULocationID" = zpu."LocationID" 
	LEFT JOIN zones zdo 
	ON t."DOLocationID" = zdo."LocationID"
WHERE 
	t."PULocationID" = (SELECT "LocationID" from zones where "Zone" = 'Astoria')
GROUP BY
	dropoff_zone
ORDER BY
	max_tip DESC;
```

```
"JFK Airport"	62.31
"Woodside"	30
"Kips Bay"	28
"NV"	25
"Upper West Side South"	20
"Astoria"	20
```

- Central Park
- Jamaica
- `JFK Airport`
- Long Island City/Queens Plaza



## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

