# Module 1 Homework Solution: Docker & SQL

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- 25.3
- 24.3.1
- 24.2.1
- 23.3.1

```bash
docker run -it --rm --entrypoint=bash python:3.13

pip -V
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```
Correct answer: 25.3

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

### Correct answer: db:5432 
Hostname: db (the service name). Containers in the same docker-compose network communicate using service names. 
Port: 5432 (the internal container port). The mapping '5433:5432' means the host uses 5433, but inside the Docker network containers use 5432.
So pgadmin should connect to db:5432.

## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 7,853
- 8,007
- 8,254
- 8,421

```sql
SELECT COUNT(*)
FROM green_taxi_data
WHERE trip_distance <= 1 AND lpep_pickup_datetime > '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
```
Correct answer: 8007


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- 2025-11-14
- 2025-11-20
- 2025-11-23
- 2025-11-25

```sql
SELECT lpep_pickup_datetime
FROM green_taxi_data
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1
```
Correct answer: 2025-11-14


## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- East Harlem North
- East Harlem South
- Morningside Heights
- Forest Hills

```sql
SELECT l."Zone"
FROM green_taxi_data AS d
JOIN taxi_zone_lookup AS l
ON d."PULocationID" = l."LocationID"
WHERE DATE(d."lpep_pickup_datetime") = '2025-11-18'
GROUP BY l."Zone"
ORDER BY SUM(d."total_amount") DESC
LIMIT 1;
```
Correct answer: East Harlem North

## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- Yorkville West
- East Harlem North
- LaGuardia Airport

```sql
SELECT l_dropoff."Zone" AS dropoff_zone
FROM green_taxi_data AS d
JOIN taxi_zone_lookup AS l_pickup
  ON d."PULocationID" = l_pickup."LocationID"
JOIN taxi_zone_lookup AS l_dropoff
  ON d."DOLocationID" = l_dropoff."LocationID"
WHERE l_pickup."Zone" = 'East Harlem North'
  AND d."lpep_pickup_datetime" >= '2025-11-01'
  AND d."lpep_pickup_datetime" < '2025-12-01'
GROUP BY l_dropoff."Zone"
ORDER BY SUM(d."tip_amount") DESC
LIMIT 1;
```
Correct answer: Yorkville West

## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

Correct answer: terraform init, terraform apply -auto-approve, terraform destroy
