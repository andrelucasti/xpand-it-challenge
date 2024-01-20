# Xpand IT Interview Challenge

### Pre-requisites:

- Java 11
- Maven 3.6.3
- Docker
- Docker Compose

## Run locally

to run locally you can run using docker containers or using your local machine
### Run using your local machine
Run the following command to build the project
```shell

export GOOGLE_PLAY_STORE_PATH=googleplaystore.csv
export GOOGLE_PLAY_STORE_USER_REVIEWS_PATH=googleplaystore_user_reviews.csv
export OUTPUT_PATH=output/

mvn clean package
cd target
java -jar app.jar 
```
#### Run using docker containers
[Running locally with docker container](local/README.md)

### Running using Spark Cluster Standalone
[Running using Spark Cluster Standalone](spark-cluster/README.md)

## Run tests
Has been implemented some unit/integration tests. 
To run tests you can run the following command

```shell
mvn clean test
```