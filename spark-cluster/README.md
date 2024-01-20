# Spark Cluster with Docker & docker-compose

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the images

The first step to deploy the cluster will be the build of the custom images, these builds can be performed with the *build-images.sh* script. 

The executions is as simple as the following steps:

```sh
chmod +x build-images.sh
./build-images.sh
```

This will create the following docker images:

* spark-base
* spark-master
* spark-worker

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker-compose up --scale spark-worker=3
```

# Resource Allocation 

This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(basically RAM & cpu cores allocation).

* The default CPU cores allocation for each spark worker is 1 core.

* The default RAM for each spark-worker is 1024 MB.

* The default RAM allocation for spark executors is 256mb.

* The default RAM allocation for spark driver is 128mb

* If you wish to modify this allocations just edit the env/spark-worker.sh file.

# Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount| Container Mount        |Purposse
---|------------------------|---
./spark-cluster/apps| /opt/spark-apps        |Used to make available your app's jars on all workers & master
./spark-cluster/data| /opt/spark-data        | Used to make available your app's data on all workers & master
./spark-cluster/data/output| /opt/spark-data/output | Used to make available your app's output data

# Running a sample application

Now let`s make the **spark submit** to run a sample application.
But, before to do that we need to copy our app's jar and dependencies into the workers and master.

### Building the application

```sh
mvn clean package
```

### Copying jars and files on the Workers and Master

```bash
#Copy spark application into nodes
cp ./target/app.jar ./spark-cluster/apps

# Copy the file to be processed to all workers's data folder
cp my/path/*.csv ./spark-cluster/data/input
```

### Check the successful copy of the data and app jar (Optional)

```sh
# master  Validations
docker exec -ti spark-cluster-spark-master-1 ls -l /opt/spark-apps
docker exec -ti spark-cluster-spark-master-1 ls -l /opt/spark-data

# Worker 1 Validations
docker exec -ti spark-cluster-spark-worker-1 ls -l /opt/spark-apps
docker exec -ti spark-cluster-spark-worker-1 ls -l /opt/spark-data

# Worker 2 Validations
docker exec -ti spark-cluster-spark-worker-2 ls -l /opt/spark-apps
docker exec -ti spark-cluster-spark-worker-2 ls -l /opt/spark-data

# Worker 3 Validations
docker exec -ti spark-cluster-spark-worker-3 ls -l /opt/spark-apps
docker exec -ti spark-cluster-spark-worker-3 ls -l /opt/spark-data
```
After running one of this commands you have to see your app's jar and files.

### Submitting the application

```sh
docker exec -ti spark-cluster-spark-master-1 spark-submit \ 
--class io.andrelucas.ClusterDeployApp \
--deploy-mode client \
--master spark://spark-master:7077 \
--verbose \
--supervise /opt/spark-apps/app.jar
```

### Check the output files

The output files will be available on the following path: **spark-cluster/data/output**

# Monitoring the cluster

To monitor the cluster you can use the spark web ui, this is available on the following url: http://localhost:8080

