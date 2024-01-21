# Running the application locally with Docker

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the images

To build the images just run the following command:

```sh
cd local
docker-compose build
```

# Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount| Container Mount |Purposse
---|-----------------|---
./data/input| /data/input     | Used to make available your app's data on container
./data/output| /data/output   | Used to make available your app's output data

## Run the docker-compose

To run the docker-compose just run the following command:

```sh
docker-compose up
```
the application will run after command above 
