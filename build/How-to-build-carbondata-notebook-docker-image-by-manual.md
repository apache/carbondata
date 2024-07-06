This documentation will describe how to build carbondata notebook docker images.

## Preparing
It should already installed docker environment.

## Downloading docker images of spark notebook 
CarbonData notebook docker images is based on spark notebook images.

Downloading latest version of docker images of spark notebook 

```shell
docker pull jupyter/all-spark-notebook:latest
```

Downloading specify version of docker images of spark notebook 

```shell
docker pull jupyter/all-spark-notebook:spark-3.4.0
```

Refer to https://hub.docker.com/r/jupyter/all-spark-notebook/tags


## Running the docker images of spark notebook

```
docker run -d -p 8888:8888 --restart always jupyter/all-spark-notebook:latest
```

## Add CarbonData jar to spark notebook images
Get container if of spark notebook container.

```shell
localhost:carbondata xubo$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED          STATUS          PORTS                                       NAMES
305f1277690c   xubo245/all-carbondata-notebook:latest   "tini -g -- start-no…"   15 minutes ago   Up 15 minutes   0.0.0.0:8888->8888/tcp, :::8888->8888/tcp   youthful_wright
```
Add carbondata jar to spark jars directory:
```
docker cp apache-carbondata-2.3.1-bin-spark3.1.1-hadoop2.7.2.jar  305f1277690c:/usr/local/spark-3.1.1-bin-hadoop3.2/jars/
```

The apache-carbondata-2.3.1-bin-spark3.1.1-hadoop2.7.2.jar is from assembly/target/scala-2.12/ directory of carbondata project.
You can build jar by maven, For example:

```
mvn -DskipTests -Pspark-3.1 clean package -Pbuild-with-format
```

## Create carbondata notebook docker images
Save the spark notebook docker images with carbondata jar to carbondata notebook image.

```
docker commit 305f1277690c xubo245/all-carbondata-notebook:latest
```
 
## Opening the notebook 

Please refer to [Using CarbonData in notebook](#../docs/using-carbondata-in-notebook.md)  