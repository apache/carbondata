This documentation will describe how to build carbondata notebook docker images.

## Preparing
It should already installed docker environment.


## Build carbondata notebook docker images by dockerfile

go to directory of dockerfile：
```
cd build/docker/carbondata-notebook
```
build image：
```
docker build -t carbondata:2.3.1  .
```

```history
localhost:carbondata-notebook xubo$ docker build -t carbondata:2.3.1  .
[+] Building 20.2s (7/7) FINISHED                                                                                                                                                                         
 => [internal] load build definition from Dockerfile                                                                                                                                                 0.0s
 => => transferring dockerfile: 260B                                                                                                                                                                 0.0s
 => [internal] load .dockerignore                                                                                                                                                                    0.0s
 => => transferring context: 2B                                                                                                                                                                      0.0s
 => [internal] load metadata for docker.io/jupyter/all-spark-notebook:spark-3.1.1                                                                                                                    0.0s
 => [1/3] FROM docker.io/jupyter/all-spark-notebook:spark-3.1.1                                                                                                                                      0.0s
 => CACHED [2/3] WORKDIR .                                                                                                                                                                           0.0s
 => [3/3] RUN wget https://dlcdn.apache.org/carbondata/2.3.0/apache-carbondata-2.3.0-bin-spark3.1.1-hadoop2.7.2.jar -P /usr/local/spark-3.1.1-bin-hadoop3.2/jars/                                   19.4s
 => exporting to image                                                                                                                                                                               0.6s
 => => exporting layers                                                                                                                                                                              0.6s
 => => writing image sha256:25c5b977aaa5e2404ce720b2f5784dfb9b16921e8bb2cab004e9e50fc0631c2c                                                                                                         0.0s 
 => => naming to docker.io/library/carbondata:2.3.1                                                                                                                                                  0.0s 
localhost:carbondata-notebook xubo$ pwd                                                                                                                                                                   
/Users/xubo/Desktop/xubo/git/carbondata1/build/docker/carbondata-notebook                                                                                                                                 
localhost:carbondata-notebook xubo$ docker images |grep carbondata
carbondata                           2.3.1                                                   25c5b977aaa5   About a minute ago   4.22GB
carbondata                           2.3.0                                                   2307a01af387   2 minutes ago        4.22GB
```
 
## Opening the notebook 

Please refer to [Using CarbonData in notebook](#../docs/using-carbondata-in-notebook.md)  