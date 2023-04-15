## Preparing
It should already installed docker environment before using carbondata in notebook.

## Downloading docker images of carbondata notebook 

Downloading latest version of docker images of carbondata notebook 

```shell
docker pull xubo245/all-carbondata-notebook:latest
```

Downloading specify version of docker images of carbondata notebook 

```shell
docker pull xubo245/all-carbondata-notebook:carbondata-2.3.0-spark3.1.1-hadoop2.7.2-v1
```

Refer to https://hub.docker.com/repository/docker/xubo245/all-carbondata-notebook/tags?page=1&ordering=last_updated
## Running the docker images of carbondata notebook

```
docker run -d -p 8888:8888 --restart always xubo245/all-carbondata-notebook:latest
```

## Opening the notebook 
Command:
```
docker ps |grep carbondata
docker logs <container_id>
```
You can get the notebook address in the last line.
  
For example:
```shell
localhost:carbondata xubo$ docker ps |grep carbondata
8a57e6fed80e   xubo245/all-carbondata-notebook:latest   "tini -g -- start-no…"   11 seconds ago   Up 8 seconds   0.0.0.0:8888->8888/tcp, :::8888->8888/tcp   affectionate_bohr
localhost:carbondata xubo$ docker logs 8a57e6fed80e
WARN: Jupyter Notebook deprecation notice https://github.com/jupyter/docker-stacks#jupyter-notebook-deprecation-notice.
/usr/local/bin/start-notebook.sh: running hooks in /usr/local/bin/before-notebook.d
/usr/local/bin/start-notebook.sh: running /usr/local/bin/before-notebook.d/spark-config.sh
/usr/local/bin/start-notebook.sh: done running hooks in /usr/local/bin/before-notebook.d
Executing the command: jupyter notebook
[I 17:24:50.960 NotebookApp] Loading IPython parallel extension
[W 2023-04-12 17:24:51.833 LabApp] 'ip' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
[W 2023-04-12 17:24:51.833 LabApp] 'port' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
[W 2023-04-12 17:24:51.833 LabApp] 'port' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
[W 2023-04-12 17:24:51.833 LabApp] 'port' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
[I 2023-04-12 17:24:51.842 LabApp] JupyterLab extension loaded from /opt/conda/lib/python3.9/site-packages/jupyterlab
[I 2023-04-12 17:24:51.842 LabApp] JupyterLab application directory is /opt/conda/share/jupyter/lab
[I 17:24:51.849 NotebookApp] Serving notebooks from local directory: /home/jovyan
[I 17:24:51.849 NotebookApp] Jupyter Notebook 6.4.0 is running at:
[I 17:24:51.849 NotebookApp] http://8a57e6fed80e:8888/?token=f2f24cd38ddb1d2e11d8dd09ab27a2062dca66efbc50c75c
[I 17:24:51.849 NotebookApp]  or http://127.0.0.1:8888/?token=f2f24cd38ddb1d2e11d8dd09ab27a2062dca66efbc50c75c
[I 17:24:51.849 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 17:24:51.853 NotebookApp] 
    
    To access the notebook, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/nbserver-8-open.html
    Or copy and paste one of these URLs:
        http://8a57e6fed80e:8888/?token=f2f24cd38ddb1d2e11d8dd09ab27a2062dca66efbc50c75c
     or http://127.0.0.1:8888/?token=f2f24cd38ddb1d2e11d8dd09ab27a2062dca66efbc50c75c
```

Then you can open the notebook by put the notebook address to browser:
```
http://127.0.0.1:8888/?token=f2f24cd38ddb1d2e11d8dd09ab27a2062dca66efbc50c75c
```

## Using carbondata in notebook:
Opening the carbondata_notebook_with_visualization.ipynb

![File Directory Structure](../docs/images/using-carbondata-in-notebook-visualization-0.png?raw=true)

You also can open this file from notebook directory：[carbondata_notebook_with_visualization.ipynb](#notebook/carbondata_notebook_with_visualization.ipynb)

Running carbondata example to visualization in notebook file:

![File Directory Structure](../docs/images/using-carbondata-in-notebook-visualization-1.png?raw=true)
![File Directory Structure](../docs/images/using-carbondata-in-notebook-visualization-2.png?raw=true)
![File Directory Structure](../docs/images/using-carbondata-in-notebook-visualization-3.png?raw=true)
