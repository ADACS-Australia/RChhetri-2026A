FROM prefecthq/prefect:3-latest
RUN pip install prefect-dask dask distributed prefect-docker
