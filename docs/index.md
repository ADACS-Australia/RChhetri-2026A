# Needle

--8<-- "README.md:intro"

## Quickstart

- From the project root, install Needle with pip:

```bash
pip install .
```

- Create the containers with Docker:

```bash
make needle
```

- Spin up the Prefect compose stack:

```bash
docker compose -f container/docker-compose.yaml up -d
```

- Write a minimal [config file](./configuration.md#minimal-configuration) to your home directory at `$HOME/.needle.yaml`

- Start the pipeline and watcher service:

```bash
needle-serve
```

- Move or copy a directory of target and calibrator observations to the source directory:

```bash
mv my_observation  /path/to/data/source/directory
```

The watcher will find the files and trigger the courier and pipeline to start.
You can monitor the watcher from the command line and everything else is visible from the Prefect UI at <localhost:4200>
