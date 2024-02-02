# py-elt

## Description

This is a simple project to run PySpark scripts in a Docker container. It uses a `docker-compose` file to create a Spark cluster with one master and two workers. The `Makefile` contains commands to upload files, submit and run scripts.
The `scripts/` folder contains the PySpark scripts, and the `files/` folder contains the files to be uploaded to the container.

***This project does not require to have Spark or Java installed in the host machine, the `requirements.txt` contains the packages used, just for linting and testing.***

## Requirements

- Python ^3.11
- Docker ^24.0.7
- Make ^3.81

## Running an example script and pulling the output files

- Run a basic script
```bash
docker compose up --build -d && make upload FILE=example.csv && make submit-run SCRIPT=example.py
```

- Pull the output file to the host machine (expecting the output files from the example script)
```bash
make pull
```
## Makefile commands
- `make upload FILE=example.csv` - Upload a file to the spark-master container
- `make submit-run SCRIPT=example.py` - Submit a script and run it
- `make pull` - Pull the output files **(This only works with the example.py script)**
- `make run SCRIPT=example.py` - Run a script
- `make submit SCRIPT=example.py` - Submit a script

## Known issues

- The most current Pandas library outputs a "waring message" when using the `to_datetime` method, this is not something that will break the script in the future, but it does add to the execution time (due to the stdout output). This can be solved by using a version below `2.0.0``, but it will make things run slower


