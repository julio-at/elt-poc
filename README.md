# py-elt

## Description

This is a simple project to run PySpark scripts in a Docker container. It uses a `docker-compose` file to create a Spark cluster with one master and two workers. 

The `Makefile` contains commands to upload files, submit and run scripts.

The `scripts/` folder contains the PySpark scripts, and the `input_files/` folder contains the files to be uploaded to the container.

The `output/` directory is the destination of the resulting files.

```
.
├── .pre-commit.yaml # To prevent commits without certain ruling
├── Makefile # Automate commands & env usage
├── README.md 
├── input_files
│   ├── (*.csv)

├── output
│   ├── (*.csv)
├── requirements.txt # can be used with pipenv / pip3
├── scripts
│   ├── process.py
│   ├── show.py
├── settings.py # project contants configs
├── compose.yml # docker manifest for the containers
├── master.Dockerfile # master Spark image
└── worker.Dockerfile # spark node image

```

***This project does not require to have Spark / Java installed in the host machine, the `requirements.txt` contains the packages used, just for linting and testing.***

## Requirements

- Python ^3.11
- Docker ^24.0.7
- Make ^3.81 

- Build the Docker containers
```bash
docker compose up --build -d

## Running an example script and pulling the output files

- Run the main process script
```bash
make upload FILE=example_data.csv && make submit-run SCRIPT=process.py
```

- Pull the output file to the host machine (expecting the output files from the example script)
```bash
make pull
```


## Makefile commands
- `make upload FILE=example_data.csv` - Upload a file to the spark-master container
- `make submit-run SCRIPT=process.py` - Submit a script and run it
- `make pull OUTPUT=output_results` - Pull the output files **(This only works with the process.py pre-executed!)**

- `make run SCRIPT=process.py` - Run a script
- `make submit SCRIPT=process.py` - Submit a script

## Known issues

- The most current Pandas library outputs a "waring message" when using the `to_datetime` method, this is not something that will break the script in the future, but it does add to the execution time (due to the stdout output). This can be solved by using a version below `2.0.0``, but it will make things run slower


