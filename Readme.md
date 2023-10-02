# Hopskotch Archive REST API

This repository contains the REST API for the Hopskotch long-term data archive. 

At this time, only basic operations are supported:
- Fetching a message by UUID
- Fetching messages published to a topic within a range of times
- Storing a message directly

## Running

[`uvicorn`](https://www.uvicorn.org) is currently the default webserver used to run the API:

	uvicorn --app-dir scripts archive_api:app

The API script inherits all of the configuration options of the [archive core](https://github.com/scimma/archive-core). Additionally, it accepts the `--hop-auth-api-root` option (or equivalent `HOP_AUTH_API_ROOT` environment variable) to define the address at which it should expect to be able to contact the hop-auth API to check user authorization. 

Settings can be specified in a TOML configuration file, the path to which can itself be specified using the `CONFIG_FILE` environemtn variable. Thus, it is frequently convenient to run the server along the lines of:

	CONFIG_FILE=config.toml uvicorn --app-dir scripts archive_api:app

### Local testing in Python virtual environment

```bash
$ python3.9 -m venv venv
$ . venv/bin/activate
(venv)
$ python -m pip install -r requirements.txt 
$ CONFIG_FILE=config.toml uvicorn --app-dir=./scripts/ --port 8888 archive_api:app

Loading configuration from config.toml
2023-09-29 13:40:23,040:utility_api.py:INFO:Basic logging is configured at INFO
2023-09-29 13:40:23,040:archive_api.py:INFO:{'config_file': None, 'log_level': 'INFO', 'log_format': '%(asctime)s:%(filename)s:%(levelname)s:%(message)s', 'db_type': 'mock', 'db_host': None, 'db_port': None, 'db_name': None, 'db_username': None, 'db_log_frequency': 100, 'db_aws_secret_name': None, 'db_aws_region': 'us-west-2', 'store_type': 'S3', 'store_primary_bucket': 'hopskotch-archive', 'store_backup_bucket': 'hopskotch-archive-backup', 'store_endpoint_url': None, 'store_region_name': None, 'store_log_every': 100, 'read_only': False, 'hop_auth_api_root': None}
2023-09-29 13:40:23,040:database_api.py:INFO:Mock Database configured
INFO:     Started server process [49466]
INFO:     Waiting for application startup.
2023-09-29 13:40:23,045:archive_api.py:INFO:Connecting to archive
2023-09-29 13:40:25,363:archive_api.py:INFO:Startup complete
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8888 (Press CTRL+C to quit)
```

### Local testing in Docker Compose

Build and deploy a set of services using Docker Compose by running

```bash
docker compose up --build -d
```

This will launch

- the archive API webserver (http://localhost:8000)
- the archive ingest script
- an instance of MinIO for S3-compatible object storage for the archive data storage (http://localhost:9001/browser)
- a PostgreSQL database for the archive metadata

By default you will need to provide Hopskotch credentials HOP_USERNAME and HOP_PASSWORD in a `.env` file as illustrated in the `env.default` file. Any environment variables defined in `.env` will override the values specified in `env.default`.

## Docker Image

For production deployment, a Docker image can be built by using the `container` make target (which is also built by the default make target). 
