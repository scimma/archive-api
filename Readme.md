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

## Docker Image

For production deployment, a Docker image can be built by using the `container` make target (which is also built by the default make target). 