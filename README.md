# inpe_stac

STAC (SpatioTemporal Asset Catalog) implementation for INPE Catalog.


## Run using a Python virtual environment

Install a specific Python version using pyenv:

```
pyenv install 3.7.0
```

Create a Python environment with the Python version above through pyenv:

```
pyenv virtualenv 3.7.0 inpe_stac
```

Activate the virtual environment:

```
pyenv activate inpe_stac
```

Install the requirements:

```
pip install -r requirements.txt
```

Export the environment variables:

```
set -a && source environment.dev.env && set +a
```

Run the service:

```
flask run --host=0.0.0.0
```


## Run using a Docker image

Build a new image using the Dockerfile (development or production):

```
docker build -t inpe_stac_dev:0.0.2 -f dev.Dockerfile . --no-cache
docker build -t registry.dpi.inpe.br/dgi/inpe_stac:0.0.2 -f prod.Dockerfile . --no-cache
```

Inside `docker-compose.yml` file there are two services (development or production), choose which one you like to run. Then, run the Docker compose:

```
docker-compose -f docker-compose.yml up
```

If you have credentials, then push the image to your registry:

```
docker push registry.dpi.inpe.br/dgi/inpe_stac:0.0.2
```
