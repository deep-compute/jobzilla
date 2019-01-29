import json
import subprocess
import shlex

import docker
from docker import from_env as client

from .exception import InvalidDockerImage, InvalidDockerCredentials, NullImage

client = client()

def get_docker_info(job_parameters):
    docker_params = job_parameters.get('docker')
    if docker_params:
        docker_cred = docker_params.get('credentials', '')
        docker_image = docker_params.get('image', '')

        return docker_cred, docker_image

    return None, None

def validate_docker_credentials(credentials):
    if credentials:
        try:
            client.login(username=credentials['username'], password=credentials['password'])

        except docker.errors.APIError as error:
            raise InvalidDockerCredentials(error)

def validate_docker_image(name):
    try:
        client.images.get(name)

    except docker.errors.ImageNotFound as inf:
        raise InvalidDockerImage(inf)

    except docker.errors.NullResource as nres:
        raise NullImage(nres)
