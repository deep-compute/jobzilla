import json
import django
from django.http import HttpResponse

import redis_lock
from redis import StrictRedis
from kwikapi import API, Request
from basescript import init_logger

from django.conf import settings
from .models import Job, Worker, as_dict
from .exception import EntryExistsError, NoEntryExistsError
from .exception import InvalidDockerCredentials, InvalidDockerImage
from .validations import get_docker_info
from .validations import validate_docker_credentials, validate_docker_image

J = Job.objects
W = Worker.objects

JCREATED = 'created'
JSUBMITTED = 'submitted'
JPAUSED = 'paused'
JRESUMED = 'resumed'
JCANCELLED = 'cancelled'
JCOMPLETED = 'completed'

WREADY = 'ready'
WBUSY = 'busy'

# FIXME: to be part of local_setting.py
REDIS_PASSWORD = settings.REDIS_PASSWORD
REDIS_PORT = settings.REDIS_PORT
REDIS_HOST = settings.REDIS_HOST
REDIS_DB = settings.REDIS_DB

LOG = init_logger(fmt=None, fpath=None, pre_hooks=[], post_hooks=[], metric_grouping_interval=1, level='warning')

class JobAPI:

    def __init__(self, log):
        self.master = MasterAPI(log=log)
        self.log = log

    def submit(self, name: str, params: dict, preserve: bool=False)-> int:
        """
        For receiving a job request

        :param: name: name of the job
        :param type: str

        :param: params: parameters neccessary for executing a job
        :param type: dict

        :param: preserve: whether to preserve temporary file
        :param type: bool

        :returns: Id of the job when successfully received
        :return type: int
        """

        credentials, image = get_docker_info(params)
        if credentials:
            validate_docker_credentials(credentials)
            validate_docker_image(image)

        j = J.create(name=name, state=JCREATED, preserve=preserve,
                parameters=json.dumps(params))

        j.validated = True
        j.save()

        return j.id

    def info(self, req: Request, job_id: int)-> dict:
        """
        Get the information about the job which is submitted

        :param: job_id: Id of the job submitted
        :param type: int

        :returns: Job information
        :return type: dict

        :raises: NoEntryExistsError: if job id doesn't exists
        """

        j = J.filter(id=job_id).values().first()
        if j:
            return as_dict(j)

    def get_progress(self, job_id: int)-> int:
        """
        Get the progress of a executing job

        :param: job_id: job id that being executed
        :param type: int

        :returns: job progress
        :return type: int
        """

        progress = J.filter(id=job_id).values().first()['progress']

        return progress

    def pause(self, job_id: int)-> None:
        """
        Pause job's execution

        :param: job_id: job id to be paused
        :param type: int
        """

        self.master._change_job_state(job_id, JPAUSED)

    def resume(self, job_id: int)-> None:
        """
        Resume job's execution

        :param: job_id: job id to be paused
        :param type: int
        """

        self.master._change_job_state(job_id, JRESUMED)

    def cancel(self, job_id: int)-> None:
        """
        Cancel job's execution

        :param: job_id: job id to be paused
        :param type: int
        """
        self.master._change_job_state(job_id, JCANCELLED)

class MasterAPI:

    def __init__(self, log):
        self.log = log
        self.redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

    def _change_job_state(self, job_id, state):
        """
        Responsible to contacting the worker for job operations.
        E.g pause/stop/restart a job

        :param: job_id: job id that executor is executing
        :param type: int

        :param: state: state of the executor machine
        :param type: str
        """

        j = J.get(id=job_id)

        if not j:
            raise NoEntryExistsError(job_id)

        j.state = state

        j.save()

    def get_job_state(self, job_id: int)-> str:
        """
        Responsible for giving job state

        :param: job_id: job id that is being executed
        :param type: int

        :returns: job's state
        :return type: str
        """
        try:
            j = J.get(id=job_id)
            job_state = j.state

            return job_state

        except:
            self.log.warn('State does not exists of given job id')

    def _register(self, name):
        try:
            w = W.create(name=name)
            w.save()

        except django.db.utils.IntegrityError:
            self.log.warn('Worker Already Registered')

    def get_job(self, name: str)-> dict:
        """
        If job is in the queue, a job will be allocated to a executor machine

        :param: name: hostname of the executor machine
        :param type: str

        :returns: Job information
        :return type: dict
        """
        with redis_lock.Lock(self.redis_conn, "job_lock"):
            self._register(name)
            j = J.filter(state=JCREATED).filter(validated=True).values().first()
            if j:
                W.filter(name=name).update(job_id=j['id'])

                J.filter(id=j['id']).update(state=JSUBMITTED)

                return as_dict(j)

    def update_state(self, job_id: int, state: str)-> None:
        """
        Will update the state of the executor machine

        :param: job_id: job id that the executor is executing
        :param type: int

        :param: state: state of the executor machine
        :param type: str
        """
        w = W.filter(job_id=job_id).update(state=state)

        if state == WREADY:
            try:
                job_state = J.filter(id=job_id).filter(state=JCANCELLED).values().first()
            except:
                self.log.warn('JOB STATE IS NONE', job_state=job_state)

            if job_state:
                W.filter(job_id=job_id).update(job_id=None)
            else:
                J.filter(id=job_id).update(state=JCOMPLETED)
                W.filter(job_id=job_id).update(job_id=None)

    def update_progress(self, job_id: int, progress: str)-> None:
        """
        Will update the job progress in db

        :param: job_id: job id that executor is executing
        :param type: int

        :param: progress: progress of the job execution
        :param type: str
        """

        j = J.filter(id=job_id).update(progress=progress)

api = API()
api.register(JobAPI(log=LOG), 'v1', 'job')
api.register(MasterAPI(log=LOG), 'v1', 'worker')
