import os
import json
import time
import shlex
import socket
import shutil
import random
import subprocess
from threading import Thread

import docker

from kwikapi import Client
from basescript import BaseScript
from deeputil import AttrDict, Dummy, keeprunning


class Executor:

    # State of the executor that it is ready for executing a job.
    WREADY = "ready"

    # State of the executor that it is executing a job
    WBUSY = "busy"

    # Executor should always ask job from master at some interval
    JOB_INTERVAL = 10

    def __init__(
        self,
        url=None,
        version=None,
        protocol=None,
        hostname=None,
        external_inp_dir=None,
        external_out_dir=None,
        external_work_dir=None,
        log=Dummy(),
    ):
        super().__init__()

        self.job_id = None
        self.master = Client(url=url, version=version, protocol=protocol)
        self.remote_inp_dir = external_inp_dir
        self.remote_out_dir = external_out_dir
        self.remote_work_dir = external_work_dir
        self.hostname = hostname
        # FIXME:no hard coding for this
        self.docker_tmp_dir = "/tmp/"
        self.state = self.WREADY
        self.progress = 0
        self.docker_client = None
        self.container_id = None
        self.job_state = None
        self.container_stdout = None
        self.container_stderr = None
        self.log_dir = None
        self._state_daemon()
        self._progress_daemon()
        self._get_job_state_daemon()
        self._handle_job_operations_daemon()
        self._get_container_stdout_daemon()
        self._get_container_stderr_daemon()
        self._write_stdout_stderr_daemon()
        self.log = log

    @keeprunning()
    def run(self):
        """
        The executor need to keeprunning and requesting for job.
        If a job is received from master machine it should execute it.
        """
        response = self.request_job()
        params = self.process_response(response)
        self.process_params(params)

    def request_job(self):
        """
        The executor will request for a job and,
        try to receive and process the response from the master.
        """
        time.sleep(self.JOB_INTERVAL)
        self.log.info("request_job")
        job_response = self.master.worker.get_job(name=self.hostname)

        return job_response

    def process_response(self, response):
        """
        After receiving a response from master machine,
        the response has to be processed in a valid format.

        >>> from pprint import pprint
	>>> from executor import Executor
	>>> e = Executor()
	>>> e.state
	'ready'
	>>> response = None
	>>> e.process_response(response)
	>>> e.state
	'ready'
	>>> params = json.dumps({"external":
	...                         {"input":"external_input",
	...                         "output":"external_output",
	...                         "workdir":"external_workdir"},
	...                      "docker":
	...                         {"image":"image",
	...                         "input":"internal_input",
	...                         "arguments":"arguments",
	...                         "output":"internal_output",
	...                          "workdir":"internal_workdir"},
	...                      })
	>>> response = {'name':'name',
	...             'id':'id',
	...             'preserve':'preserve',
	...             'parameters':params}
	>>> pprint(response)
	{'id': 'id',
	 'name': 'name',
	 'parameters': '{"external": {"input": "external_input", "workdir": '
		       '"external_workdir", "output": "external_output"}, "docker": '
		       '{"input": "internal_input", "image": "image", "arguments": '
		       '"arguments", "workdir": "internal_workdir", "output": '
		       '"internal_output"}}',
	 'preserve': 'preserve'}
	>>> pprint(e.process_response(response))
	{'arguments': 'arguments',
	 'docker_cred': '',
	 'docker_inp_dir': 'internal_input',
	 'docker_out_dir': 'internal_output',
	 'docker_work_dir': 'internal_workdir',
	 'environment': {},
	 'image_name': 'image',
	 'job_id': 'id',
	 'job_name': 'name',
	 'preserve': 'preserve',
	 'remote_inp_dir': 'external_input',
	 'remote_out_dir': 'external_output',
	 'remote_work_dir': 'external_workdir'}
        >>> e.state
        'busy'
        """

        if not response:
            self.log.info("no_job", worker_state=self.state)

        else:
            self.log.info("job_received", worker_state=self.state)
            params = self._get_parameters(response)
            self.state = self.WBUSY

            return params

    def _get_parameters(self, response):
        """
        The job paramters have to be extracted out from the response.

	>>> from pprint import pprint
        >>> from executor import Executor
        >>> e = Executor()
        >>> params = json.dumps({"external":{},
        ...                     "docker":
        ...                         {"image":"image",
        ...                         "input":"input",
        ...                         "arguments":"arguments",
        ...                         "output":"output"},
        ...                     })
 	>>> response = {'name':'name',
	...             'id':'id',
	...             'preserve':'preserve',
	...             'parameters':params}
        >>> pprint(response)
	{'id': 'id',
	 'name': 'name',
	 'parameters': '{"external": {}, "docker": {"input": "input", "image": '
		       '"image", "arguments": "arguments", "output": "output"}}',
	 'preserve': 'preserve'}
        >>> response.get('id')
        'id'
      	>>> response.get('name')
        'name'
        >>> response.get('preserve')
        'preserve'
        >>> pprint(response.get('parameters'))
        ('{"external": {}, "docker": {"input": "input", "image": "image", "arguments": '
         '"arguments", "output": "output"}}')
        >>> json.loads(response.get('parameters'))['external']
        {}
        >>> pprint(json.loads(response.get('parameters'))['docker'])
	{'arguments': 'arguments',
	 'image': 'image',
	 'input': 'input',
	 'output': 'output'}
        >>> params = json.dumps({"external":
        ...                         {"input":"external_input",
        ...                         "output":"external_output",
        ...                         "workdir":"external_workdir"},
        ...                     "docker":
        ...                         {"image":"image",
        ...                         "input":"internal_input",
        ...                         "arguments":"arguments",
        ...                         "output":"internal_output",
        ...                         "workdir":"internal_workdir"},
        ...                     })
 	>>> response = {'name':'name',
	...             'id':'id',
	...             'preserve':'preserve',
	...             'parameters':params}
	>>> pprint(e._get_parameters(response))
	{'arguments': 'arguments',
	 'docker_cred': '',
	 'docker_inp_dir': 'internal_input',
	 'docker_out_dir': 'internal_output',
	 'docker_work_dir': 'internal_workdir',
	 'environment': {},
	 'image_name': 'image',
	 'job_id': 'id',
	 'job_name': 'name',
	 'preserve': 'preserve',
	 'remote_inp_dir': 'external_input',
	 'remote_out_dir': 'external_output',
	 'remote_work_dir': 'external_workdir'}
        >>>
        """

        self.log.info("_handle_params", response=response)
        self.job_id = job_id = response.get("id", "")
        job_name = response.get("name", "")
        preserve = response.get("preserve", "")

        external = json.loads(response["parameters"])["external"]
        external_inp_dir = external.get("input", self.remote_inp_dir)
        external_out_dir = external.get("output", self.remote_out_dir)
        external_work_dir = external.get("workdir", self.remote_work_dir)

        docker_params = json.loads(response["parameters"])["docker"]
        docker_inp_dir = docker_params.get("input", "")
        docker_out_dir = docker_params.get("output", "")
        docker_work_dir = docker_params.get("workdir", "")
        image_name = docker_params.get("image", "")
        docker_cred = docker_params.get("credentials", "")
        docker_env = docker_params.get("envs", {})
        docker_args = docker_params.get("arguments", "")
        # FIXME: docker arguments should also contain job_id if some process requires it.
        # FIXME: find a better way to implement it.
        params = {
            "job_id": job_id,
            "job_name": job_name,
            "preserve": preserve,
            "docker_inp_dir": docker_inp_dir,
            "docker_out_dir": docker_out_dir,
            "docker_work_dir": docker_work_dir,
            "image_name": image_name,
            "preserve": preserve,
            "remote_inp_dir": external_inp_dir,
            "remote_out_dir": external_out_dir,
            "remote_work_dir": external_work_dir,
            "docker_cred": docker_cred,
            "environment": docker_env,
            "arguments": docker_args,
        }

        return AttrDict(params)

    def _construct_docker_cmd(self, p):
        """
        Docker image is your actual job, now in order to execute the docker,
        docker command has to be constructed in order to execute it.
        Based on the job paramters docker command will be constructed.

        >>> from pprint import pprint
        >>> from executor import Executor
        >>> e = Executor()
        >>> params = {'arguments': 'arguments',
        ...          'docker_cred': '',
        ...          'docker_inp_dir': 'internal_input',
        ...          'docker_out_dir': 'internal_output',
        ...          'docker_work_dir': 'internal_workdir',
        ...          'environment': {},
        ...          'image_name': 'image',
        ...          'job_id': 'id',
        ...          'job_name': 'name',
        ...          'preserve': 'preserve',
        ...          'remote_inp_dir': 'external_input',
        ...          'remote_out_dir': 'external_output',
        ...          'remote_work_dir': 'external_workdir'}
        >>> params_dict = AttrDict(params)
        >>> image, volumes, argument, tmp_dir = e._construct_docker_cmd(params_dict)
        >>> image
        'image'
        >>> pprint(volumes)
	{'external_input': {'bind': 'internal_input', 'mode': 'rw'},
	 'external_output': {'bind': 'internal_output', 'mode': 'rw'},
	 'external_workdir': {'bind': 'internal_workdir', 'mode': 'rw'}}
        >>> argument
        'arguments'
        >>> # Doctest end, clearing noise
        >>> shutil.rmtree(tmp_dir)
        >>> os.path.exists(tmp_dir)
        False
        """

        self.log_dir = self._create_log_dir(p.remote_out_dir, self.job_id)
        tmp_dir = self._make_tmpdir(p, self.job_id)
        volumes = {
            p.remote_inp_dir: {"bind": p.docker_inp_dir, "mode": "rw"},
            p.remote_out_dir: {"bind": p.docker_out_dir, "mode": "rw"},
            p.remote_work_dir: {"bind": p.docker_work_dir, "mode": "rw"},
        }

        return p.image_name, volumes, p.arguments, tmp_dir

    def process_params(self, params):
        """
        After the paramters are extracted they need to be processed further,
        so that the actual execution of the job can be done.

        >>> from executor import Executor
        >>> e = Executor()
        >>> try:
        ...     e.process_params()
        ... except:
        ...     print('Params Required')
        ...
        Params Required
        """
        image, volumes, docker_cmd, tmp_dir = self._construct_docker_cmd(params)
        self.run_docker(
            params.docker_cred, image, params.environment, volumes, docker_cmd
        )
        self._preserve_tmp_data(params.preserve, tmp_dir, params.remote_out_dir)

    def _docker_login(self, credentials):
        """
        If you have a privately owned docker image it need to be pulled.
        For pulling your image from docker hub a login is required.
        You need to provide docker hub login credentials for that.

        >>> from executor import Executor
        >>> e = Executor()
        >>> credentials = None
        >>> e._docker_login(credentials)
        """

        if credentials:
            self.docker_client = docker.from_env()
            self.docker_client.login(
                username=credentials.username, password=credentials.password
            )

    def run_docker(self, cred, image, env, volumes, cmd):
        """
        Docker image needs to be executed after image is successfully pulled,
        and docker command is contructed based on the job parameters.
        """
        self.log.info("run_docker", state=self.state)
        self._docker_login(cred)
        self.docker_client = docker.from_env()
        self.container_id = self.docker_client.containers.run(
            image,
            environment=env,
            volumes=volumes,
            command=cmd,
            stderr=True,
            detach=True,
        )
        self.container_id.wait()
        self.log.info("Stopping container")
        self.container_id.stop()
        self.log.info("container_stopped")

        self.container_id.remove()

        self.container_id = None
        self.log.info("container_removed")

        self.state = self.WREADY

        self.log.info("docker_done", worker_state=self.state)

    @keeprunning()
    def _get_container_stdout(self):
        if self.container_id:
            self.container_stdout = self.container_id.logs(
                stdout=True, stderr=False
            ).decode("utf-8", "ignore")

    def _get_container_stdout_daemon(self):
        stdout = Thread(target=self._get_container_stdout)
        stdout.daemon = True
        stdout.start()

        return stdout

    @keeprunning()
    def _get_container_stderr(self):
        if self.container_id:
            self.container_stderr = self.container_id.logs(
                stdout=False, stderr=True
            ).decode("utf-8", "ignore")

    def _get_container_stderr_daemon(self):
        stderr = Thread(target=self._get_container_stderr)
        stderr.daemon = True
        stderr.start()

        return stderr

    @keeprunning()
    def _write_stdout_stderr(self):
        if self.container_id:
            stdout_f = open(os.path.join(self.log_dir, "stdout"), "w+")
            stderr_f = open(os.path.join(self.log_dir, "stderr"), "w+")
            if self.container_stdout:
                stdout_f.write(self.container_stdout)
            if self.container_stderr:
                stderr_f.write(self.container_stderr)

    def _write_stdout_stderr_daemon(self):
        write = Thread(target=self._write_stdout_stderr)
        write.daemon = True
        write.start()

        return write

    def _create_log_dir(self, out_dir, job_id):
        log_dir = os.path.join(out_dir, "logs", repr(job_id))
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        return log_dir

    def _get_job_state_daemon(self):
        """
        As the job is being executed their may be chances that state of job is
        changed to following:
            pause
            restart
            cancel
        In these cases it is important to always get the state of the job so that
        operations can be performed

        >>> from executor import Executor
        >>> e = Executor()
        >>> th = e._get_job_state_daemon()
        >>> th.isDaemon()
        True
        >>> th.is_alive()
        True
        """
        job_state = Thread(target=self._get_job_state)
        job_state.daemon = True
        job_state.start()

        return job_state

    @keeprunning()
    def _get_job_state(self):
        """
        Responsible for continuously getting the state of the job which is being executed
        """
        self.job_state = self.master.worker.get_job_state(job_id=self.job_id)

    @keeprunning()
    def _handle_job_operations(self):
        """
        Once the docker container has been started,
        there can be external operations like, pause, restart, cancel;
        which should be handled.
        """

        if self.job_state == "paused":
            self.container_id.pause()

        if self.job_state == "resumed":
            self.container_id.unpause()

        if self.job_state == "cancelled":
            # self.log.info('STOOOOOOOOOOOOPIN')
            self.container_id.kill()
            # self.log.info('STOPPPPPPPPPPPPPPPDDDDDDDDDD')

    def _handle_job_operations_daemon(self):
        job_operation = Thread(target=self._handle_job_operations)
        job_operation.daemon = True
        job_operation.start()

    @keeprunning()
    def _progress(self):
        """
        Responsible for getting progress on your job
        """

        # FIXME: hardcoding
        # FIXME: progress.txt appended with job_id
        progress = os.path.join(
            self.remote_work_dir, "progress.txt" + repr(self.job_id)
        )
        if os.path.exists(progress):
            progress_f = open(progress)
            self._get_progress_update(progress_f)
            progress_f.close()

        # self.log.info("removing progress file", job_state=self.job_state, state=self.state)
        if self.state == self.WREADY or self.job_state == "cancelled":
            os.remove(progress)

    def _get_progress_update(self, f):
        """
        If your job shows a progress it needs to be parsed.

        >>> try:
        ...     e._get_progress_update()
        ... except:
        ...     print('Filepointer Required')
        ...
        Filepointer Required
        """

        prev_update = None

        while self.state != self.WREADY:
            update = f.readline()
            # self.log.info("insidie _get_progress_update", state=self.state, update=update)

            if not update:
                if prev_update:
                    self.log.info("progress_update", cur_update=prev_update)
                    cur_update = prev_update
                    self._update_progress(cur_update)
                    prev_update = None

                continue

            prev_update = update.strip()

    def _update_progress(self, progress):
        """
        After progress is parsed it needs to be sent to the master machine.
        """

        self.master.worker.update_progress(job_id=self.job_id, progress=progress)

    def _make_tmpdir(self, p, job_id):
        """
        If your job request uses a temporary directory for spitting out
        temporary files, these will be stored locally to executor machine.

        >>> import random
        >>> random.seed(1)
        >>> from executor import Executor
        >>> e = Executor()
        >>> params = {'arguments': 'arguments',
        ...  'docker_cred': '',
        ...  'docker_inp_dir': 'internal_input/',
        ...  'docker_out_dir': 'internal_output/',
        ...  'docker_work_dir': 'internal_workdir/',
        ...  'environment': {},
        ...  'image_name': 'image',
        ...  'job_id': 'id',
        ...  'job_name': 'job_name',
        ...  'preserve': 'preserve',
        ...  'remote_inp_dir': 'external_input/',
        ...  'remote_out_dir': 'external_output/',
        ...  'remote_work_dir': 'external_workdir/'}
        >>> params = AttrDict(params)
        >>> job_id = random.randint(0, 9)
        >>> tmp_dir = e._make_tmpdir(params, job_id)
        >>> tmp_dir
        'external_workdir/2job_name'
        >>> # Doctest end, clearing noise
        >>> shutil.rmtree(params.remote_work_dir)
        >>> shutil.rmtree(params.remote_out_dir)
        >>> os.path.exists(params.remote_out_dir)
        False
        >>> os.path.exists(params.remote_work_dir)
        False
        """

        tmp_path = os.path.join(p.remote_work_dir + repr(job_id) + p.job_name)
        if not os.path.exists(tmp_path):
            os.makedirs(tmp_path)

            return tmp_path

    def _preserve_tmp_data(self, preserve, tmp_dir, out_dir):
        """
        As part of the job parameters you need to provide a boolean value,
        which will determine to conserve temporary files spitted out during
        job execution or not, True will result in preserving them in your
        output directory else they will be vanished forever.

        >>> from executor import Executor
        >>> e = Executor()
        >>> tmp_dir = 'temporary_dir/'
        >>> out_dir = 'output_dir/'
        >>> os.mkdir(tmp_dir)
        >>> os.path.exists(tmp_dir)
        True
        >>> os.mkdir(out_dir)
        >>> os.path.exists(out_dir)
        True
        >>> preserve = True
        >>> e._preserve_tmp_data(preserve, tmp_dir, out_dir)
        >>> os.path.exists(tmp_dir)
        False
        >>> os.path.exists(out_dir)
        True
        >>> os.path.exists(out_dir + tmp_dir)
        True
        >>> os.listdir(out_dir)
        ['temporary_dir']
        >>> # Doctest done, cleaning noise
        ...
        >>> shutil.rmtree(out_dir)
        >>> os.path.exists(out_dir)
        False
        >>> os.path.exists(tmp_dir)
        False
        >>> preserve = False
        >>> os.mkdir(tmp_dir)
        >>> os.mkdir(out_dir)
        >>> e._preserve_tmp_data(preserve, tmp_dir, out_dir)
        >>> os.path.exists(tmp_dir)
        False
        >>> os.path.exists(out_dir)
        True
        >>> os.path.exists(out_dir + tmp_dir)
        False
        >>> os.listdir(out_dir)
        []
        >>> # Doctest done, cleaning noise
        ...
        >>> shutil.rmtree(out_dir)
        >>> os.path.exists(out_dir)
        False
        >>>
        """

        if not preserve:
            try:
                shutil.rmtree(tmp_dir)
            except:
                # FIXME: add no file exception
                pass

        else:
            shutil.move(tmp_dir, out_dir)

    @keeprunning()
    def update_state(self):
        """
        Responsible for sending the executor state to master machine,
        irrespective of job being executed or not.
        """

        self.master.worker.update_state(job_id=self.job_id, state=self.state)

    def _state_daemon(self):
        """
        A daemon thread is required to send the state of the executor.
        The state of the executor will change from being ready to busy
        depending on if its executing a job or not.

        >>> from executor import Executor
        >>> e = Executor()
        >>> th = e._state_daemon()
        >>> th.isDaemon()
        True
        >>> th.is_alive()
        True
        """

        state = Thread(target=self.update_state)
        state.daemon = True
        state.start()

        return state

    def _progress_daemon(self):
        """
        As the job is executing if the docker image supports getting
        progress as its executing, this thread will be responsible to
        send the progress to the master machine on a regular basis.

        >>> from executor import Executor
        >>> e = Executor()
        >>> th = e._progress_daemon()
        >>> th.isDaemon()
        True
        >>> th.is_alive()
        True
        """

        progress = Thread(target=self._progress)
        progress.daemon = True
        progress.start()

        return progress
