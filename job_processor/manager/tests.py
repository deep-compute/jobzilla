import json
from datetime import datetime

from django.test import TestCase, Client
from django.core.urlresolvers import reverse

import manager
from manager.models import Job, Worker, as_dict
from manager.views import MasterAPI, JobAPI
from manager.validations import get_docker_info
from manager.validations import validate_docker_credentials, validate_docker_image


class Dummy:
    def create_job(
        self,
        name="test_job",
        parameters=dict(parameters="paramters"),
        state="created",
        preserve=True,
        dt_created=datetime.now(),
        dt_updated=datetime.now(),
        progress="progress",
        validated=False,
    ):

        tmp = Job.objects.create(
            name=name,
            parameters=parameters,
            state=state,
            preserve=preserve,
            dt_created=dt_created,
            dt_updated=dt_updated,
            progress=progress,
            validated=validated,
        )
        return tmp

    def create_worker(self, name="worker1", state="state"):

        return Worker.objects.create(name=name, state=state)


class ModelsTest(TestCase, Dummy):
    DUMMY = Dummy()

    def test_as_dict(self):
        item = {"a": 1, "b": 2}
        item_d = as_dict(item)
        self.assertEqual(item, item_d)
        self.assertTrue(isinstance(item_d, dict))

    def test_job_creation(self):
        job = self.DUMMY.create_job()

        self.assertTrue(isinstance(job, Job))

    def test_job_instances(self):
        job = self.DUMMY.create_job()

        self.assertTrue(isinstance(job.id, int))
        self.assertTrue(isinstance(job.name, str))
        self.assertTrue(isinstance(job.parameters, dict))
        self.assertTrue(isinstance(job.state, str))
        self.assertTrue(isinstance(job.preserve, bool))
        self.assertTrue(isinstance(job.dt_created, datetime))
        self.assertTrue(isinstance(job.dt_updated, datetime))
        self.assertTrue(isinstance(job.progress, str))

    def test_worker_creation(self):
        worker = self.DUMMY.create_worker()

        self.assertTrue(isinstance(worker, Worker))

    def test_worker_instances(self):
        worker = self.DUMMY.create_worker()

        self.assertTrue(isinstance(worker.id, int))
        self.assertTrue(isinstance(worker.job_id, (int, type(None))))
        self.assertTrue(isinstance(worker.name, str))
        self.assertTrue(isinstance(worker.state, str))


class ViewsTest(TestCase, Dummy):
    DUMMY = Dummy()

    def _process_response(self, resp):
        resp_content = json.loads(resp.content.decode("utf-8"))
        success = resp_content.get("success", False)

        return success

    def test_request_job(self):
        # FIXME: 1) There is a better way using reverse
        url = "/api/v1/worker/get_job?name=test_name"
        client = Client()
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_worker_update_state(self):
        # FIXME: *1
        url = "/api/v1/worker/update_state?job_id=1&state=state"
        client = Client()
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_worker_update_progress(self):
        # FIXME: *1
        url = "/api/v1/worker/update_progress?job_id=1&progress=progress"
        client = Client()
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_job_submit_job(self):
        # FIXME: *1
        url = '/api/v1/job/submit?name=name&params={"parameters":"paramters"}&preserve=True'
        client = Client()
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_job_info(self):
        # FIXME: *1
        url = "/api/v1/job/info?job_id=1"
        client = Client()
        resp = client.get(url)

        self.assertEqual(resp.status_code, 200)

    def test_job_pause(self):
        client = Client()
        job_url = "/api/v1/job/submit?name=test&params={}"
        resp = client.get(job_url)
        url = "/api/v1/job/pause?job_id={}".format(resp.json().get("result"))
        client = Client()
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_job_resume(self):
        client = Client()
        job = "/api/v1/job/submit?name=test&params={}"
        resp = client.get(job)

        url = "/api/v1/job/resume?job_id={}".format(resp.json().get("result"))
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_job_cancel(self):

        client = Client()
        create_url = "/api/v1/job/submit?name=test&params={}"
        resp = client.get(create_url)
        url = "/api/v1/job/cancel?job_id={}".format(resp.json().get("result"))
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)

    def test_get_job_state(self):
        client = Client()
        job = "/api/v1/job/submit?name=test&params={}"
        resp = client.get(job)
        url = "/api/v1/worker/get_job_state?job_id={}".format(resp.json().get("result"))
        resp = client.get(url)
        success = self._process_response(resp)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(success, True)


class ValidationTests(TestCase):
    def test_get_docker_info(self):
        params = dict(parameters="paramaters")
        credentials, image = get_docker_info(params)

        self.assertTrue(isinstance(params, dict))
        self.assertTrue(isinstance(credentials, type(None)))
        self.assertTrue(isinstance(image, type(None)))

        params = {
            "docker": {
                "credentials": {"username": "xyz", "password": "zxc"},
                "image": "img",
            }
        }
        credentials, image = get_docker_info(params)

        self.assertTrue(isinstance(params, dict))
        self.assertTrue(isinstance(credentials, dict))
        self.assertTrue(isinstance(image, str))

    def test_validate_docker_credentials(self):
        credentials = None
        res = validate_docker_credentials(credentials)

        self.assertTrue(isinstance(res, type(None)))

        credentials = {"username": "xyz", "password": "zxc"}
        with self.assertRaises(manager.exception.InvalidDockerCredentials) as ex:
            validate_docker_credentials(credentials)

        self.assertTrue(
            "unauthorized: incorrect username or password" in str(ex.exception)
        )
        self.assertTrue("401 Client Error" in str(ex.exception.error))

    def test_validate_docker_image(self):
        image = None
        with self.assertRaises(manager.exception.NullImage) as ni:
            validate_docker_image(image)

        self.assertTrue("Resource ID was not provided" in str(ni.exception))
        self.assertTrue("Resource ID was not provided" in str(ni.exception.error))

        image = "xyz"
        with self.assertRaises(manager.exception.InvalidDockerImage) as idi:
            validate_docker_image(image)
