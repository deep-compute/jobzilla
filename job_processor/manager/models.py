import json

from django.db import models
from django.core.serializers.json import DjangoJSONEncoder

from .exception import NoEntryExistsError

JOB_STATE_CHOICES = (
                        ('created', 'created'),
                        ('submitted', 'submitted'),
                        ('completed', 'completed'),
                        ('paused', 'paused'),
                        ('cancelled', 'cancelled'),
                        ('resumed', 'resumed'),
                    )

WORKER_STATE_CHOICES = (
                            ('ready', 'ready'),
                            ('busy', 'busy'),
                        )

def as_dict(item):
    """
    Makes items json serializable
    """

    if not item:
        raise NoEntryExistsError(item)

    data = json.loads(json.dumps(item, sort_keys=True, cls=DjangoJSONEncoder))

    return data

class Job(models.Model):
    """
    Job's model definition
    """

    name = models.CharField(max_length=255)
    parameters = models.TextField()
    state = models.CharField(max_length=255, choices=JOB_STATE_CHOICES)
    preserve = models.BooleanField(default=False)
    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)
    progress = models.TextField()
    validated = models.BooleanField(default=False)

class Worker(models.Model):
    """
    Worker's model definition
    """

    job = models.ForeignKey(Job, on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=255, unique=True)
    state = models.CharField(max_length=255, choices=WORKER_STATE_CHOICES)
