"""Task decorators."""
from celery import Celery
from celery.app import app_or_default


def periodic_task(task_func, *task_args, **task_kwargs):
    app: Celery = app_or_default()
    run_every = task_kwargs.pop("run_every")

    @app.on_after_configure.connect
    def inner_task(sender: Celery, **sender_kwargs):
        sender.add_periodic_task(run_every, task_func, **task_kwargs)

    return inner_task
