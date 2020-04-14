"""Task decorators."""
import functools

from celery import Celery
from celery.app import app_or_default

__all__ = ["periodic_task"]


# A list of periodic tasks that are to be connected when Celery is ready.
# The tasks are stored as a list of (*arg, **kwarg) tuples.
_periodic_tasks = None


def _add_periodic_task(*args, **kwargs):
    """Queues a periodic task to be registered after Celery has finished initializing."""
    global _periodic_tasks

    if not _periodic_tasks:
        _periodic_tasks = []

        # Connect to a Celery signal (once) to know when it's safe to register the tasks.
        app: Celery = app_or_default()
        app.on_after_configure.connect(_register_periodic_tasks)

    _periodic_tasks.append((args, kwargs))


def _register_periodic_tasks(sender: Celery, **kwargs):
    """
    Registers each task that was queued by _add_periodic_task. While it would be
    convenient to just do this directly in the `periodic_task` decorator, the problem
    there is that the signal callback is stored on the stack and becomes dead as soon
    as the decorator exits.
    """
    global _periodic_tasks

    # Add each task.
    for task in _periodic_tasks:
        sender.add_periodic_task(*task[0], **task[1])

    # No longer needed.
    _periodic_tasks = None


def periodic_task(run_every, **task_kwargs):
    """
    Decorator for creating a periodic task.

    `run_every` specifies when or how often the periodic task will be scheduled to run.

    It supports several different types:

    - `float`: interpreted as seconds.
    - `timedelta`: interpreted as a regular time interval.
    - `celery.schedules.crontab`: interpreted as an interval using crontab notation.
    - `celery.schedules.solar`: interpreted as an interval based on solar occurences.

    ### Example

    ```
        from django_celery_beat.decorators import periodic_task
        from datetime import timedelta

        @periodic_task(run_every=timedelta(minutes=5))
        def say_hello():
            print("Hello, world!")
    ```

    ### Resources

    Info on crontab scheduling:

    https://docs.celeryproject.org/en/v4.1.0/userguide/periodic-tasks.html#crontab-schedules

    Info on solar scheduling:

    https://docs.celeryproject.org/en/v4.1.0/userguide/periodic-tasks.html#solar-schedules
    """

    app: Celery = app_or_default()

    def wrapper(task_func):
        # Wrap the decorated function to convert it into a celery task while also
        # preserving its original properties so that a celery worker can find it.
        @app.task
        @functools.wraps(task_func)
        def wrapped_task(*args, **kwargs):
            return task_func(*args, **kwargs)

        _add_periodic_task(run_every, wrapped_task, task_kwargs)

        return wrapped_task

    return wrapper
