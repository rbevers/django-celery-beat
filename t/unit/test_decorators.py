import types
from unittest import mock, TestCase

import celery.local
from celery import Celery
from django_celery_beat.decorators import (
    periodic_task,
    _periodic_tasks,
    _register_all_periodic_tasks,
)


class PeriodicTaskDecoratorTests(TestCase):
    def setUp(self):
        _periodic_tasks.clear()

    @mock.patch("django_celery_beat.decorators._app.configured", False)
    def test_func_becomes_celery_task(self):
        """Test the @periodic_task decorator converts a plain function into a celery task."""

        @periodic_task(run_every=0)
        def fn():
            pass

        self.assertTrue(hasattr(fn, "delay"))

    @mock.patch("django_celery_beat.decorators._app.configured", False)
    def test_task_added_to_queue_when_not_ready(self):
        """Test that tasks are added to a queue when the Celery app is not yet configured."""

        @periodic_task(run_every=123, kwarg_test="blah")
        def fn():
            pass

        self.assertEqual(len(_periodic_tasks), 1)

        args, kwargs = _periodic_tasks[0]

        self.assertEqual(args, (123, fn))
        self.assertEqual(kwargs, {"kwarg_test": "blah"})

    @mock.patch("django_celery_beat.decorators._app.configured", True)
    @mock.patch("django_celery_beat.decorators._register_periodic_task")
    def test_task_registered_immediately_when_app_ready(self, register_mock):
        """Test that when Celery is ready the task is registered immediately and not added to the queue."""

        @periodic_task(run_every=123)
        def fn():
            pass

        self.assertEqual(len(_periodic_tasks), 0)
        self.assertEqual(register_mock.call_count, 1)
        self.assertEqual(register_mock.call_args[0], (123, fn))

    @mock.patch("django_celery_beat.decorators._app.configured", False)
    @mock.patch("django_celery_beat.decorators._register_periodic_task")
    def test_all_tasks_registered(self, register_mock):
        """Test that all tasks in the queue are registered and the queue is cleared."""

        @periodic_task(run_every=123)
        def fn1():
            pass

        @periodic_task(run_every=456)
        def fn2():
            pass

        self.assertEqual(len(_periodic_tasks), 2)

        _register_all_periodic_tasks()

        self.assertEqual(len(_periodic_tasks), 0)
        self.assertEqual(register_mock.call_count, 2)
        self.assertEqual(register_mock.call_args_list[0][0], (123, fn1))
        self.assertEqual(register_mock.call_args_list[1][0], (456, fn2))
