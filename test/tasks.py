import os
import time

from celery import Celery

broker = os.environ.get('AMQP_HOST', 'amqp://guest:guest@localhost//')

celery = Celery('tasks', broker=broker)

celery.conf.update(
        CELERY_ACCEPT_CONTENT=["json"],
        CELERY_RESULT_BACKEND = "amqp",
        CELERY_RESULT_SERIALIZER='json',
        )


@celery.task
def add(x, y):
    print('got task to add {} + {} = {}'.format(x, y, x+y))
    return x + y


@celery.task
def sleep(x):
    time.sleep(x)
    return x


@celery.task
def curtime():
    current_time = int(time.time() * 1000)
    print('the time is {}'.format(current_time))
    print('the time is {}'.format(time.time()))
    return current_time


@celery.task
def error(msg):
    raise Exception(msg)


@celery.task
def echo(msg):
    return msg


# client should call with ignoreResult=True as results are never sent
@celery.task(ignore_result=True)
def send_email(to='me@example.com', title='hi'):
    print("Sending email to '%s' with title '%s'" % (to, title))


if __name__ == "__main__":
    celery.start()
