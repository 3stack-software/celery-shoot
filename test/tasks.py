import os
import time

from celery import Celery

broker = os.environ.get("AMQP_HOST", "amqp://guest:guest@localhost//")

app = Celery("tasks", broker=broker, backend="rpc://")


@app.task
def add(x, y):
    print(f"got task to add {x} + {y} = {x+y}")
    return x + y


@app.task
def sleep(x):
    time.sleep(x)
    return x


@app.task
def curtime():
    current_time = int(time.time() * 1000)
    print(f"the time is {current_time}")
    print(f"the time is {time.time()}")
    return current_time


@app.task
def error(msg):
    raise Exception(msg)


@app.task
def echo(msg):
    return msg


# client should call with ignoreResult=True as results are never sent
@app.task(ignore_result=True)
def send_email(to="me@example.com", title="hi"):
    print("Sending email to '%s' with title '%s'" % (to, title))


if __name__ == "__main__":
    app.worker_main(argv=["worker", "--loglevel=INFO"])
