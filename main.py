import numpy as np
from streamz import Stream
from dask.distributed import Client
from time import sleep, perf_counter
from dataclasses import dataclass
from tornado import gen
from tornado.ioloop import IOLoop
from timeit import timeit


def provider(x):
    sleep(1)
    return x


def consumer1(x):
    return x**2


def consumer2(x):
    return x**3


def test_async():
    client = Client()
    print("cluster: ", client.cluster)
    print("cluster: ", client.cluster.workers)
    stream = Stream().scatter()
    # stream = Stream()
    start = perf_counter()
    for _ in range(10):
        stream.map(provider).map(consumer1).gather().sink(print)
        stream.map(provider).map(consumer2).gather().sink(print)

    stream.emit(2)
    print(f"finished in: {perf_counter() - start}s")


def test_loop():
    stream = Stream()
    stream.sink(print)
    consume = stream.map(provider).map(consumer1).filter(lambda x: x < 1000)
    consume.connect(stream)

    stream.emit(2)


def test_flow_control():
    source = Stream()
    source.sliding_window(2, return_partial=False).sink(print)

    source.emit(1)
    source.emit(2)
    source.emit(3)


@gen.coroutine
def test_buffer():
    source = Stream(asynchronous=True)
    source.buffer(10).map(lambda x: x).sink(print)

    for i in range(50):
        yield source.emit(i)
        sleep(0.1)


def test_time_delay():
    source = Stream(asynchronous=False)
    source.delay(20).map(lambda x: x**2).sink(print)

    source.emit(3)


@gen.coroutine
def f():
    source = Stream(asynchronous=True)  # tell the stream we're working asynchronously
    source.map(lambda x: x + 1).delay(5).sink(print)

    for x in range(10):
        yield source.emit(x)
        yield gen.sleep(1)


@gen.coroutine
def f1():
    source = Stream(asynchronous=True)  # tell the stream we're working asynchronously
    source.map(lambda x: x + 1).rate_limit(0.500).sink(print)

    for x in range(10):
        yield source.emit(x)


def f2(x):
    return x + 1


@dataclass
class MyClass:
    foo: int = 12
    bar: int = 13


def custom_model_provider(config: MyClass):
    return config


def custom_model_consumer(config: MyClass):
    return config.foo + config.bar


def test_custom_model():
    source = Stream()
    source.map(custom_model_provider).map(custom_model_consumer).sink(print)

    source.emit(x=MyClass(10, 10))


def test_scenario1():
    @dataclass
    class MyModel:
        value: int
        latest: bool

    def producer(x: MyModel):
        if not x.latest:
            if x.value < 65536:
                return x
            x.latest = True
            return x

    def consumer(x: MyModel):
        x.value = x.value**2
        return x

    source = Stream()
    end = source.map(producer).map(consumer)

    end.sink(lambda x: print(x.value))
    end.filter(lambda x: x.latest == True).sink(lambda x: print("finished: ", x.value))
    end.filter(lambda x: x.latest == False).connect(source)
    source.emit(MyModel(2, False))


def test_scenario2():
    def prov1(x):
        sleep(0.2)
        return np.arange(x)

    def prov2(x):
        sleep(0.5)
        return np.arange(x, 4 * x)

    def merge(x):
        return np.append(x[0], x[1])

    def consumer(x):
        return x / 2

    source = Stream()
    p1 = source.map(prov1)
    p2 = source.map(prov2)

    joined = p1.zip(p2).map(merge).map(consumer).sink(print)
    source.emit(2)


def test_scenario3():
    source = Stream()
    source.sliding_window(2, return_partial=False).sink(print)

    source.emit(1)
    source.emit(2)
    source.emit(3)


if __name__ == "__main__":
    test_scenario3()
