
import collections
import threading
import logging
import time
import grpc

from fedlearner.bridge import bridge_pb2

class _MethodDetail(
    collections.namedtuple('_MethodDetails',
                          ('method', 'request_serializer',
                          'response_deserializer'))):
    pass

class WaitInterceptor(grpc.UnaryUnaryClientInterceptor,
                       grpc.UnaryStreamClientInterceptor,
                       grpc.StreamUnaryClientInterceptor,
                       grpc.StreamStreamClientInterceptor):
    def __init__(self, wait):
        self._methods = set()
        self._wait = wait

    def register_method(self, method):
        self._methods.add(method)

    def _handle(self, continuation, client_call_details, request):
        if client_call_details.method in self._methods:
            self._wait(client_call_details.timeout)
        return continuation(client_call_details, request)

    def intercept_unary_unary(self, continuation, client_call_details,
                              request):
        return self._handle(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details,
                               request):
        return self._handle(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details,
                               request_iterator):
        return self._handle(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details,
                                request_iterator):
        return self._handle(continuation, client_call_details, request_iterator)

def stream_stream_request_serializer(request):
    return bridge_pb2.SendRequest.SerializeToString(request)

def stream_stream_response_deserializer(serialized_response):
    return bridge_pb2.SendResponse.FromString(serialized_response)

class RetryInterceptor(grpc.UnaryUnaryClientInterceptor,
                       grpc.UnaryStreamClientInterceptor,
                       grpc.StreamUnaryClientInterceptor,
                       grpc.StreamStreamClientInterceptor):
    def __init__(self, retry_interval):
        assert retry_interval > 0
        self._retry_interval = retry_interval
        self._method_details = dict()

    def register_method(self, method,
                        request_serializer,
                        response_deserializer):
        self._method_details[method] = _MethodDetail(
            method, request_serializer, response_deserializer)

    def intercept_unary_unary(self, continuation, client_call_details,
                              request):
        method_details = self._method_details.get(client_call_details.method)
        if method_details:
            return _grpc_with_retry(
                lambda: continuation(client_call_details, request),
                self._retry_interval)

        return continuation(client_call_details, request)


    def intercept_unary_stream(self, continuation, client_call_details,
                               request):
        method_details = self._method_details.get(client_call_details.method)
        if method_details:
            return _grpc_with_retry(
                lambda: continuation(client_call_details, request),
                self._retry_interval)

        return continuation(client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details,
                               request_iterator):
        method_details = self._method_details.get(client_call_details.method)
        if method_details:
            return _grpc_with_retry(
                lambda: continuation(client_call_details, request_iterator),
                self._retry_interval)

        return continuation(client_call_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details,
                                request_iterator):
        method_details = self._method_details.get(client_call_details.method)
        if not method_details:
            return continuation(client_call_details, request_iterator)

        srq = _SingleConsumerSendRequestQueue(
            request_iterator, method_details.request_serializer)
        acker = _AckHelper()

        def call_fn():
            consumer = srq.consumer()
            acker.set_consumer(consumer)
            res = continuation(client_call_details, iter(consumer))
            return res

        def response_iterator(init_stream_response):
            stream_response = init_stream_response
            while True:
                try:
                    for res in stream_response:
                        acker.ack(res.ack)
                        yield method_details.response_deserializer(res.payload)
                    return
                except grpc.RpcError as e:
                    if _grpc_error_need_recover(e):
                        logging.warning("[Bridge] grpc stream error, status: %s"
                            ", details: %s, wait %ds for retry",
                            e.code(), e.details(), self._retry_interval)
                        time.sleep(self._retry_interval)
                        stream_response = _grpc_with_retry(
                            call_fn, self._retry_interval)
                        continue
                    raise e

        init_stream_response = _grpc_with_retry(call_fn, self._retry_interval)

        return response_iterator(init_stream_response)

class _AckHelper():
    def __init__(self):
        self._consumer = None

    def set_consumer(self, consumer):
        self._consumer = consumer

    def ack(self, ack):
        self._consumer.ack(ack)

class _SingleConsumerSendRequestQueue():
    class Consumer():
        def __init__(self, queue):
            self._queue = queue

        def ack(self, ack):
            self._queue.ack(self, ack)

        def __iter__(self):
            return self

        def __next__(self):
            return self._queue.next(self)

    def __init__(self, request_iterator, request_serializer):
        self._lock = threading.Lock()
        self._seq = 0
        self._offset = 0
        self._deque = collections.deque()
        self._consumer = None

        self._request_lock = threading.Lock()
        self._request_iterator = request_iterator
        self._request_serializer = request_serializer


    def _reset(self):
        #logging.debug("[Bridge] _SingleConsumerSendRequestQueue reset"
        #    ",self._offset: %d, self._seq: %d, len(self._deque): %d",
        #    self._offset, self._seq, len(self._deque))
        self._offset = 0

    def _empty(self):
        return self._offset == len(self._deque)

    def _get(self):
        assert not self._empty()
        req = self._deque[self._offset]
        self._offset += 1
        #logging.debug("[Bridge] _SingleConsumerSendRequestQueue get: %d"
        #    ", self._offset: %d, len(self._deque): %d, self._seq: %d",
        #    req.seq, self._offset, len(self._deque), self._seq)
        return req

    def _add(self, raw_req):
        req = bridge_pb2.SendRequest(
            seq=self._seq,
            payload=self._request_serializer(raw_req))
        self._seq += 1
        self._deque.append(req)

    def _consumer_check(self, consumer):
        return self._consumer == consumer

    def _consumer_check_or_call(self, consumer, call):
        if not self._consumer_check(consumer):
            call()

    def ack(self, consumer, ack):
        with self._lock:
            if not self._consumer_check(consumer):
                return
            if ack >= self._seq:
                return
            n = self._seq - ack
            while len(self._deque) >= n:
                self._deque.popleft()
                self._offset -= 1

    def next(self, consumer):
        def stop_iteration_fn():
            raise StopIteration()
        while True:
            with self._lock:
                self._consumer_check_or_call(consumer, stop_iteration_fn)
                if not self._empty():
                    return self._get()

            # get from request_iterator
            with self._request_lock:
                with self._lock:
                    # check again
                    self._consumer_check_or_call(consumer, stop_iteration_fn)
                    if not self._empty():
                        return self._get()
                # call next maybe block by user code
                # then return data or raise StopIteration()
                # so use self._request_lock instead of self._lock
                raw_req = next(self._request_iterator)
                with self._lock:
                    self._add(raw_req)
                    self._consumer_check_or_call(consumer, stop_iteration_fn)
                    return self._get()

    def consumer(self):
        with self._lock:
            self._reset()
            self._consumer = _SingleConsumerSendRequestQueue.Consumer(self)
            return self._consumer


def _grpc_with_retry(fn, interval=1):
    while True:
        try:
            res = fn()
            #pylint: disable=unidiomatic-typecheck
            if type(res) == grpc.RpcError:
                raise res
            return res
        except grpc.RpcError as e:
            if _grpc_error_need_recover(e):
                logging.warning("[Bridge] grpc error, status: %s"
                    ", details: %s, wait %ds for retry",
                    e.code(), e.details(), interval)
                time.sleep(interval)
                continue
            raise e

def _grpc_error_need_recover(e):
    if not isinstance(e, grpc.RpcError):
        return False
    if e.code() in (grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.INTERNAL):
        return True
    if e.code() == grpc.StatusCode.UNKNOWN:
        httpstatus = _grpc_error_get_http_status(e)
        if httpstatus:
            if 400 <= httpstatus < 500:
                return True
    return False

def _grpc_error_get_http_status(e):
    try:
        details = e.details()
        if details.count("http2 header with status") > 0:
            fields = details.split(":")
            if len(fields) == 2:
                return int(details.split(":")[1])
    except Exception: #pylint: disable=broad-except
        pass

    return None
