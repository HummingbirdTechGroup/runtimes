#!/usr/bin/env python

import os
import imp
import datetime
import pickle
import io
import time

from multiprocessing import Process, Queue
from queue import Empty
import bottle
import prometheus_client as prom

mod = imp.load_source('function',
                      '/kubeless/%s.py' % os.getenv('MOD_NAME'))
func = getattr(mod, os.getenv('FUNC_HANDLER'))
func_port = os.getenv('FUNC_PORT', 8080)

timeout = float(os.getenv('FUNC_TIMEOUT', 180))

pipe_chunk_size = float(os.getenv('FUNC_CHUNK_SIZE', 32768))
churk_min_wait = float(os.getenv('FUNC_CHUNK_MIN_WAIT', 0.1))

app = application = bottle.app()

func_hist = prom.Histogram('function_duration_seconds',
                           'Duration of user function in seconds',
                           ['method'])
func_calls = prom.Counter('function_calls_total',
                           'Number of calls to user function',
                          ['method'])
func_errors = prom.Counter('function_failures_total',
                           'Number of exceptions in user function',
                           ['method'])

function_context = {
    'function-name': func,
    'timeout': timeout,
    'runtime': os.getenv('FUNC_RUNTIME'),
    'memory-limit': os.getenv('FUNC_MEMORY_LIMIT'),
}

def funcWrap(q, event, c):
    try:
        retval = func(event, c)
    except Exception as inst:
        retval = inst

    # chunk up return value to fit inside OS pipe max 64kb
    buff = io.BytesIO(pickle.dumps(ret))

    while True: # loop through and read chunks
        chunk = buff.read(pipe_chunk_size)
        if len(chunk) == 0;
            break
        q.put(chunk)

    q.put()

@app.get('/healthz')
def healthz():
    return 'OK'

@app.get('/metrics')
def metrics():
    bottle.response.content_type = prom.CONTENT_TYPE_LATEST
    return prom.generate_latest(prom.REGISTRY)


@app.route('/<:re:.*>', method=['GET', 'POST', 'PATCH', 'DELETE'])
def handler():
    req = bottle.request
    content_type = req.get_header('content-type')
    data = req.body.read()
    if content_type == 'application/json':
        data = req.json
    event = {
        'data': data,
        'event-id': req.get_header('event-id'),
        'event-type': req.get_header('event-type'),
        'event-time': req.get_header('event-time'),
        'event-namespace': req.get_header('event-namespace'),
        'extensions': {
            'request': req
        }
    }
    method = req.method
    func_calls.labels(method).inc()
    with func_errors.labels(method).count_exceptions():
        with func_hist.labels(method).time():
            q = Queue()
            p = Process(target=funcWrap, args=(q, event, function_context))
            p.start()

            acc = b"" # accumulate chunks
            end_time = time.time() + timeout # forecast timeout time
            finished = False

            while p.is_alive() or not q.empty():
                if time.time() > end_time: # timeout
                    break

                try:
                    res = q.get(timeout=max(chunk_min_wait, end_time-time.time()))
                except Empty:
                    pass
                else:
                    if not res:
                        finished = True
                        break

                    acc += res

            # If thread is still active
            if p.is_alive():
                p.terminate()
                p.join()

            if not finished:
                return bottle.HTTPError(408, "Timeout while processing the function")
            else:
                p.join()
                res = pickle.loads(acc)

                if isinstance(res, Exception):
                    raise res

                return res



if __name__ == '__main__':
    import logging
    import sys
    import requestlogger
    loggedapp = requestlogger.WSGILogger(
        app,
        [logging.StreamHandler(stream=sys.stdout)],
        requestlogger.ApacheFormatter())
    bottle.run(loggedapp, server='cherrypy', host='0.0.0.0', port=func_port)
