#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import threading
import eventloop

class MainThread(threading.Thread):
    def __init__(self, loop):
        threading.Thread.__init__(self)
        self.loop = loop

    def run(self):
        ServerPool._loop(self.loop)

    def stop(self):
        self.loop.thread_stop()


class ServerPool(object):
    instance = None

    bridgeAdd = 0
    bridgeRemove = 0

    def __init__(self):
        self.loop = eventloop.EventLoop()
        self.thread = MainThread(self.loop)
        self.thread.start()

    @property
    def getloop(self):
        return self.loop

    @staticmethod
    def get_instance():
        if ServerPool.instance is None:
            ServerPool.instance = ServerPool()
        return ServerPool.instance

    @staticmethod
    def _loop(loop):
        try:
            loop.run()
        except (KeyboardInterrupt, IOError, OSError) as e:
            logging.error(e)
            import traceback
            traceback.print_exc()
            exit(0)
        except Exception as e:
            logging.error(e)
            import traceback
            traceback.print_exc()

    def stop(self):
        self.thread.stop()