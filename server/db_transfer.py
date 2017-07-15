#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging

from server_pool import ServerPool

class Dbv3Transfer(object):


    @staticmethod
    def thread_db(obj):
        ServerPool.get_instance()

    @staticmethod
    def thread_db_stop():
        ServerPool.get_instance().stop()