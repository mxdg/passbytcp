#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import time
import select
import json
from master import *
import server_pool
from master2 import *

import socket



class EventLoop(object):

    def __init__(self):
        self._stopping = False

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn_rd =[sock]

        self.portdict = {}
        self.socketbridge = SocketBridge()
        self.socketbridge.start_as_daemon()

    @property
    def getPortdict(self):
        return self.portdict

    def thread_stop(self):
        self._stopping = True
        exists = self.portdict.keys()
        for cur in exists:
            logging.info("loop dispose port {}".format(cur))
            self.portdict[cur].dispose()


    def run(self):
        while not self._stopping:
            # logging.debug('hh:{}'.format(r['hh']))

            try:
                f = open('config.json', 'r')
                configs = json.load(f)
                http = configs['http']
                tcp = configs['tcp']

                check = {}

                if http:
                    host, port = http['customer'].split(":")
                    if self.portdict.get(port) is None:
                        self.portdict[port] = http_service(http)
                    self.portdict[port].updateconfig(http)
                    check[port] = http

                for c in tcp:
                    host, port = c['master'].split(":")
                    check[port] = c

                exists = self.portdict.keys()
                removelist = []
                for cur in exists:
                    if not check.get(cur):
                        logging.info("loop dispose port {}".format(cur))
                        self.portdict[cur].dispose()
                        removelist.append(cur)

                for re in removelist:
                    self.portdict.pop(re)

                for (port,c) in check.items():
                    if not self.portdict.get(port):
                        logging.info("run init {}".format(c['master']))
                        self.portdict[port] = Mastar_line(self.socketbridge)
                        self.portdict[port].main_master(c)

            except Exception as e:
                logging.info("fail config.json e:{}".format(e))
            finally:
                if f:
                    f.close()


            # logging.debug('using event model: 123')
            logging.info("bridgeAdd:{},bridgeRemove:{}".format(server_pool.ServerPool.bridgeAdd,server_pool.ServerPool.bridgeRemove))
            time.sleep(10)
