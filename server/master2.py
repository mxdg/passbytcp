#!/usr/bin/env python3
# coding=utf-8

from common_func import *
import queue
# import ssl
import base64

class http_service:
    def __init__(self,cfg):
        self.cfg = cfg

        self.customer_listen_addr = split_host(cfg['customer'])

        self.socketbridge = HttpBridge("http")
        self.socketbridge.start_as_daemon()

        self.run_master(None,self.customer_listen_addr)

    def updateconfig(self,cfg):
        self.cfg = cfg

        to_master = split_host(cfg['to_master'])
        hostdict = {}

        for item in cfg['host']:
            iteminfo = {}
            iteminfo['port'] = to_master[1]
            if item.get('auth'):
                iteminfo['username'] = item['auth']['username']
                iteminfo['password'] = item['auth']['password']
            if self.customer_listen_addr[1] == 80:
                hostdict['http://' + item['domain']] = iteminfo
            else:
                hostdict['http://' + item['domain'] + ':' + str(self.customer_listen_addr[1])] = iteminfo
        if self.socketbridge:
            self.socketbridge.updateconfig(hostdict)

    def dispose(self):
        if self.master:
            self.master.dispose()

    def run_master(self,communicate_addr, customer_listen_addr):
        log.info("http_service customer from: {}".format(
            fmt_addr(customer_listen_addr)))

        self.master = HTServer(customer_listen_addr, communicate_addr,None,self.socketbridge)
        self.master.serve_forever()


class https_service:
    pemfile = 'serverssl/ssl.crt'  # 服务证书公钥
    keyfile = 'serverssl/ssl.key'  # 服务证书密钥

    def __init__(self,cfg):
        self.cfg = cfg
        self.communicate_addr = split_host(cfg['master'])
        self.customer_listen_addr = split_host(cfg['customer'])
        self.SECRET_KEY = cfg['secretkey']

        hostdict = {}
        hostdict['https://hehui.ngrokhk.linkbus.xyz:8443'] = 10014
        self.socketbridge = HttpBridge(hostdict,"https")
        self.socketbridge.start_as_daemon()

        self.pkg = CtrlPkg()
        self.pkg.recalc_crc32(self.SECRET_KEY)

        logging.info(
            "main_master,{},{},id:{},self.pkg:{}".format(cfg['master'], self.SECRET_KEY, id(self), id(self.pkg)))

        self.run_master(self.communicate_addr,self.customer_listen_addr,self.pkg)

    def run_master(self,communicate_addr, customer_listen_addr,pkg):
        log.info("http_service from: {} customer from: {}".format(
            fmt_addr(communicate_addr), fmt_addr(customer_listen_addr)))

        self.master = HTServer(customer_listen_addr, communicate_addr,True,pkg,self.socketbridge)
        self.master.serve_forever()

class HTServer:
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 ssl=None, socketbridge=None,_listening_sockets=None):
        """

        :param customer_listen_addr: equals to the -c/--customer param
        :param communicate_addr: equals to the -m/--master param
        """

        # self.ssl = ssl

        self._stopped = {"stop":False}

        self._listening_sockets = []
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

        self.working_pool = {}

        self.socket_bridge = socketbridge

        # a queue for customers who have connected to us,
        #   but not assigned a slaver yet
        self.pending_customers = queue.Queue()

        # self.communicate_addr = communicate_addr



        # prepare Thread obj, not activated yet
        self.customer_listen_addr = customer_listen_addr
        _fmt_communicate_addr = fmt_addr(self.customer_listen_addr)

        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(_fmt_communicate_addr),
            daemon=True,
        )


        # prepare assign_slaver_daemon
        self.thread_pool["assign_slaver_daemon"] = threading.Thread(
            target=self._assign_slaver_daemon,
            name="assign_slaver_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

    def dispose(self):
        self._stopped['stop'] = True
        logging.info("master dispose {}".format(self._stopped))
        # while len(self.slaver_pool):
        #     slaver = self.slaver_pool.pop()
        #     try:
        #         slaver['addr_slaver'].shutdown(socket.SHUT_WR)
        #     except Exception as e:
        #         pass
        #     try:
        #         slaver['conn_slaver'].shutdown(socket.SHUT_WR)
        #     except Exception as e:
        #         pass
        #     try:
        #         slaver['addr_slaver'].close()
        #     except Exception as e:
        #         pass
        #     try:
        #         slaver['conn_slaver'].close()
        #     except Exception as e:
        #         pass
        # self.working_pool = None
        for sock in self._listening_sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                pass
            try:
                sock.close()
            except Exception as e:
                pass
        self.thread_pool["socket_bridge"] = None
        self.pending_customers = None

    def serve_forever(self):
        # if not self.external_slaver:
        #     self.thread_pool["listen_slaver"].start()
        # self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()
        self.thread_pool["assign_slaver_daemon"].start()
        self.thread_pool["socket_bridge"] = self.socket_bridge.get_thread()

        # while True:
        #     time.sleep(10)

    def try_bind_port(self,sock, addr):
        while not self._stopped['stop']:
            try:
                sock.bind(addr)
            except Exception as e:
                log.error((
                          "unable to bind {}, {}. If this port was used by the recently-closed shootback itself\n"
                          "then don't worry, it would be available in several seconds\n"
                          "we'll keep trying....").format(addr, e))
                log.debug(traceback.format_exc())
                time.sleep(3)
            else:
                break

    def _transfer_complete(self, addr_customer):
        """a callback for SocketBridge, do some cleanup jobs"""
        log.info("customer complete: {}".format(addr_customer))
        del self.working_pool[addr_customer]

    def _serve_customer(self, conn_customer,):
        """put customer and slaver sockets into SocketBridge, let them exchange data"""
        self.socket_bridge.add_conn_pair(
            conn_customer,
            functools.partial(  # it's a callback
                # 这个回调用来在传输完成后删除工作池中对应记录
                self._transfer_complete,
                conn_customer.getpeername()
            )
        )

    def _send_heartbeat(self,conn_slaver):
        """send and verify heartbeat pkg"""
        conn_slaver.send(self.pkg.pbuild_heart_beat().raw)

        pkg, verify = self.pkg.recv(
            conn_slaver, expect_ptype=CtrlPkg.PTYPE_HEART_BEAT)  # type: CtrlPkg,bool

        if not verify:
            return False

        if pkg.prgm_ver < 0x000B:
            # shootback before 2.2.5-r10 use two-way heartbeat
            #   so there is no third pkg to send
            pass
        else:
            # newer version use TCP-like 3-way heartbeat
            #   the older 2-way heartbeat can't only ensure the
            #   master --> slaver pathway is OK, but the reverse
            #   communicate may down. So we need a TCP-like 3-way
            #   heartbeat
            conn_slaver.send(self.pkg.pbuild_heart_beat().raw)

        return verify

    def _heart_beat_daemon(self):
        default_delay = 5 + SPARE_SLAVER_TTL // 12
        delay = default_delay
        log.info("heart beat daemon start, delay: {}s".format(delay))
        while not self._stopped['stop']:
            time.sleep(delay)
            # log.debug("heart_beat_daemon: hello! im weak")

            # ---------------------- preparation -----------------------
            slaver_count = len(self.slaver_pool)

            # logging.info("_heart_beat_daemon test {},{}".format(id(self),self._stopped))

            if not slaver_count:
                log.warning("heart_beat_daemon: sorry, no slaver available, keep sleeping")
                # restore default delay if there is no slaver
                delay = default_delay
                continue
            else:
                # notice this `slaver_count*2 + 1`
                # slaver will expire and re-connect if didn't receive
                #   heartbeat pkg after SPARE_SLAVER_TTL seconds.
                # set delay to be short enough to let every slaver receive heartbeat
                #   before expire
                delay = 1 + SPARE_SLAVER_TTL // max(slaver_count * 2 + 1, 12)

            # pop the oldest slaver
            #   heartbeat it and then put it to the end of queue
            slaver = self.slaver_pool.popleft()
            addr_slaver = slaver["addr_slaver"]

            # ------------------ real heartbeat begin --------------------
            start_time = time.perf_counter()
            try:
                hb_result = self._send_heartbeat(slaver["conn_slaver"])
            except Exception as e:
                log.warning("error during heartbeat to {}: {}".format(
                    fmt_addr(addr_slaver), e))
                log.debug(traceback.format_exc())
                hb_result = False
            finally:
                time_used = round((time.perf_counter() - start_time) * 1000.0, 2)
            # ------------------ real heartbeat end ----------------------

            if not hb_result:
                log.warning("heart beat failed: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                try_close(slaver["conn_slaver"])
                del slaver["conn_slaver"]

                # if heartbeat failed, start the next heartbeat immediately
                #   because in most cases, all 5 slaver connection will
                #   fall and re-connect in the same time
                delay = 0

            else:
                log.debug("heartbeat success: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                self.slaver_pool.append(slaver)

    def _handshake(self,conn_slaver):
        conn_slaver.send(self.pkg.pbuild_hs_m2s().raw)

        log.debug("CtrlPkg key{},{}".format(self.pkg.SECRET_KEY_CRC32,self.pkg.SECRET_KEY_REVERSED_CRC32))

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return False

        pkg, verify = self.pkg.decode_verify(buff, CtrlPkg.PTYPE_HS_S2M)  # type: CtrlPkg,bool

        log.debug("CtrlPkg from slaver {}: {}".format(conn_slaver.getpeername(), pkg))

        return verify

    def _get_an_active_slaver(self):
        """get and activate an slaver for data transfer"""
        try_count = 10
        while not self._stopped['stop']:
            try:
                logging.info("master _get_an_active_slaver self.slaver_pool:{},{}".format(id(self.slaver_pool), self.slaver_pool))
                dict_slaver = self.slaver_pool.popleft()
            except:
                if try_count:
                    time.sleep(0.02)
                    try_count -= 1
                    if try_count % 10 == 0:
                        log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))
                    continue
                return None

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                hs = self._handshake(conn_slaver)
            except Exception as e:
                log.warning("Handshake failed: {},key:{},{},{},{}".format(e,id(self),self.pkg.skey,self.pkg.SECRET_KEY_CRC32,self.pkg.SECRET_KEY_REVERSED_CRC32))
                log.debug(traceback.format_exc())
                hs = False

            if hs:
                return conn_slaver
            else:
                log.warning("slaver handshake failed: {}".format(dict_slaver["addr_slaver"]))
                try_close(conn_slaver)

                time.sleep(0.02)

    def _assign_slaver_daemon(self):
        while not self._stopped['stop']:
            conn_customer, addr_customer,tmp = self.pending_customers.get()

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer
            }

            try:
                self._serve_customer(conn_customer)
            except:
                try:
                    try_close(conn_customer)
                except:
                    pass
                continue

    def _listen_customer(self):
        # if self.ssl:
        #     context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        #     context.load_cert_chain(certfile=https_service.pemfile, keyfile=https_service.keyfile)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.try_bind_port(sock, self.customer_listen_addr)
        sock.listen(20)
        self._listening_sockets.append(sock)
        log.info("Listening for customers: {}".format(
            fmt_addr(self.customer_listen_addr)))
        while not self._stopped['stop']:
            conn_customer, addr_customer = sock.accept()
            # if self.ssl:
            #     conn_customer = context.wrap_socket(conn_customer, server_side=True)
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, self.pending_customers.qsize() + 1
            ))

            # just put it into the queue,
            #   let _assign_slaver_daemon() do the else
            #   don't block this loop
            self.pending_customers.put((conn_customer, addr_customer,None))


class HttpBridge:
    def __init__(self,agre="http"):
        self.conn_rd = set()  # record readable-sockets
        self.fdmap={}
        self.map = {}  # record sockets pairs
        self.callbacks = {}  # record callbacks
        self.tmp_thread = None
        self.agre = agre

        self.hostdict = {}

    def updateconfig(self,host):
        if host is not None:
            self.hostdict = host

    def add_conn_pair(self, conn1, callback=None):
        self.conn_rd.add(conn1)

        logging.info("HttpBridge add_conn_pair con:{}".format(conn1))

        # record callback
        if callback is not None:
            self.callbacks[conn1] = callback

    def get_thread(self):
        return self.tmp_thread

    def start_as_daemon(self):
        t = threading.Thread(target=self.start)
        t.daemon = True
        t.start()
        log.info("SocketBridge daemon started")
        self.tmp_thread = t;
        # return t

    def start(self):
        server_pool.ServerPool.bridgeAdd += 1
        while True:
            try:
                self._start()
            except:
                log.error("FATAL ERROR! SocketBridge failed {}".format(
                    traceback.format_exc()
                ))

    def _start(self):
        # memoryview act as an recv buffer
        # refer https://docs.python.org/3/library/stdtypes.html#memoryview
        # buff = memoryview(bytearray(RECV_BUFFER_SIZE))
        while True:
            if not self.conn_rd:
                # sleep if there is no connections
                time.sleep(0.06)
                continue

            # blocks until there is socket(s) ready for .recv
            # notice: sockets which were closed by remote,
            #   are also regarded as read-ready by select()
            r, w, e = select.select(self.conn_rd, [], [], 0.5)

            for s in r:  # iter every read-ready or closed sockets
                try:
                    # here, we use .recv_into() instead of .recv()
                    #   recv data directly into the pre-allocated buffer
                    #   to avoid many unnecessary malloc()
                    # see https://docs.python.org/3/library/socket.html#socket.socket.recv_into
                    buff = bytearray(RECV_BUFFER_SIZE)
                    rec_len = s.recv_into(buff, RECV_BUFFER_SIZE)

                    agre = self.agre
                    buffdata = buff[:rec_len]
                    infoss = buffdata.decode('utf-8')
                    heads = httphead(infoss)
                    logging.info("recv head:{}".format(heads))
                    url = agre + '://' + heads['Host']
                    logging.info("hostdict PAN DUAN:{},len:{}".format(url,len(self.conn_rd)))

                    cfg = self.hostdict.get(url)
                    logging.info("hostdict config url:{},cfg:{}".format(url, cfg))
                    auth = None
                    if cfg:
                        if cfg.get('username'):
                            if heads.get('Authorization'):
                                _,pwd = heads['Authorization'].split('Basic ')
                                pwd = base64.b64decode(pwd)
                                auth = cfg['username']+":"+cfg['password']
                                logging.info("http password compare:{} - {}".format(pwd.decode("utf-8"),auth))
                                if pwd.decode("utf-8") == auth:
                                    auth = True
                            else:
                                auth = None
                        else:
                            auth = True
                    else:
                        self._rd_shutdown(s,False,0)
                        continue
                    if auth is True:
                        portcfg = self.hostdict[url]
                        loop = server_pool.ServerPool.get_instance().getloop
                        dict = loop.getPortdict
                        logging.info("http resend by url:{}".format(url))
                        # m1 = id(buff)
                        # b2 = buff[:rec_len]
                        # m2 = id(b2)
                        # logging.info("地址比较:{},{}".format(m1,m2))
                        dict[str(portcfg['port'])].master.add_http_customer(s,s.getpeername(),buffdata)
                        self.conn_rd.remove(s)
                        # tosock = self.map[s]
                        # self.conn_rd.remove(tosock)
                        # self.map.pop(s)
                        # self.map.pop(tosock)
                        continue
                    else:
                        html = 'Tunnel %s not found' % heads['Host']
                        header = "HTTP/1.0 401 Unauthorised" + "\r\n"
                        header += "Server: SokEvo/1.0" + "\r\n"
                        header += 'WWW-Authenticate: Basic realm="'+ heads['Host'] +'"' + "\r\n"
                        header += "Content-Type: text/html" + "\r\n"
                        header += "Content-Length: %d" + "\r\n"
                        header += "\r\n" + "%s"
                        buf = header % (len(html.encode('utf-8')), html)
                        s.send(buf.encode('utf-8'))
                        logging.info("http resend fail url:{}".format(url))
                        # self._rd_shutdown(s,False,1)

                except Exception as e:
                    # unable to read, in most cases, it's due to socket close
                    self._rd_shutdown(s,False,2,e)
                    continue

                if not rec_len:
                    # read zero size, closed or shutdowned socket
                    self._rd_shutdown(s,False,3)
                    continue

                # try:
                #     # send data, we use `buff[:rec_len]` slice because
                #     #   only the front of buff is filled
                #     self.map[s].send(buff[:rec_len])
                # except Exception as e:
                    # unable to send, close connection
                self._rd_shutdown(s,False,4)
                    # continue

    def _rd_shutdown(self, conn, once=False,ff=False,e=False):
        """action when connection should be read-shutdown
        :type conn: socket.SocketType
        """

        logging.info("HttpBridge _rd_shutdown  con:{} from:{}  e:{}".format(conn,ff,e))

        if conn in self.conn_rd:
            self.conn_rd.remove(conn)

        try:
            conn.shutdown(socket.SHUT_RD)
        except:
            pass

        if not once and conn in self.map:  # use the `once` param to avoid infinite loop
            # if a socket is rd_shutdowned, then it's
            #   pair should be wr_shutdown.
            self._wr_shutdown(self.map[conn], True)

        if self.map.get(conn) not in self.conn_rd:
            # if both two connection pair was rd-shutdowned,
            #   this pair sockets are regarded to be completed
            #   so we gonna close them
            self._terminate(conn)

    def _wr_shutdown(self, conn, once=False):
        """action when connection should be write-shutdown
        :type conn: socket.SocketType
        """
        try:
            conn.shutdown(socket.SHUT_WR)
        except:
            pass

        if not once and conn in self.map:  # use the `once` param to avoid infinite loop
            #   pair should be rd_shutdown.
            # if a socket is wr_shutdowned, then it's
            self._rd_shutdown(self.map[conn], True)

    def _terminate(self, conn):
        """terminate a sockets pair (two socket)
        :type conn: socket.SocketType
        :param conn: any one of the sockets pair
        """
        try_close(conn)  # close the first socket

        server_pool.ServerPool.bridgeRemove += 1

        # ------ close and clean the mapped socket, if exist ------
        if conn in self.map:
            _mapped_conn = self.map[conn]
            try_close(_mapped_conn)
            if _mapped_conn in self.map:
                del self.map[_mapped_conn]

            del self.map[conn]  # clean the first socket
        else:
            _mapped_conn = None  # just a fallback

        # ------ callback --------
        # because we are not sure which socket are assigned to callback,
        #   so we should try both
        if conn in self.callbacks:
            try:
                self.callbacks[conn]()
            except Exception as e:
                log.error("traceback error: {}".format(e))
                log.debug(traceback.format_exc())
            del self.callbacks[conn]
        elif _mapped_conn and _mapped_conn in self.callbacks:
            try:
                self.callbacks[_mapped_conn]()
            except Exception as e:
                log.error("traceback error: {}".format(e))
                log.debug(traceback.format_exc())
            del self.callbacks[_mapped_conn]
