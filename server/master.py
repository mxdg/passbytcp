#!/usr/bin/env python3
# coding=utf-8
from common_func import *
import queue
import threading

#_listening_sockets = []  # for close at exit
# __author__ = "Aploium <i@z.codes>"
# __website__ = "https://github.com/aploium/shootback"

local = threading.local()

class Master:
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None,pkg=None, socketbridge=None,_listening_sockets=None):
        """

        :param customer_listen_addr: equals to the -c/--customer param
        :param communicate_addr: equals to the -m/--master param
        """

        self.pkg = pkg

        self._stopped = {"stop":False}

        logging.info("Master__init__,{},{}".format(id(self),id(self.pkg)))

        self._listening_sockets = []
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

        self.working_pool = {}

        self.socket_bridge = socketbridge

        # a queue for customers who have connected to us,
        #   but not assigned a slaver yet
        self.pending_customers = queue.Queue()

        self.communicate_addr = communicate_addr

        _fmt_communicate_addr = fmt_addr(self.communicate_addr)

        if slaver_pool:
            # 若使用外部slaver_pool, 就不再初始化listen
            # 这是以后待添加的功能
            self.external_slaver = True
            self.thread_pool["listen_slaver"] = None
        else:
            # 自己listen来获取slaver
            self.external_slaver = False
            self.slaver_pool = collections.deque()
            # prepare Thread obj, not activated yet
            self.thread_pool["listen_slaver"] = threading.Thread(
                target=self._listen_slaver,
                name="listen_slaver-{}".format(_fmt_communicate_addr),
                daemon=True,
            )

        logging.info("master init self.slaver_pool:{},{}".format(id(self.slaver_pool),self.slaver_pool))

        # prepare Thread obj, not activated yet
        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare Thread obj, not activated yet
        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(_fmt_communicate_addr),
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
        while len(self.slaver_pool):
            slaver = self.slaver_pool.pop()
            try:
                slaver['addr_slaver'].shutdown(socket.SHUT_WR)
            except Exception as e:
                pass
            try:
                slaver['conn_slaver'].shutdown(socket.SHUT_WR)
            except Exception as e:
                pass
            try:
                slaver['addr_slaver'].close()
            except Exception as e:
                pass
            try:
                slaver['conn_slaver'].close()
            except Exception as e:
                pass
        self.working_pool = None
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
        self.pkg.clean_crc32()

    def serve_forever(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
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

    def _serve_customer(self, conn_customer, conn_slaver,tmp):
        """put customer and slaver sockets into SocketBridge, let them exchange data"""
        self.socket_bridge.add_conn_pair(
            conn_customer, conn_slaver,tmp,
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
        """

        每次取出slaver队列头部的一个, 测试心跳, 并把它放回尾部.
            slaver若超过 SPARE_SLAVER_TTL 秒未收到心跳, 则会自动重连
            所以睡眠间隔(delay)满足   delay * slaver总数  < TTL
            使得一轮循环的时间小于TTL,
            保证每个slaver都在过期前能被心跳保活
        """
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
        """
        handshake before real data transfer
        it ensures:
            1. client is alive and ready for transmission
            2. client is shootback_slaver, not mistakenly connected other program
            3. verify the SECRET_KEY
            4. tell slaver it's time to connect target

        handshake procedure:
            1. master hello --> slaver
            2. slaver verify master's hello
            3. slaver hello --> master
            4. (immediately after 3) slaver connect to target
            4. master verify slaver
            5. enter real data transfer
        """
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
        """assign slaver for customer"""
        while not self._stopped['stop']:
            # get a newly connected customer
            conn_customer, addr_customer,tmp = self.pending_customers.get()

            conn_slaver = self._get_an_active_slaver()
            if conn_slaver is None:
                log.warning("Closing customer[{}] because no available slaver found".format(
                    addr_customer))
                try_close(conn_customer)

                continue
            else:
                log.debug("Using slaver: {} for {}".format(conn_slaver.getpeername(), addr_customer))

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
                "tmp":tmp
            }

            try:
                self._serve_customer(conn_customer, conn_slaver,tmp)
            except Exception as e:
                try:
                    logging.info("_serve_customer fail e:{},{}".format(e,conn_customer))
                    try_close(conn_customer)
                except:
                    pass
                continue

    def _listen_slaver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.try_bind_port(sock, self.communicate_addr)
        sock.listen(10)
        self._listening_sockets.append(sock)
        log.info("Listening for slavers: {}".format(
            fmt_addr(self.communicate_addr)))
        while not self._stopped['stop']:
            logging.info("_listen_slaver stop:{},{}".format(id(self._stopped),self._stopped))
            conn, addr = sock.accept()
            self.slaver_pool.append({
                "addr_slaver": addr,
                "conn_slaver": conn,
            })
            log.info("{} Got slaver {} Total: {}".format(
                self.communicate_addr,fmt_addr(addr), len(self.slaver_pool)
            ))

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.try_bind_port(sock, self.customer_listen_addr)
        sock.listen(20)
        self._listening_sockets.append(sock)
        log.info("Listening for customers: {}".format(
            fmt_addr(self.customer_listen_addr)))
        while not self._stopped['stop']:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, self.pending_customers.qsize() + 1
            ))

            # just put it into the queue,
            #   let _assign_slaver_daemon() do the else
            #   don't block this loop
            self.pending_customers.put((conn_customer, addr_customer,None))

    def add_http_customer(self,conn,addr,tmp):
        log.info("Serving customer: {} Total customers: {}".format(
            conn, self.pending_customers.qsize() + 1
        ))
        self.pending_customers.put((conn, addr,tmp))

class Mastar_line:
    def __init__(self,socketbridge):
        self.__website__ = "https://github.com/aploium/shootback"
        self._listening_sockets = []
        self.SPARE_SLAVER_TTL = 0
        self.SECRET_KEY = ""
        self.socketbridge = socketbridge
        self.master = None

    def dispose(self):
        if self.master:
            self.master.dispose()
        self.SECRET_KEY = ""
        self.socketbridge = None

    def run_master(self,communicate_addr, customer_listen_addr,pkg):
        log.info("shootback {} running as master".format(version_info()))
        # log.info("author: {}  site: {}".format(__author__, __website__))
        log.info("slaver from: {} customer from: {}".format(
            fmt_addr(communicate_addr), fmt_addr(customer_listen_addr)))

        self.master = Master(customer_listen_addr, communicate_addr,self._listening_sockets,pkg,self.socketbridge)
        self.master.serve_forever()


    def main_master(self,args):

        communicate_addr = split_host(args['master'])
        customer_listen_addr = split_host(args['customer'])

        self.SECRET_KEY = args['secretkey']

        self.pkg = CtrlPkg()
        self.pkg.recalc_crc32(self.SECRET_KEY)

        logging.info("main_master,{},{},id:{},self.pkg:{}".format(args['master'],self.SECRET_KEY,id(self),id(self.pkg)))



        local.SPARE_SLAVER_TTL = SPARE_SLAVER_TTL
        # if args.quiet < 2:
        #     if args.verbose:
        #         level = logging.DEBUG
        #     elif args.quiet:
        #         level = logging.WARNING
        #     else:
        #         level = logging.INFO
        configure_logging(logging.INFO)

        self.run_master(communicate_addr, customer_listen_addr,self.pkg)


