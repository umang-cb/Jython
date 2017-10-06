'''
Created on Sep 26, 2017

@author: riteshagarwal
'''
import logger
import unittest
import copy
import datetime
import time
import string
import random
import logging
import json
import commands
import mc_bin_client
import traceback
from memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.cluster import Cluster
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.stats_tools import StatsCommon
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.exception import ServerUnavailableException
from couchbase_helper.data_analysis_helper import *
from testconstants import STANDARD_BUCKET_PORT
from testconstants import MIN_COMPACTION_THRESHOLD
from testconstants import MAX_COMPACTION_THRESHOLD
from membase.helper.cluster_helper import ClusterOperationHelper
from security.rbac_base import RbacBase
from couchbase_cli import CouchbaseCLI
import testconstants
from scripts.collect_server_info import cbcollectRunner

class node_utils():
    def kill_server_memcached(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.kill_memcached()
                remote_client.disconnect()

    def start_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                RemoteUtilHelper.enable_firewall(server)

    def stop_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.disable_firewall()
                remote_client.disconnect()

    def get_victim_nodes(self, nodes, master=None, chosen=None, victim_type="master", victim_count=1):
        victim_nodes = [master]
        if victim_type == "graceful_failover_node":
            victim_nodes = [chosen]
        elif victim_type == "other":
            victim_nodes = self.add_remove_servers(nodes, nodes, [chosen, self.master], [])
            victim_nodes = victim_nodes[:victim_count]
        return victim_nodes

    def get_services_map(self, reset=True, master=None):
        if not reset:
            return
        else:
            self.services_map = {}
        if not master:
            master = self.master
        rest = RestConnection(master)
        map = rest.get_nodes_services()
        for key, val in map.iteritems():
            for service in val:
                if service not in self.services_map.keys():
                    self.services_map[service] = []
                self.services_map[service].append(key)

    def get_nodes_services(self):
        list_nodes_services = {}
        rest = RestConnection(self.master)
        map = rest.get_nodes_services()
        for k, v in map.iteritems():
            if "8091" in k:
                k = k.replace(":8091", "")
            if len(v) == 1:
                v = v[0]
            elif len(v) > 1:
                v = ",".join(v)
            list_nodes_services[k] = v
        """ return {"IP": "kv", "IP": "index,kv"} """
        return list_nodes_services

    def _kill_nodes(self, nodes, servers, bucket_name):
        self.reboot = self.input.param("reboot", False)
        if not self.reboot:
            for node in nodes:
                _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                         "password": self.servers[0].rest_password}
                node_rest = RestConnection(_node)
                _mc = MemcachedClientHelper.direct_client(_node, bucket_name)
                self.log.info("restarted the node %s:%s" % (node.ip, node.port))
                pid = _mc.stats()["pid"]
                command = "os:cmd(\"kill -9 {0} \")".format(pid)
                self.log.info(command)
                killed = node_rest.diag_eval(command)
                self.log.info("killed ??  {0} ".format(killed))
                _mc.close()
        else:
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                command = "reboot"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()
                time.sleep(self.wait_timeout * 8)
                shell = RemoteMachineShellConnection(server)
                command = "/sbin/iptables -F"
                output, error = shell.execute_command(command)
                command = "iptables -F"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()

    def _restart_memcache(self, bucket_name):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        is_partial = self.input.param("is_partial", "True")
        _nodes = []
        if len(self.servers) > 1:
            skip = 2
        else:
            skip = 1
        if is_partial:
            _nodes = nodes[:len(nodes):skip]
        else:
            _nodes = nodes

        _servers = []
        for server in self.servers:
            for _node in _nodes:
                if server.ip == _node.ip:
                    _servers.append(server)

        self._kill_nodes(_nodes, _servers, bucket_name)

        start = time.time()
        memcached_restarted = False

        for server in _servers:
            while time.time() - start < (self.wait_timeout * 2):
                mc = None
                try:
                    mc = MemcachedClientHelper.direct_client(server, bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    self.log.info("New warmup uptime %s:%s" % (
                        new_uptime, self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]))
                    if new_uptime < self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                    if mc:
                        mc.close()
                    time.sleep(5)
            if not memcached_restarted:
                self.fail("memcached did not start %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                
    def wait_service_started(self, server, wait_time=120):
        shell = RemoteMachineShellConnection(server)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, error = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                self.log.info("Couchbase service is running")
                shell.disconnect()
                return
            else:
                self.log.warn("couchbase service is not running. {0}".format(output))
                self.sleep(10)
        shell.disconnect()
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))


    '''
    Returns ip address of the requesting machine
    '''

    def getLocalIPAddress(self):
        status, ipAddress = commands.getstatusoutput(
            "ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress


    # get the dot version e.g. x.y
    def _get_version(self):
        rest = RestConnection(self.master)
        version = rest.get_nodes_self().version
        return float(version[:3])

    def wait_node_restarted(self, server, wait_time=120, wait_if_warmup=False, check_service=False):
        now = time.time()
        if check_service:
            self.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [server], self, wait_time=wait_time - num * 10, wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                self.sleep(10)
