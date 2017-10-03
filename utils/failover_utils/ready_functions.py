'''
Created on Sep 26, 2017

@author: riteshagarwal
'''
class failover_utils():
    def compare_per_node_for_failovers_consistency(self, map1):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = {}
        for bucket in map1.keys():
            map = {}
            nodeMap = {}
            logic = True
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    if vbucket in map.keys():
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. UUID {3}, Change node {4}. UUID {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['id'], node, id)
                        if int(map[vbucket]['seq']) == int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. seq {3}, Change node {4}. seq {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['seq'], node, seq)
                        if int(map[vbucket]['num_entries']) == int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. num_entries {3}, Change node {4}. num_entries {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['num_entries'], node, num_entries)
                    else:
                        map[vbucket] = {}
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        nodeMap[vbucket] = node
            bucketMap[bucket] = map
        self.assertTrue(logic, output)
        return bucketMap

    def get_failovers_logs(self, servers, buckets):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        servers = self.get_kv_nodes(servers)
        new_failovers_stats = self.data_collector.collect_failovers_stats(buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(new_failovers_stats)
        return new_failovers_stats

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets, perNode=False, comp_map=None):
        """
            Method to compare failovers log information to a previously stored value
        """
        comp_map = {}
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': "<="}
        comp_map["num_entries"] = {'type': "string", 'operation': "<="}

        self.log.info(" Begin Verification for failovers logs comparison ")
        servers = self.get_kv_nodes(servers)
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(prev_failovers_stats, new_failovers_stats,
                                                                            "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(compare_failovers_result, addedItems=False,
                                                                             deletedItems=False, updatedItems=False)
        self.assertTrue(isNotSame, summary)
        self.log.info(" End Verification for failovers logs comparison ")
        return new_failovers_stats

    def compare_per_node_for_failovers_consistency(self, map1, vbucketMap):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = {}
        logic = True
        for bucket in map1.keys():
            map = {}
            tempMap = {}
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    state = vbucketMap[bucket][node][vbucket]['state']
                    if vbucket in map.keys():
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: UUID {4}, Change node {5} {6} UUID {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['id'], node, state, id)
                        if int(map[vbucket]['seq']) != int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: seq {4}, Change node {5} {6}  :: seq {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['seq'], node, state, seq)
                        if int(map[vbucket]['num_entries']) != int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: num_entries {4}, Change node {5} {6} :: num_entries {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['num_entries'], node, state, num_entries)
                    else:
                        map[vbucket] = {}
                        tempMap[vbucket] = {}
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        tempMap[vbucket]['node'] = node
                        tempMap[vbucket]['state'] = state
            bucketMap[bucket] = map
        self.assertTrue(logic, output)
        return bucketMap

    def compare_vbucketseq_failoverlogs(self, vbucketseq={}, failoverlog={}):
        """
            Method to compare failoverlog and vbucket-seq for uuid and  seq no
        """
        isTrue = True
        output = ""
        for bucket in vbucketseq.keys():
            for vbucket in vbucketseq[bucket].keys():
                seq = vbucketseq[bucket][vbucket]['abs_high_seqno']
                uuid = vbucketseq[bucket][vbucket]['uuid']
                fseq = failoverlog[bucket][vbucket]['seq']
                fuuid = failoverlog[bucket][vbucket]['id']
                if seq < fseq:
                    output += "\n Error Condition in bucket {0} vbucket {1}:: seq : vbucket-seq {2} != failoverlog-seq {3}".format(
                        bucket, vbucket, seq, fseq)
                    isTrue = False
                if uuid != fuuid:
                    output += "\n Error Condition in bucket {0} vbucket {1}:: uuid : vbucket-seq {2} != failoverlog-seq {3}".format(
                        bucket, vbucket, uuid, fuuid)
                    isTrue = False
        self.assertTrue(isTrue, output)

    def get_failovers_logs(self, servers, buckets):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        vbucketMap = self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket=True,
                                                               collect_vbucket_seqno=False,
                                                               collect_vbucket_details=False, perNode=True)
        new_failovers_stats = self.data_collector.collect_failovers_stats(buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(new_failovers_stats, vbucketMap)
        return new_failovers_stats

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets, perNode=False):
        """
            Method to compare failovers log information to a previously stored value
        """
        comp_map = {}
        compare = "=="
        if self.withMutationOps:
            compare = "<="
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': compare}
        comp_map["num_entries"] = {'type': "string", 'operation': compare}
        self.log.info(" Begin Verification for failovers logs comparison ")
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(prev_failovers_stats, new_failovers_stats,
                                                                            "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(compare_failovers_result, addedItems=False,
                                                                             deletedItems=False, updatedItems=False)
        self.assertTrue(isNotSame, summary)
        self.log.info(" End Verification for Failovers logs comparison ")
        return new_failovers_stats
