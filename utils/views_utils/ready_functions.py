'''
Created on Sep 26, 2017

@author: riteshagarwal
'''
class views_utils():
    def async_create_views(self, server, design_doc_name, views, bucket="default", with_query=True,
                           check_replication=False):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket, with_query,
                                                    check_replication=check_replication)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(server, design_doc_name, None, bucket, with_query,
                                                check_replication=check_replication)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default", timeout=None, check_replication=False):
        if len(views):
            for view in views:
                self.cluster.create_view(server, design_doc_name, view, bucket, timeout,
                                         check_replication=check_replication)
        else:
            self.cluster.create_view(server, design_doc_name, None, bucket, timeout,
                                     check_replication=check_replication)

    def make_default_views(self, prefix, count, is_dev_ddoc=False, different_map=False):
        ref_view = self.default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in xrange(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}' % str(i),
                                  None, is_dev_ddoc))
            return views
        else:
            return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in xrange(count)]
