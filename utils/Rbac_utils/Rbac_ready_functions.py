'''
Created on Dec 1, 2017

@author: riteshagarwal
'''
from security.rbac_base import RbacBase
from membase.api.rest_client import RestConnection

class rbac_utils():
    def __init__(self, master):
        self.master = master
        self.rest = RestConnection(master)
        
    def _create_user_and_grant_role(self, username, role, source='builtin'):
        user = [{'id':username,'password':'password','name':'Some Name'}]
        response = RbacBase().create_user_source(user,source,self.master)
        user_role_list = [{'id':username,'name':'Some Name','roles':role}]
        response = RbacBase().add_user_role(user_role_list, self.rest, source)

    def _drop_user(self, username):
        user = [username]
        response = RbacBase().remove_user_role(user,self.rest)
