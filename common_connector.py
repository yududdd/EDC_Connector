'''
Created on Mar 11, 2021

@author: Bill_Zhang
'''

class ConnectorBase(object):
    configuration = None
        
          
    def config(self, configuration):
        self.configuration = configuration
    
    def connect(self):
        # empty
        pass
    
    def disconnect(self):
        # empty
        pass
    
    def upload(self, *params):
        # empty
        pass
    
    def download(self, *params):
        # empty
        pass
