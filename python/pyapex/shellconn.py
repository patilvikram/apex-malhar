

from py4j.java_gateway import JavaGateway

class ShellConnector(object):
    gateway = None
    entry_point = None

    def __init__(self):
        self.gateway = JavaGateway()

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ShellConnector, cls).__new__(cls)

        return cls.instance

    def get_jvm_gateway(self):
        return self.gateway

    def get_entry_point(self):
        return self.gateway.entry_point
