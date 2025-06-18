class ComponentFailureException(Exception):
    """Custom exception for when a component fails too many times in a short time period"""
    def __init__(self, message, component_name):
        self.message = message
        self.component_name = component_name
        super().__init__(message)
