class UndefException(Exception):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name + ' function is not defined'

    def __repr__(self):
        return "UndefException('" + self.name + "')"

