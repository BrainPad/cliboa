class Helper(object):
    @staticmethod
    def set_property(cls, method, value):
        """
        This method allows you to set a value
        to the class with either method directly or via property setter.
        Either way, the method must be implemented
        to set the value for the class parameter like below.

        -- eg1 --
        class Foo():
            def __init__(self):
                self._bar = None

            def bar(bar):
                self._bar = bar

        -- eg2 --
        class Foo2():
            def __init__(self):
                self._bar = None

            @property
            def bar(self):
                return self._bar

            @bar.setter
            def bar(self, bar):
                self._bar = bar

        Arguments
            cls (Class): Class instance
            method (str): method name
            value (any): value to set
        """
        attr = getattr(type(cls), method, None)
        if isinstance(attr, property):
            # Set property via property.setter
            setattr(cls, method, value)
        else:
            # Set property directly by method
            call = getattr(cls, method, None)
            if call is not None:
                call(value)
