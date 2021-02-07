# NOTE: this should inherit from (object) to function correctly with python 2.7
class CachedProperty(object):
    """ A property that is only computed once per instance and
    then stores the result in _cached_properties of the object.

        Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
        """

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self

        propname = self.func.__name__

        if not hasattr(obj, '_cached_properties'):
            obj._cached_properties = {}

        if propname not in obj._cached_properties:
            obj._cached_properties[propname] = self.func(obj)
            # value = obj.__dict__[propname] = self.func(obj)

        return obj._cached_properties[propname]

    @staticmethod
    def clear(obj):
        """clears cache of obj"""
        if hasattr(obj, '_cached_properties'):
            obj._cached_properties = {}

    @staticmethod
    def is_cached(obj, propname):
        if hasattr(obj, '_cached_properties') and propname in obj._cached_properties:
            return True
        else:
            return False