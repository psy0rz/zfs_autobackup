#Used for baseclasses that dont implement their own logging (Like ExecuteNode)
#Usually logging is implemented in subclasses (Like ZfsNode thats a subclass of ExecuteNode), but for regression testing its nice to have these stubs.

class LogStub:
    """Just a stub, usually overriden in subclasses."""

    # simple logging stubs
    def debug(self, txt):
        print("DEBUG  : " + txt)

    def verbose(self, txt):
        print("VERBOSE: " + txt)

    def error(self, txt):
        print("ERROR  : " + txt)