import logging
import traceback


def tracebacker(func):
    """
    A decorator for getting more informative tracebacks when debugging Jupyter cells.
    """

    def wrapper():
        try:
            func()
        except Exception as exception:
            print(exception)
            traceback.print_exc()

    return wrapper


def create_logger():
    """
    Create logger for diagnostic
    """
    # create logger
    loggr = logging.getLogger("eg-etl")
    loggr.setLevel(logging.DEBUG)

    # monkey patch
    def emit(self, record):
        msg = self.format(record)
        fs = "%s" if getattr(record, "continued", False) else "%s\n"
        self.stream.write = print
        self.stream.write(fs % msg)
        self.flush()

    logging.StreamHandler.emit = emit

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)s - %(funcName)20s() ] - %(message)s"
    )

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    loggr.addHandler(ch)
    return loggr
