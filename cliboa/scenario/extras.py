import logging


class ExceptionHandler(object):
    def __init__(self):
        super().__init__()
        self._logger = logging.getLogger(__name__)

    def handle_error(self, *args, **kwargs):
        # Please implement in a subclass if you would like to do something.
        self._logger.warning(args)
