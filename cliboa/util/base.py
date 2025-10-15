import warnings
from typing import Optional

from cliboa.util.log import _get_logger


class _BaseObject:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = _get_logger(self.__class__.__module__ + "." + self.__class__.__name__)


def _warn_deprecated(deprecated: str, instead: Optional[str] = None, stacklevel: int = 3) -> None:
    err_mes = f"{deprecated} is deprecated."
    if instead:
        err_mes += (f" Use {instead} instead.",)
    warnings.warn(
        DeprecationWarning,
        stacklevel=stacklevel,
    )
