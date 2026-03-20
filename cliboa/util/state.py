class _StateManager:
    def __init__(self):
        self._state: str = "_Initialized"
        self._steps_max: int = 0
        self._steps_current: int = 0
        self._in_steps: bool = False

    @property
    def current(self) -> str:
        if self._in_steps:
            if self._steps_max >= 10:
                return f"[{self._steps_current:02d}/{self._steps_max:02d}]{self._state}"
            else:
                return f"[{self._steps_current}/{self._steps_max}]{self._state}"
        else:
            return self._state

    @property
    def steps_max(self) -> int:
        return self._steps_max

    def __str__(self) -> str:
        return self.current

    def set(self, new_state: str):
        if not isinstance(new_state, str):
            raise TypeError("The state must be a string.")
        self._state = new_state

    def set_steps_max(self, value: int):
        self._steps_max = value

    def set_steps_current(self, value: int):
        self._steps_current = value

    def set_in_steps(self, value: bool):
        self._in_steps = value


state = _StateManager()
