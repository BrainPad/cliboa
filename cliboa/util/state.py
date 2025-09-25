class StateManager:
    def __init__(self):
        self._state: str = "_Initialized"

    @property
    def current(self) -> str:
        return self._state

    def __str__(self) -> str:
        # NOTE: The full format "[05/12]StepName" was compromised.
        # TODO: Revisit this if we do a major refactor.
        return self.current

    def set(self, new_state: str):
        if not isinstance(new_state, str):
            raise TypeError("The state must be a string.")
        self._state = new_state


state = StateManager()
