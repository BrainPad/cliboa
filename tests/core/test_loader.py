from typing import Any

import pytest

from cliboa.core.loader import _ScenarioLoader
from cliboa.util.exception import FileNotFound, ScenarioFileInvalid


class _DummyScenarioLoader(_ScenarioLoader):
    def __init__(self, scenario_file: str, is_required: bool = False, **kwargs):
        self._mock_data = kwargs.pop("mock_data", None)
        super().__init__(scenario_file, is_required, **kwargs)

    def _load(self) -> Any:
        return self._mock_data


class TestScenarioLoader:
    """
    Unit tests for the _ScenarioLoader class using a _DummyScenarioLoader.
    """

    DUMMY_FILE_PATH = "dummy/scenario.yml"

    def test_init_required_file_not_found_raises_exception(self, mocker):
        mocker.patch("os.path.isfile", return_value=False)

        with pytest.raises(FileNotFound) as exc_info:
            _DummyScenarioLoader(self.DUMMY_FILE_PATH, is_required=True)

        assert self.DUMMY_FILE_PATH in str(exc_info.value)

    def test_init_and_call_not_required_file_not_found(self, mocker):
        mocker.patch("os.path.isfile", return_value=False)
        loader = _DummyScenarioLoader(self.DUMMY_FILE_PATH, is_required=False)
        result = loader()
        assert result is None

    def test_call_load_returns_non_dict_raises_exception(self, mocker):
        mocker.patch("os.path.isfile", return_value=True)
        invalid_data = "this is a string, not a dict"
        loader = _DummyScenarioLoader(
            self.DUMMY_FILE_PATH, is_required=False, mock_data=invalid_data
        )

        with pytest.raises(ScenarioFileInvalid) as exc_info:
            loader()
        assert self.DUMMY_FILE_PATH in str(exc_info.value)
        assert "invalid" in str(exc_info.value)

    def test_call_load_returns_dict_returns_dict(self, mocker):
        mocker.patch("os.path.isfile", return_value=True)
        expected_dict = {"step": "test", "key": "value"}
        loader = _DummyScenarioLoader(
            self.DUMMY_FILE_PATH, is_required=False, mock_data=expected_dict
        )

        result = loader()
        assert result == expected_dict
        assert isinstance(result, dict)
