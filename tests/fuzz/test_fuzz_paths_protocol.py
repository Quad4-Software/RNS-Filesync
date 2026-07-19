"""Fuzz tests for paths and protocol."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rns_filesync import protocol
from rns_filesync.paths import PathJailError, normalize_relpath

pytestmark = pytest.mark.fuzz


@given(st.text(min_size=1, max_size=64))
def test_normalize_never_accepts_dotdot(text):
    try:
        result = normalize_relpath(text)
    except PathJailError:
        return
    assert ".." not in result.split("/")
    assert result not in (".", "")


@given(
    st.dictionaries(
        st.sampled_from(
            [
                protocol.MSG_FILE_LIST,
                protocol.MSG_FILE_REQUEST,
                protocol.MSG_DELTA_REQUEST,
                protocol.MSG_EMPTY_FILE,
                protocol.MSG_FILE_UPDATE,
                protocol.MSG_FILE_DELETION,
                protocol.MSG_FILE_LIST_REQUEST,
            ],
        ),
        st.one_of(st.none(), st.text(max_size=20), st.integers(0, 100), st.booleans()),
        min_size=0,
        max_size=5,
    ),
)
def test_encode_decode_known_types(extra):
    # Force a valid type key
    payload = {"type": protocol.MSG_FILE_LIST_REQUEST, "browser": False}
    payload.update({k: v for k, v in extra.items() if k != "type"})
    raw = protocol.encode_message(payload)
    decoded = protocol.decode_message(raw)
    assert decoded["type"] in protocol.KNOWN_TYPES


@given(st.binary(min_size=0, max_size=64))
def test_decode_garbage_does_not_crash(raw):
    try:
        protocol.decode_message(raw)
    except Exception:
        pass
