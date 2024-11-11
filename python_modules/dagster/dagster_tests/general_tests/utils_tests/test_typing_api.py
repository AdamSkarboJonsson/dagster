import typing

from dagster._utils.typing_api import (
    flatten_unions,
    get_optional_inner_type,
    is_closed_python_dict_type,
    is_closed_python_list_type,
    is_closed_python_optional_type,
    is_closed_python_set_type,
    is_closed_python_tuple_type,
    is_typing_type,
)


def test_closed_python_dict():
    assert is_closed_python_dict_type(dict[str, int]) is True

    # Documenting current behavior -- it seems possible that this is not intended
    assert is_closed_python_dict_type(dict[str, tuple]) is True
    assert is_closed_python_dict_type(dict[str, list]) is True
    assert is_closed_python_dict_type(dict[str, dict]) is True
    assert is_closed_python_dict_type(dict[str, dict[str, dict]]) is True
    assert is_closed_python_dict_type(dict[str, typing.Optional[dict]]) is True

    assert is_closed_python_dict_type(dict) is False
    assert is_closed_python_dict_type(dict) is False
    assert is_closed_python_dict_type(None) is False
    assert is_closed_python_dict_type(1) is False
    assert is_closed_python_dict_type("foobar") is False
    assert is_closed_python_dict_type(typing.Optional) is False
    assert is_closed_python_dict_type(list) is False


def test_is_typing_optional_py_3():
    assert is_closed_python_optional_type(typing.Optional[int])
    assert not is_closed_python_optional_type(typing.Optional)
    assert not is_closed_python_optional_type(None)
    assert not is_closed_python_optional_type(int)
    assert not is_closed_python_optional_type(list)
    assert not is_closed_python_optional_type("foobar")


def test_get_inner_optional_py_3():
    assert get_optional_inner_type(typing.Optional[int]) is int


def test_closed_tuple_type():
    assert is_closed_python_tuple_type(tuple[int, str]) is True

    # Documenting current behavior -- it seems possible that this is not intended
    assert is_closed_python_tuple_type(tuple[str, tuple]) is True
    assert is_closed_python_tuple_type(tuple[str, list]) is True
    assert is_closed_python_tuple_type(tuple[str, dict]) is True
    assert is_closed_python_tuple_type(tuple[str, dict[str, dict]]) is True
    assert is_closed_python_tuple_type(tuple[str, typing.Optional[dict]]) is True

    assert is_closed_python_tuple_type(tuple) is False
    assert is_closed_python_tuple_type(tuple) is False
    assert is_closed_python_tuple_type(1) is False
    assert is_closed_python_tuple_type("foobar") is False
    assert is_closed_python_tuple_type(typing.Optional) is False
    assert is_closed_python_tuple_type(list) is False


def test_closed_set_type():
    assert is_closed_python_set_type(set[int]) is True
    assert is_closed_python_set_type(set) is False
    assert is_closed_python_set_type(set) is False
    assert is_closed_python_set_type(1) is False
    assert is_closed_python_set_type("foobar") is False
    assert is_closed_python_set_type(typing.Optional) is False
    assert is_closed_python_set_type(list) is False
    assert is_closed_python_set_type(dict) is False
    assert is_closed_python_set_type(dict[int, str]) is False
    assert is_closed_python_set_type(tuple) is False
    assert is_closed_python_set_type(tuple[int, str]) is False

    # Documenting current behavior -- it seems possible that this is not intended
    assert is_closed_python_set_type(set[tuple]) is True
    assert is_closed_python_set_type(set[list]) is True
    assert is_closed_python_set_type(set[dict]) is True
    assert is_closed_python_set_type(set[dict[str, dict]]) is True
    assert is_closed_python_set_type(set[typing.Optional[dict]]) is True


def test_closed_list_type():
    assert is_closed_python_list_type(list[int]) is True

    assert is_closed_python_list_type(list) is False
    assert is_closed_python_list_type(list) is False
    assert is_closed_python_list_type(None) is False
    assert is_closed_python_list_type(1) is False
    assert is_closed_python_list_type("foobar") is False
    assert is_closed_python_list_type(typing.Optional) is False
    assert is_closed_python_list_type(dict) is False
    assert is_closed_python_list_type(dict[int, str]) is False
    assert is_closed_python_list_type(tuple) is False
    assert is_closed_python_list_type(tuple[int, str]) is False


def test_is_typing_type():
    assert is_typing_type("foobar") is False
    assert is_typing_type(1) is False
    assert is_typing_type(dict) is False
    assert is_typing_type(int) is False
    assert is_typing_type(list) is False
    assert is_typing_type(None) is False
    assert is_typing_type(set) is False
    assert is_typing_type(tuple) is False
    assert is_typing_type(dict) is True
    assert is_typing_type(dict[int, str]) is True
    assert is_typing_type(dict[str, dict[str, dict]]) is True
    assert is_typing_type(dict[str, dict]) is True
    assert is_typing_type(dict[str, list]) is True
    assert is_typing_type(dict[str, typing.Optional[dict]]) is True
    assert is_typing_type(dict[str, tuple]) is True
    assert is_typing_type(list) is True
    assert is_typing_type(list[int]) is True
    assert is_typing_type(typing.Optional) is False
    assert is_typing_type(typing.Optional[int]) is True
    assert is_typing_type(set) is True
    assert is_typing_type(set[int]) is True
    assert is_typing_type(set[dict[str, dict]]) is True
    assert is_typing_type(set[dict]) is True
    assert is_typing_type(set[list]) is True
    assert is_typing_type(set[typing.Optional[dict]]) is True
    assert is_typing_type(set[tuple]) is True
    assert is_typing_type(tuple) is True
    assert is_typing_type(tuple[int, str]) is True
    assert is_typing_type(tuple[str, dict[str, dict]]) is True
    assert is_typing_type(tuple[str, dict]) is True
    assert is_typing_type(tuple[str, list]) is True
    assert is_typing_type(tuple[str, typing.Optional[dict]]) is True
    assert is_typing_type(tuple[str, tuple]) is True


def test_flatten_unions() -> None:
    assert flatten_unions(str) == {str}
    assert flatten_unions(typing.Union[str, float]) == {str, float}
    assert flatten_unions(typing.Union[str, float, int]) == {str, float, int}
    assert flatten_unions(typing.Optional[str]) == {str, type(None)}
    assert flatten_unions(typing.Optional[typing.Union[str, float]]) == {
        str,
        float,
        type(None),
    }
    assert flatten_unions(typing.Union[typing.Union[str, float], int]) == {
        str,
        float,
        int,
    }
    assert flatten_unions(typing.Any) == {typing.Any}  # type: ignore
