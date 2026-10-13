from importlib import import_module


def test_package_can_be_imported() -> None:
    package = import_module("await_lens")

    assert package.__name__ == "await_lens"
