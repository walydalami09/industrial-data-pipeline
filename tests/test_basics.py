def add(a: int, b: int) -> int:
    return a + b


def test_add() -> None:
    result = add(2, 3)

    assert result == 5