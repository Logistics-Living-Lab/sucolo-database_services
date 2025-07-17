from pytest_mock import MockerFixture as MockerFixture

from sucolo_database_services.data_access import DataAccess


def test_city_not_found(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
def test_invalid_radius() -> None: ...
def test_invalid_penalty() -> None: ...


def test_get_all_indices(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...


def test_get_hexagon_static_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...


def test_error_handling(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...


def test_get_multiple_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
