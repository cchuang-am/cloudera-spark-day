from psk.service import core_service


def test_get_app_name():
    app_name = core_service.get_app_name()

    assert app_name != 'app'