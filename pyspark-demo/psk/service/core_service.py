from os import environ


def get_app_name():
    app_name = environ.get('APP_NAME')

    return app_name if app_name else 'app'