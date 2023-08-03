def build_connection_string(
    driver, server, database, uid, pwd, port="1433", extra_connection_params=""
):
    return f"Driver={driver};Server={server},{port};Database={database};Uid={uid};Pwd={pwd};{extra_connection_params}"


def authenticated(func):
    def wrapper(self, *args, **kwargs):
        if not self.authenticated:
            self.authenticate()
        return func(self, *args, **kwargs)

    return wrapper


def connected(func):
    def wrapper(self, *args, **kwargs):
        if not self.connected:
            self.connect()
        return func(self, *args, **kwargs)

    return wrapper
