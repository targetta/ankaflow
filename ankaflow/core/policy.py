class FlowControl:
    ON_ERROR_FAIL = "fail"
    ON_ERROR_WARN = "warn"

    def __init__(self, on_error: str = "fail"):
        self.on_error = on_error
