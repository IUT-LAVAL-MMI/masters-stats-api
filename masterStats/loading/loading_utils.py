def secure_converter(value, dtype, zero_on_error=False):
    try:
        return dtype(value)
    except ValueError as e:
        return 0 if zero_on_error else None