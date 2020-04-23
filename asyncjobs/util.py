def fate(future):
    """Return a word describing the state of the given future."""
    if not future.done():
        return 'unfinished'
    elif future.cancelled():
        return 'cancelled'
    elif future.exception() is not None:
        return 'failed'
    else:
        return 'success'
