class ConfigError(ValueError):
    """Bad or unusable job configuration. Retrying will never fix it."""