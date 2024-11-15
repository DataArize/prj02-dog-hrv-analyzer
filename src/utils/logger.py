import logging


def create_logger(name: str, log_level: str = "DEBUG"):
    """
    Creates and configures a logger.

    :param name: Name of the logger (typically the module or script name).
    :param log_level: The log level, default is DEBUG.
    :return: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level.upper())  # Set log level dynamically
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level.upper())
    console_handler.setFormatter(formatter)

    # Optional: File handler to log into a file (uncomment to use)
    # file_handler = logging.FileHandler('app.log')
    # file_handler.setLevel(log_level.upper())
    # file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    # logger.addHandler(file_handler)  # Uncomment to log to a file

    return logger
