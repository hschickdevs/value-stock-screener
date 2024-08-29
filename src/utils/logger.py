import logging

from utils.misc import get_logfile

formatter = logging.Formatter('%(module)s : %(levelname)s : %(asctime)s : %(message)s')

# Get logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Get logfile name & setup file handler
file_handler = logging.FileHandler(get_logfile())
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Setup stream handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(stream_handler)