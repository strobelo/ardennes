from os import path
import logging.config
from ardennes.core import Ardennes

log_file_path = path.join(path.dirname(path.abspath(__file__)), "logging.conf")
logging.config.fileConfig(log_file_path)
