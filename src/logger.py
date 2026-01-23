import logging
import os
from pathlib import Path
from typing import Optional
from datetime import datetime
from logging.handlers import RotatingFileHandler


class Logger:
    def __init__(self):
        self.logpath = Path(os.path.dirname(os.path.abspath(__file__)))
        pass