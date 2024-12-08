# %%
import requests
import pandas as pd
import numpy as np
import os
from pathlib import Path
from dotenv import load_dotenv
from countries import *
from difflib import get_close_matches
from io import StringIO
from typing import Tuple, Literal, Optional

# Set environment
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)
env = os.getenv("env")

# %%
