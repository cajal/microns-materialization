import datajoint as dj
import cloudvolume

import numpy as np
import datetime
import pandas as pd
import traceback
import sys
from pathlib import Path
import time


if 'ipykernel' in sys.modules:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

import microns_materialization_config as config
schema_name = 'microns_h01_materialization'

config.register_adapters(schema_name, context=locals())
config.register_externals(schema_name)

# Schema creation
schema = dj.schema(schema_name)
schema.spawn_missing_classes()