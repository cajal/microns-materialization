import datajoint as dj
from datajoint import datajoint_plus as djp
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

from microns_materialization_api import config
schema_obj = config.SCHEMAS.H01_MATERIALIZATION

config.register_adapters(schema_obj, context=locals())
config.register_externals(schema_obj)

# Schema creation
schema = dj.schema(schema_obj.value)
schema.spawn_missing_classes()