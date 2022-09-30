import pandas as pd
import numpy as np
from time import time
from pathlib import Path


def deal_space(s):
    if isinstance(s, float) and np.isnan(s):
        return s
    elif isinstance(s, float) or isinstance(s, float):
        return str(s).strip()
    elif isinstance(s, str):
        return s.strip()
    return s

def deal_blank(data,cols):
    for col in cols:
        data[col] = data[col].map(deal_space)
    return data

ti = time()
if __name__ == "__main__":
    path = Path("C:/Share/spd_test/testing_file/")
    # paths: List[Path] = [Path(path) for path in glob(path + "*.csv")]
    paths = list(path.glob("*.csv"))
    print(paths[:5])

    for path in paths:
        path = pd.read_csv(path)
        deal_blank(path,["出站"])
print(f"INFO: total elapsed time={(time() - ti) / 60.} (minutes)")