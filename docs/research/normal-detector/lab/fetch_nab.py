"""Fetch the NAB series run_all.py reads, and NAB's labeled incident windows (MIT licensed, github.com/numenta/NAB)."""
import os, sys, urllib.request
from run_all import NAB
BASE = "https://raw.githubusercontent.com/numenta/NAB/master/"
root = sys.argv[1]
for name in NAB:
    path = os.path.join(root, name + ".csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    urllib.request.urlretrieve(BASE + "data/" + name + ".csv", path)
urllib.request.urlretrieve(BASE + "labels/combined_windows.json", os.path.join(root, "labels.json"))
urllib.request.urlretrieve(BASE + "LICENSE.txt", os.path.join(root, "LICENSE.txt"))
