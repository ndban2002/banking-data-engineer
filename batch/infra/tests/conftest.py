"""Import the Spark-submit job helpers without installing a Python package."""

from pathlib import Path
import sys


JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))
