# Trajectory Data Tools

This repository contains tools for processing, exporting, and analyzing vehicle trajectory data. The tools support converting proprietary `.tppkl` data into standard CSV and Parquet formats, updating metadata, and visualizing spacetime diagrams.

## Installation

1. Clone this repository.
2. Install the required Python packages using `pip`:

```bash
pip install -r requirements.txt
```

**Requirements:**

- Python 3.x
- `numpy`
- `pandas`
- `pyarrow`
- `matplotlib`
- `jupyterlab` (for running the notebook)

## Files and Usage



### `update_parquet_meta.py`

A utility script to update the embedded metadata in a Parquet file using a JSON file. This is useful if you manually modify the metadata (e.g., in a generated JSON file) and want to sync it back to the Parquet file without re-processing the raw data.

**Usage:**

```bash
# Update metadata in place (overwrites the parquet file)
python update_parquet_meta.py data/example.parquet data/example.json

# Update and save to a new file
python update_parquet_meta.py data/example.parquet data/example.json --output data/example_updated.parquet
```

### `data_tools.py`

Contains helper functions to read the exported Parquet data and visualize it. It can also be run as a standalone script to generate spacetime diagrams for each lane.

**Features:**

- `read_parquet(path)`: Reads Parquet files and restores the data to a dictionary format, including metadata.
- `plot_trajectory_spacetime_diagram(data, meta)`: Generates time-space diagrams for each lane, coloring trajectories by speed.

**Usage:**

```bash
# Generate spacetime diagrams from a Parquet file
python data_tools.py data/example.parquet
```

The output images will be saved in the `fig/` directory.

### `read_parquet_data_freeway.ipynb`

A Jupyter Notebook that demonstrates how to read and analyze the Parquet data interactively. It typically uses functions from `data_tools.py` to load data and perform exploratory data analysis.

**Usage:**

Open the notebook in Jupyter Lab or Notebook:

```bash
jupyter lab read_parquet_data_freeway.ipynb
```
