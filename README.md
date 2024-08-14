# Factored Datathon 2024

TBD

## Overview

TBD

### Components

TBD

### Data Visualization

TBD

### Data Architecture

TBD 

## Local installation
### Using Conda

```
cd $HOME && git clone git@github.com:glev1/factored-datathon-2024-seed42.git
cd $HOME/factored-datathon-2024-seed42
conda create -n datathon python=3.11
conda activate datathon
pip install -r requirements.txt
```

### Using venv

```
cd $HOME && git clone git@github.com:glev1/factored-datathon-2024-seed42.git
cd $HOME/factored-datathon-2024-seed42
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

1. Run local scrapper
```
cd $HOME/factored-datathon-2024-seed42/data/scraper
python3 local_scraper.py --sd 20240810 --ed 20240813 
```
You can use --sd and --ed to specify retrieval start and end dates.