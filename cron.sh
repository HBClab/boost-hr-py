#!/bin/bash

# cron to run the main pipeline on linux machine then use gh cli to start an actions script
# === setup environment ===

# Source the Conda activate script
source /opt/anaconda3-2024.10-1/etc/profile.d/conda.sh

# Activate Conda env
conda activate boost-hr

# Move to project home dir
cd "$(dirname "$0")"

# grab any new code changes, otherwise skip
git pull --ff-only origin main

# === run the python script ===

cd hr && python main.py 'vosslnx'
cd ..


# === push results to github ===
git add .
git commit -m "automated commit by vosslab linux"
git push

# === run gh workflow ===
# temp for now
