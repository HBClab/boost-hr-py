#!/bin/bash

# cron to run the main pipeline on linux machine then use gh cli to start an actions script
# === setup environment ===
# set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source /opt/anaconda3-2024.10-1/etc/profile.d/conda.sh
conda activate boost-hr

cd "${REPO_ROOT}"

# grab any new code changes, otherwise skip
git pull --ff-only origin main

# === run the python script ===

python hr/main.py 'vosslnx'


# === push results to github ===
git add .
git commit -m "automated commit by vosslab linux"
git push

# === run gh workflow ===
# temp for now
