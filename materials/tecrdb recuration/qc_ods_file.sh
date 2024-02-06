#!/bin/bash
cd "/home/r.giessmann/2024-01-00_openTECR_recuration"
echo "downloading Google Docs..."
wget -O "openTECR recuration.ods" "https://docs.google.com/spreadsheets/d/1jLIxEXVzE2SAzIB0UxBfcFoHrzjzf9euB6ART2VDE8c/export?format=ods"
echo "...done."

eval "$(conda shell.bash hook)"
conda activate py311
python 2024-01-06-opentecr-recuration-quality-check-and-extract-table-data.py
if [ $? -eq 0 ]; then
  open `pwd`
fi
