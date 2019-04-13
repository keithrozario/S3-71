#! /bin/bash
cd serverless
sls deploy
cd ..
python -m venv venv/
source venv/bin/activate
pip install -r requirements.txt