#! /bin/bash

# SLS Deployment
cd serverless
sls deploy

# Python Setup
cd ..
python -m venv venv/
source venv/bin/activate
pip install -r requirements.txt
