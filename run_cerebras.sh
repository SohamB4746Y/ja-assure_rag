#!/bin/bash
source venv/bin/activate
export CEREBRAS_API_KEY="csk-rc34c4eyhpwjjv25y3df4ev9em3emm96e4wkr5nh4hyhkxdk"
export CEREBRAS_MODEL="gpt-oss-120b"
export TRANSFORMERS_OFFLINE=1
export HF_HUB_OFFLINE=1
python main.py
