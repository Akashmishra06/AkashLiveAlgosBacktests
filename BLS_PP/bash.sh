cd "/root/AkashLiveAlgosBacktests/BLS_PP"
/usr/local/bin/pm2 start "BLS_PP.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="BLS_PP-1" --no-autorestart --time
