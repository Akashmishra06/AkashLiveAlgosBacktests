cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_N"
/usr/local/bin/pm2 start "RMA_N_double.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_N_double-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_SS"
/usr/local/bin/pm2 start "RMA_SS.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_SS-1" --no-autorestart --time
