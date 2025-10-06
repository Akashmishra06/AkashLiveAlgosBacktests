
cd "/root/AkashLiveAlgosBacktests/RMA/RMA_N"
/usr/local/bin/pm2 start "RMA_N.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_N-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/RMA/RMA_SS"
/usr/local/bin/pm2 start "RMA_SS.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_SS-1" --no-autorestart --time
