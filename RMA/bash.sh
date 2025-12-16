cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_N_1Min"
/usr/local/bin/pm2 start "RMA_N_1Min.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_N_1Min-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_SS_1Min"
/usr/local/bin/pm2 start "RMA_SS_1Min.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_SS_1Min-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_N_2Min"
/usr/local/bin/pm2 start "RMA_N_2Min.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_N_2Min-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/RMA/RMA_SS_2Min"
/usr/local/bin/pm2 start "RMA_SS_2Min.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="RMA_SS_2Min-1" --no-autorestart --time
