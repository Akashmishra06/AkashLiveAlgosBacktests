cd "/root/AkashLiveAlgosBacktests/PSR/algo5/PSR_3PM_BN"
/usr/local/bin/pm2 start "PSR_3PM_BN.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_3PM_BN-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo5/PSR_3PM_N"
/usr/local/bin/pm2 start "PSR_3PM_N.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_3PM_N-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo5/PSR_3PM_SS"
/usr/local/bin/pm2 start "PSR_3PM_SS.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_3PM_SS-1" --no-autorestart --time
