cd "/root/development/AkashLiveAlgosBacktests/base"
/usr/local/bin/pm2 start "MTPR_M_N.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="MTPR_M_N-11" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/base"
/usr/local/bin/pm2 start "MTPR_W_SS.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="MTPR_M_SS-11" --no-autorestart --time