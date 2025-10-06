cd "/root/AkashLiveAlgosBacktests/PSR/algo3/PSR_BIB_BN"
/usr/local/bin/pm2 start "PSR_BIB_BN_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BIB_BN_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo3/PSR_BIB_SS"
/usr/local/bin/pm2 start "PSR_BIB_SS_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BIB_SS_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo3/PSR_BIB_N"
/usr/local/bin/pm2 start "PSR_BIB_N_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BIB_N_without_DSL-1" --no-autorestart --time
