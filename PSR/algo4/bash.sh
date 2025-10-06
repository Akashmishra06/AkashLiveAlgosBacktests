cd "/root/AkashLiveAlgosBacktests/PSR/algo4/PSR_123_BN"
/usr/local/bin/pm2 start "PSR_123_BN_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_123_BN_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo4/PSR_123_N"
/usr/local/bin/pm2 start "PSR_123_N_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_123_N_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo4/PSR_123_SS"
/usr/local/bin/pm2 start "PSR_123_SS_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_123_SS_without_DSL-1" --no-autorestart --time
