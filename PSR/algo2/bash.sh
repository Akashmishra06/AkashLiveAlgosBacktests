cd "/root/AkashLiveAlgosBacktests/PSR/algo2/PSR_HL_BN"
/usr/local/bin/pm2 start "PSR_HL_BN_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_HL_BN_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo2/PSR_HL_SS"
/usr/local/bin/pm2 start "PSR_HL_SS_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_HL_SS_without_DSL-1" --no-autorestart --time

cd "/root/AkashLiveAlgosBacktests/PSR/algo2/PSR_HL_N"
/usr/local/bin/pm2 start "PSR_HL_N_without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_HL_N_without_DSL-1" --no-autorestart --time
