cd "/root/AkashLiveAlgosBacktests/PSR/algo1/PSR_BN"
/usr/local/bin/pm2 start "PSR_BN_Without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BN_Without_DSL-1" --no-autorestart --time
