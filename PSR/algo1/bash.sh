# cd "/root/AkashLiveAlgosBacktests/PSR/algo1/PSR_BN"
# /usr/local/bin/pm2 start "PSR_BN_Without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BN_Without_DSL-1" --no-autorestart --time

# cd "/root/AkashLiveAlgosBacktests/PSR/algo1/PSR_SS"
# /usr/local/bin/pm2 start "PSR_SS_Without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_SS_Without_DSL-1" --no-autorestart --time

# cd "/root/AkashLiveAlgosBacktests/PSR/algo1/PSR_N"
# /usr/local/bin/pm2 start "PSR_N_Without_DSL.py" --interpreter="/root/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_N_Without_DSL-1" --no-autorestart --time


cd "/root/development/AkashLiveAlgosBacktests/PSR/algo1/PSR_BN"
/usr/local/bin/pm2 start "PSR_BN_With_3K_DSL.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_BN_With_3K_DSL-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/PSR/algo1/PSR_SS"
/usr/local/bin/pm2 start "PSR_SS_With_10K_DSL.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_SS_With_10K_DSL-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/PSR/algo1/PSR_N"
/usr/local/bin/pm2 start "PSR_N_With_6K_DSL.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="PSR_N_With_6K_DSL-1" --no-autorestart --time