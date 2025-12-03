cd "/root/development/AkashLiveAlgosBacktests/BLS50_H30K"
/usr/local/bin/pm2 start "BLS50_H30K.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="BLS50_H30K-1" --no-autorestart --time

cd "/root/development/AkashLiveAlgosBacktests/BLS50_V30K"
/usr/local/bin/pm2 start "BLS50_V30K.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="BLS50_V30K-1" --no-autorestart --time
