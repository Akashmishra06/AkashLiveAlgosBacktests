
# cd "/root/RMS_Backtest/RMS_N"
# /usr/local/bin/pm2 start "RMS_N.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="RMS_N-11" --no-autorestart --time


# cd "/root/RMS_Backtest/RMS_N"
# /usr/local/bin/pm2 start "RMS_N_doubleConfermation.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="RMS_N_doubleConfermation-11" --no-autorestart --time


cd "/root/RMS_Backtest/RMS_N"
/usr/local/bin/pm2 start "RMS_N_dynamic_rsi.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="RMS_N_dynamic_rsi-11" --no-autorestart --time
