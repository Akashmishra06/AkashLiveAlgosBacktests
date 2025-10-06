cd "/root/PSR/algo5/PSR_3PM_BN"
/usr/local/bin/pm2 start "PSR_3PM_BN.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_3PM_BN-11" --no-autorestart --time

cd "/root/PSR/algo5/PSR_3PM_N"
/usr/local/bin/pm2 start "PSR_3PM_N.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_3PM_N-11" --no-autorestart --time

cd "/root/PSR/algo5/PSR_3PM_SS"
/usr/local/bin/pm2 start "PSR_3PM_SS.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_3PM_SS-11" --no-autorestart --time
