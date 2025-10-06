# cd "/root/PMS/PSR/PSR_BN"
# /usr/local/bin/pm2 start "PSR_BN_Without_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_BN_Without_DSL-11" --no-autorestart --time

# cd "/root/PMS/PSR/PSR_SS"
# /usr/local/bin/pm2 start "PSR_SS_Without_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_SS_Without_DSL-11" --no-autorestart --time

cd "/root/PMS/PSR/PSR_N"
/usr/local/bin/pm2 start "PSR_N_Without_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_N_Without_DSL-11" --no-autorestart --time


# cd "/root/PMS/PSR/PSR_BN"
# /usr/local/bin/pm2 start "PSR_BN_With_3K_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_BN_With_3K_DSL-11" --no-autorestart --time

# cd "/root/PMS/PSR/PSR_SS"
# /usr/local/bin/pm2 start "PSR_SS_With_10K_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_SS_With_10K_DSL-11" --no-autorestart --time

cd "/root/PMS/PSR/PSR_N"
/usr/local/bin/pm2 start "PSR_N_With_6K_DSL.py" --interpreter="/root/akashResearchAndDevelopment/..venv/bin/python3" --name="PSR_N_With_6K_DSL-11" --no-autorestart --time