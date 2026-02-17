from configparser import ConfigParser
from .logicLevelExecute import Strategy


def main(client="01623", ipad_name='overnight'):
    configfile = "/root/Executor_RMS/logics/portfolio_ipad/config.ini"
    config = ConfigParser()
    config.read(configfile)

    baseSym = config.get('strategyParameters', 'baseSym')

    runAlgo = Strategy(client)
    runAlgo.run_strategy(baseSym, client)


if __name__ == "__main__":
    main(clientID="01623", ipad_name='overnight')
