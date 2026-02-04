def getFinalStrike(timeDate, lastIndexTimeData, baseSym, indexPrice, Expiry,
                   AimedOTM, strikeDiff, premLimit1, premiumLimit2, side,
                   getCallSym, getPutSym, fetchAndCacheFnoHistData, strategyLogger=None):
    """
    Optimized function to fetch option symbol with premium within given limits.
    """
    try:
        if side == "CE":
            getSym = getCallSym
            otm = 0
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
        elif side == "PE":
            getSym = getPutSym
            otm = AimedOTM
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
        else:
            return None

        data_obj = fetchAndCacheFnoHistData(sym, lastIndexTimeData)

        while data_obj["c"] > premiumLimit2 or data_obj["c"] < premLimit1:
            if data_obj["c"] > premiumLimit2:
                otm += 1
            elif data_obj["c"] < premLimit1:
                otm -= 1
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
            data_obj = fetchAndCacheFnoHistData(sym, lastIndexTimeData)

        return sym

    except Exception as e:
        if strategyLogger:
            strategyLogger.info(e)
        return None