from typing import List, Optional
from datetime import datetime, timezone, timedelta, time
from system.strategy import BaseStrategyFast, Signal
import polars as pl  # type: ignore


class SensexOvernightStrategyFast(BaseStrategyFast):

    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)

        # ---- Config ----
        self.quantity = self.config.get("quantity", 75)
        self.max_ce = self.config.get("max_ce", 3)
        self.max_pe = self.config.get("max_pe", 3)
        self.min_premium = self.config.get("min_premium", 200)
        self.max_premium = self.config.get("max_premium", 1000)

        # Trailing SL rules (SELL positions)
        self.trail_rules = [
            (0.70, 0.60),
            (0.60, 0.50),
            (0.50, 1),
        ]

        self.last_processed_ts = -1
        self.trailing_sl_cache = {}

        # ---- Daily State ----
        self.current_date = None

        self.daily_open = None          # 9:15 open
        self.daily_close = None         # 15:25 close
        self.today_candle = None        # red / green

        self.prev_day_candle = None     # yesterday candle
        self.entry_1525_done = False
        self.reverse_916_done = False

    # ============================================================
    # DATA TRANSFORM
    # ============================================================
    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        return df.with_columns(
            pl.from_epoch(pl.col("ti"), time_unit="s")
            .dt.strftime("%d%b%y")
            .str.to_uppercase()
            .alias("ti_str")
        )

    # ============================================================
    # MAIN HANDLER
    # ============================================================
    def on_packet(self, packet: dict, execution_data: dict) -> List[Signal]:

        signals: List[Signal] = []
        ts = packet.get("timestamp")
        if ts is None:
            return signals

        tf1 = packet.get("1m", {}).get("current_data", {})
        spot = tf1.get("SENSEX")
        if not spot:
            return signals

        ist = datetime.fromtimestamp(ts, timezone(timedelta(hours=5, minutes=30)))
        current_time = ist.time()
        current_date_str = ist.strftime("%Y-%m-%d")

        # ========================================================
        # NEW DAY RESET (DO NOT RESET prev_day_candle)
        # ========================================================
        if self.current_date != current_date_str:
            self.current_date = current_date_str

            self.daily_open = None
            self.daily_close = None
            self.today_candle = None

            self.entry_1525_done = False
            self.reverse_916_done = False

            self.log(
                f"NEW DAY {current_date_str} | PrevDayCandle:{self.prev_day_candle}",
                level="INFO"
            )

        # ========================================================
        # CAPTURE DAILY OPEN (9:15)
        # ========================================================
        if time(9, 15) <= current_time <= time(9, 16):
            if self.daily_open is None:
                self.daily_open = spot.get("o") or spot.get("c")

        # ========================================================
        # CAPTURE DAILY CLOSE (15:25)
        # ========================================================
        if time(15, 25) <= current_time <= time(15, 26):
            if self.daily_close is None:
                self.daily_close = spot.get("c")

        # ========================================================
        # OPEN POSITIONS
        # ========================================================
        pos_df = self.get_positions_df()
        open_trades = pos_df.to_dicts() if pos_df is not None else []

        ce_count = sum(1 for p in open_trades if p.get("metadata", {}).get("type") == "CE")
        pe_count = sum(1 for p in open_trades if p.get("metadata", {}).get("type") == "PE")

        # ========================================================
        # SAME DAY ENTRY @ 15:25
        # ========================================================
        if (
            not self.entry_1525_done
            and self.daily_open is not None
            and self.daily_close is not None
            and time(15, 26) <= current_time <= time(15, 29)
        ):
            self.today_candle = ("red" if self.daily_close < self.daily_open else "green")

            self.log(
                f"15:25 CANDLE | O:{self.daily_open} C:{self.daily_close} "
                f"CANDLE:{self.today_candle}",
                level="INFO"
            )

            sig = None

            if self.today_candle == "red" and ce_count < self.max_ce:
                self.log("15:25 ENTRY → SELL CE", level="INFO")
                sig = self._sell_option("CE", spot, execution_data, ts)

            elif self.today_candle == "green" and pe_count < self.max_pe:
                self.log("15:25 ENTRY → SELL PE", level="INFO")
                sig = self._sell_option("PE", spot, execution_data, ts)

            if sig:
                signals.append(sig)
                self.entry_1525_done = True

            # Store for NEXT DAY reversal
            self.prev_day_candle = self.today_candle

        # ========================================================
        # NEXT DAY REVERSAL ENTRY @ 9:16 (ADD ONLY)
        # ========================================================
        if (
            not self.reverse_916_done
            and self.prev_day_candle is not None
            and time(9, 16) <= current_time <= time(9, 18)
        ):
            sig = None

            if self.prev_day_candle == "red" and pe_count < self.max_pe:
                self.log("9:16 REVERSAL → SELL PE", level="INFO")
                sig = self._sell_option("PE", spot, execution_data, ts)

            elif self.prev_day_candle == "green" and ce_count < self.max_ce:
                self.log("9:16 REVERSAL → SELL CE", level="INFO")
                sig = self._sell_option("CE", spot, execution_data, ts)

            if sig:
                signals.append(sig)
                self.reverse_916_done = True

        # ========================================================
        # RISK MANAGEMENT (BYTE-FOR-BYTE ORIGINAL)
        # ========================================================
        if ts != self.last_processed_ts:
            self.last_processed_ts = ts
            signals.extend(
                self._manage_risk(open_trades, execution_data, spot, ts)
            )

        return signals

    # ============================================================
    # PREMIUM PRICE FILTERING
    # ============================================================
    def _find_strike_with_valid_premium(
        self,
        opt_type: str,
        spot_price: float,
        execution_data: dict,
        expiry: str,
        initial_strike: int
    ) -> Optional[tuple[str, float]]:
        """
        Find a strike with premium in range [min_premium, max_premium].
        If initial strike premium is out of range, search OTM/ITM side accordingly.
        
        Returns: (symbol, price) or None if not found
        """
        
        # Try initial strike first
        symbol = f"SENSEX{expiry}{initial_strike}{opt_type}"
        opt_data = execution_data.get(symbol)
        
        if opt_data and "c" in opt_data:
            price = opt_data["c"]
            if self.min_premium <= price <= self.max_premium:
                return (symbol, price)
        
        # Premium too high (> 1000) → search OTM (higher strikes)
        if opt_data and "c" in opt_data and opt_data["c"] > self.max_premium:
            self.log(
                f"Premium {opt_data['c']} exceeds max {self.max_premium} for {symbol}, searching OTM...",
                level="INFO"
            )
            for offset in range(100, 1001, 100):
                otm_strike = initial_strike + offset
                otm_symbol = f"SENSEX{expiry}{otm_strike}{opt_type}"
                otm_data = execution_data.get(otm_symbol)
                
                if otm_data and "c" in otm_data:
                    otm_price = otm_data["c"]
                    if self.min_premium <= otm_price <= self.max_premium:
                        return (otm_symbol, otm_price)
        
        # Premium too low (< 200) → search ITM (lower strikes)
        elif opt_data and "c" in opt_data and opt_data["c"] < self.min_premium:
            self.log(
                f"Premium {opt_data['c']} below min {self.min_premium} for {symbol}, searching ITM...",
                level="INFO"
            )
            for offset in range(100, 1001, 100):
                itm_strike = initial_strike - offset
                if itm_strike < 0:
                    break
                    
                itm_symbol = f"SENSEX{expiry}{itm_strike}{opt_type}"
                itm_data = execution_data.get(itm_symbol)
                
                if itm_data and "c" in itm_data:
                    itm_price = itm_data["c"]
                    if self.min_premium <= itm_price <= self.max_premium:
                        return (itm_symbol, itm_price)
        
        return None

    # ============================================================
    # OPTION ENTRY
    # ============================================================
    def _sell_option(
        self,
        opt_type: str,
        spot: dict,
        execution_data: dict,
        ts: int
    ) -> Optional[Signal]:

        spot_price = spot.get("c")
        if spot_price is None:
            self.log("SPOT PRICE MISSING", level="WARNING")
            return None

        current_expiry = spot.get("CurrentExpiry")
        next_expiry = spot.get("NextExpiry")
        today = spot.get("ti_str")

        if not current_expiry:
            self.log("CURRENT EXPIRY MISSING IN SPOT DATA", level="WARNING")
            return None

        if today == current_expiry:
            expiry = next_expiry
        else:
            expiry = current_expiry

        if not expiry:
            self.log("EXPIRY RESOLUTION FAILED", level="WARNING")
            return None

        initial_strike = round(spot_price / 100) * 100
        
        # Find strike with valid premium in range [200, 1000]
        strike_result = self._find_strike_with_valid_premium(
            opt_type, spot_price, execution_data, expiry, initial_strike
        )
        
        if not strike_result:
            self.log(
                f"NO VALID STRIKE FOUND for {opt_type} with premium in [{self.min_premium}, {self.max_premium}]",
                level="WARNING"
            )
            return None
        
        symbol, price = strike_result

        self.log(
            f"SELL ENTRY CONFIRMED {symbol} | Spot:{spot_price} Strike:{initial_strike} Price:{price}",
            level="INFO"
        )

        return self.create_signal(
            symbol=symbol,
            action="SELL",
            quantity=self.quantity,
            timestamp=ts,
            price=price,
            reason="DAILY_REVERSAL_SELL",
            custom_metadata={
                "type": opt_type,
                "expiry": expiry,
                "entry_price": price,
            },
        )

    # ============================================================
    # RISK MANAGEMENT (UNCHANGED FROM ORIGINAL)
    # ============================================================
    def _manage_risk(
        self,
        trades: List[dict],
        prices: dict,
        spot: dict,
        ts: int
    ) -> List[Signal]:

        signals: List[Signal] = []

        ist = datetime.fromtimestamp(ts, timezone(timedelta(hours=5, minutes=30)))
        today = spot.get("ti_str", "")

        for trade in trades:
            symbol = trade["symbol"]
            trade_id = trade["trade_id"]
            entry = trade.get("entry_price")
            meta = trade.get("metadata", {})

            ltp = prices.get(symbol, {}).get("c")

            if entry is None or ltp is None:
                continue

            # ---------------- 80% TARGET ----------------
            target_price = entry * 0.20
            if ltp <= target_price:
                signals.append(self.create_signal(
                    symbol=symbol,
                    action="EXIT",
                    quantity=0,
                    timestamp=ts,
                    reason="TARGET_80",
                    price=ltp,
                    trade_id_to_close=trade_id,
                ))
                self.trailing_sl_cache.pop(trade_id, None)
                self.log(
                    f"TARGET HIT for {symbol} | LTP:{ltp} Target:{target_price}",
                    level="INFO"
                )
                continue

            # ---------------- 50% STOP LOSS ----------------
            sl_price = entry * 1.50
            if ltp >= sl_price:
                signals.append(self.create_signal(
                    symbol=symbol,
                    action="EXIT",
                    quantity=0,
                    timestamp=ts,
                    reason="STOP_LOSS_50",
                    price=ltp,
                    trade_id_to_close=trade_id,
                ))
                self.trailing_sl_cache.pop(trade_id, None)
                self.log(
                    f"STOP LOSS HIT for {symbol} | LTP:{ltp} SL:{sl_price}",
                    level="INFO"
                )
                continue

            # ---------------- EXPIRY EXIT ----------------
            if ist.hour == 15 and ist.minute == 20:
                if meta.get("expiry") == today:
                    signals.append(self.create_signal(
                        symbol=symbol,
                        action="EXIT",
                        quantity=0,
                        timestamp=ts,
                        reason="EXPIRY_EXIT",
                        price=ltp,
                        trade_id_to_close=trade_id,
                    ))
                    self.trailing_sl_cache.pop(trade_id, None)
                    self.log(
                        f"EXPIRY EXIT for {symbol} | LTP:{ltp} Expiry:{today}",
                        level="INFO"
                    )
                    continue

            # ---------------- TRAILING SL ----------------
            current_sl = self.trailing_sl_cache.get(trade_id)
            updated_sl = current_sl

            for trigger, sl_mul in self.trail_rules:
                if ltp <= entry * trigger:
                    proposed = entry * sl_mul
                    if updated_sl is None or proposed < updated_sl:
                        updated_sl = proposed

                        self.log(f"trail sl updated for {symbol} | LTP:{ltp} Entry:{entry} Trigger:{trigger} ", level="INFO")

            if updated_sl != current_sl:
                self.trailing_sl_cache[trade_id] = updated_sl

            if updated_sl is not None and ltp >= updated_sl:
                signals.append(self.create_signal(
                    symbol=symbol,
                    action="EXIT",
                    quantity=0,
                    timestamp=ts,
                    reason="TRAIL_SL",
                    price=ltp,
                    trade_id_to_close=trade_id,
                ))
                self.trailing_sl_cache.pop(trade_id, None)
                self.log(f"TRAIL SL HIT for {symbol} | LTP:{ltp} SL:{updated_sl}", level="INFO")

        return signals