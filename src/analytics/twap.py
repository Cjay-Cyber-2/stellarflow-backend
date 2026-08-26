import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class TradePoint:
    timestamp: datetime
    price: float
    volume: float


class TWAPEngine:
    """Calculates Time-Weighted Average Price (TWAP) with outlier filtering."""

    @staticmethod
    def filter_outliers(trades: List[TradePoint], variance_threshold: float = 0.50) -> List[TradePoint]:
        """Filters out trades exceeding `variance_threshold` (default 50%) from moving median."""
        if not trades:
            return []

        prices = [t.price for t in trades]
        median_price = float(np.median(prices))

        if median_price == 0:
            return trades

        filtered_trades = []
        for trade in trades:
            variance = abs(trade.price - median_price) / median_price
            if variance <= variance_threshold:
                filtered_trades.append(trade)

        return filtered_trades

    @classmethod
    def calculate_twap(
        cls,
        trades: List[TradePoint],
        window: timedelta,
        current_time: Optional[datetime] = None,
    ) -> float:
        """Calculates time-weighted average price over a given time window."""
        if not trades:
            return 0.0

        if current_time is None:
            current_time = datetime.now(timezone.utc)

        start_time = current_time - window
        
        # Sort trades by timestamp ascending
        sorted_trades = sorted(trades, key=lambda t: t.timestamp)
        
        # Filter trades within the window
        window_trades = [t for t in sorted_trades if t.timestamp >= start_time]

        # Filter price outliers
        clean_trades = cls.filter_outliers(window_trades)

        if not clean_trades:
            return 0.0

        # Compute time-weighted average price using linear interval integration
        total_time_weighted_price = 0.0
        total_time_delta = 0.0

        for i in range(len(clean_trades)):
            current = clean_trades[i]
            # Determine interval duration to the next trade or current_time
            if i < len(clean_trades) - 1:
                next_time = clean_trades[i + 1].timestamp
            else:
                next_time = current_time

            duration = (next_time - current.timestamp).total_seconds()
            if duration > 0:
                total_time_weighted_price += current.price * duration
                total_time_delta += duration

        if total_time_delta == 0:
            return clean_trades[-1].price

        return round(total_time_weighted_price / total_time_delta, 6)