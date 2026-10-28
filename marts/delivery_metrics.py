"""Business metrics for delivery promises, measured in days."""
import numpy as np


def late_delivery_days(actual, predicted):
    """Per-order penalty: zero when on time, otherwise days beyond the promise."""
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    if actual.ndim != 1 or actual.shape != predicted.shape or actual.size == 0:
        raise ValueError('Expected nonempty one-dimensional arrays of equal length')
    if not (np.isfinite(actual).all() and np.isfinite(predicted).all()):
        raise ValueError('Actual and predicted days must be finite')
    return np.maximum(actual - predicted, 0.0)


def mean_late_delivery_days(actual, predicted):
    """Mean penalty across ALL orders, including zero penalties; lower is better."""
    return float(late_delivery_days(actual, predicted).mean())
