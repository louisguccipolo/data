"""Deterministic seller × city × forecast-date features; no order-specific inputs."""
import numpy as np
import pandas as pd

KEYS = ['seller_id', 'city_id', 'forecast_date']


def make_delivery_features(frame, base_features, *, engineered=True, use_ids=False):
    """Use the same transformation in training and nightly batch inference.

    ``base_features`` comes from the versioned mart contract. No target encoding,
    fitted imputation or global statistics are computed here.
    """
    missing = set(base_features + KEYS) - set(frame.columns)
    if missing:
        raise ValueError(f'Missing pair features: {sorted(missing)}')
    x = frame[base_features].copy()
    if engineered:
        d = pd.to_datetime(frame.forecast_date)
        x['route_state'] = frame.seller_state + '-' + frame.customer_state
        x['dow_sin'] = np.sin(2 * np.pi * d.dt.dayofweek / 7)
        x['dow_cos'] = np.cos(2 * np.pi * d.dt.dayofweek / 7)
        x['month_sin'] = np.sin(2 * np.pi * (d.dt.month - 1) / 12)
        x['month_cos'] = np.cos(2 * np.pi * (d.dt.month - 1) / 12)
        x['days_since_2017'] = (d - pd.Timestamp('2017-01-01')).dt.days
        for c in ['distance_km', 'n_zip_prefixes', 'pair_n_delivered',
                  'seller_n_delivered', 'seller_n_orders_total', 'seller_age_days',
                  'seller_orders_last_7d', 'seller_orders_last_30d', 'seller_backlog',
                  'city_n_delivered', 'city_orders_last_30d', 'route_n_delivered']:
            x['log1p_' + c] = np.log1p(frame[c].clip(lower=0))
        for prefix in ['pair', 'seller', 'city', 'route']:
            x[prefix + '_cold_start'] = frame[prefix + '_n_delivered'].eq(0)
        x['pair_history_reliability'] = frame.pair_n_delivered / (frame.pair_n_delivered + 10)
        x['seller_load_growth'] = (frame.seller_orders_last_7d + 1) / (
            frame.seller_orders_last_30d * 7 / 30 + 1)
        x['seller_backlog_to_weekly_orders'] = frame.seller_backlog / (
            frame.seller_orders_last_7d + 1)
        x['handling_plus_route_transit'] = (
            frame.seller_handling_days_smoothed + frame.route_transit_days_smoothed)
        x['handling_plus_city_transit'] = (
            frame.seller_handling_days_smoothed + frame.city_transit_days_smoothed)
        x['pair_vs_seller_state_days'] = (
            frame.pair_delivery_days_smoothed - frame.seller_state_delivery_days_smoothed)
        x['seller_state_vs_seller_days'] = (
            frame.seller_state_delivery_days_smoothed - frame.seller_delivery_days_smoothed)
        x['route_recent_vs_lifetime_days'] = (
            frame.route_transit_days_last_30d - frame.route_transit_days_smoothed)
        x['pair_recency_missing'] = frame.pair_days_since_last_delivery.isna()
    if use_ids:
        x['seller_id'] = frame.seller_id
        x['city_id'] = frame.city_id
    categorical = [c for c in x if pd.api.types.is_object_dtype(x[c])
                   or isinstance(x[c].dtype, (pd.StringDtype, pd.CategoricalDtype))]
    for c in categorical:
        x[c] = x[c].fillna('__missing__').astype(str)
    for c in x.columns.difference(categorical):
        x[c] = pd.to_numeric(x[c], errors='coerce').astype(float).replace(
            [np.inf, -np.inf], np.nan)
    return x, categorical
