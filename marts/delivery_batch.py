"""Nightly pair inference, backed by local marts or a future SQL feature table."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from .delivery_features import KEYS, make_delivery_features


class DeliveryModel:
    def __init__(self, model_dir):
        root = Path(model_dir)
        self.metadata = json.loads((root / 'metadata.json').read_text())
        self.p50 = CatBoostRegressor().load_model(str(root / 'p50.cbm'))
        self.p90 = CatBoostRegressor().load_model(str(root / 'p90.cbm'))

    def predict(self, pair_features):
        """One row per seller-city-date. Can consume a SQL-backed DataFrame."""
        if pair_features.duplicated(KEYS).any():
            raise ValueError('Expected unique seller_id × city_id × forecast_date')
        dates = pd.to_datetime(pair_features.forecast_date)
        if dates.lt(pd.Timestamp(self.metadata['fit_as_of'])).any():
            raise ValueError('Model was trained after the requested forecast date')
        x, _ = make_delivery_features(
            pair_features, self.metadata['base_features'],
            engineered=self.metadata['engineered'], use_ids=self.metadata['use_ids'])
        if list(x) != self.metadata['features']:
            raise ValueError('Feature schema differs from the saved model')
        out = pair_features[KEYS].copy()
        offset = pair_features.pair_delivery_days_smoothed.to_numpy() if self.metadata.get('residual') else 0
        out['eta_p50_days'] = np.maximum(0, self.p50.predict(x, thread_count=4) + offset)
        raw90 = np.maximum(0, self.p90.predict(x, thread_count=4) + offset)
        out['eta_p90_days'] = np.maximum(out.eta_p50_days, raw90)
        out['has_pair_history'] = pair_features.pair_n_delivered.gt(0).values
        out['model_version'] = self.metadata['model_version']
        out['computed_at_utc'] = pd.Timestamp.now(tz='UTC')
        return out


class LocalPairFeatures:
    """Build one date's sparse histories once, then stream the full pair grid.

    This is a reference adapter for the Parquet phase. A SQL mart can later
    provide the same features directly to DeliveryModel.predict().
    """
    def __init__(self, store):
        # Reuse the exact feature builder used to produce the training marts.
        src = str(Path(__file__).parent / 'src')
        if src not in sys.path:
            sys.path.insert(0, src)
        from history import Histories
        from training import pair_features
        self._assemble = pair_features
        self.store = store
        self.t = {n: store.read('stg_' + n) for n in ['sellers', 'order_reviews']}
        self.d = store.read('fct_order_delivery')
        self.f = store.read('fct_order_features')
        self.rv = store.read('fct_order_review')
        self.so = store.read('fct_seller_order')
        products = store.read('stg_products')[['product_id', 'product_category_name']]
        trans = store.read('stg_category_translation')
        x = (store.read('stg_order_items')[['order_id', 'product_id']]
             .merge(products, on='product_id', validate='many_to_one')
             .merge(trans, on='product_category_name', validate='many_to_one')
             .rename(columns={'product_category_name_english': 'category'})
             .merge(self.d[['order_id', 'purchase_ts']], on='order_id', validate='many_to_one'))
        cfg = SimpleNamespace(smoothing_k=float(store.manifest['parameters']['smoothing_k']))
        self.engine = Histories(self.t, self.d, self.f, self.rv, x, self.so, cfg)
        self.geo = store.read('dim_geo_zip')
        self.cities = store.read('dim_city')
        self.calendar = store.read('dim_calendar')
        self._snapshot = None

    def active_universe(self, date):
        date = pd.Timestamp(date).normalize()
        sellers = sorted(self.so.loc[self.so.purchase_ts.ge(date - pd.Timedelta(days=90))
                         & self.so.purchase_ts.lt(date), 'seller_id'].unique())
        ctx = self.engine.ctx
        cities = sorted(ctx.loc[ctx.purchase_ts.ge(date - pd.Timedelta(days=365))
                        & ctx.purchase_ts.lt(date), 'city_id'].unique())
        return sellers, cities

    def prepare(self, date, seller_ids=None, city_ids=None):
        date = pd.Timestamp(date).normalize()
        if not self.calendar.date.eq(date).any():
            raise ValueError('Date outside the available calendar; refresh source marts first')
        active_sellers, active_cities = self.active_universe(date)
        seller_ids = list(active_sellers if seller_ids is None else seller_ids)
        city_ids = list(active_cities if city_ids is None else city_ids)
        if set(seller_ids) - set(self.t['sellers'].seller_id):
            raise ValueError('Unknown sellers; refresh seller geography first')
        if set(city_ids) - set(self.cities.city_id):
            raise ValueError('Unknown cities; refresh city geography first')
        states = sorted(self.cities.set_index('city_id').loc[city_ids, 'state'].unique())
        seller_states = sorted(self.t['sellers'].set_index('seller_id').loc[seller_ids, 'seller_state'].unique())
        needs = {
            'seller': pd.DataFrame({'seller_id': seller_ids}),
            'city': pd.DataFrame({'city_id': city_ids}),
            'route': pd.DataFrame({'route_state': [s + '-' + c for s in seller_states for c in states]}),
            'state': pd.MultiIndex.from_product([seller_ids, states], names=['seller_id', 'customer_state']).to_frame(index=False),
            'pair': self.so.loc[self.so.purchase_ts.lt(date)
                              & self.so.seller_id.isin(seller_ids)
                              & self.so.city_id.isin(city_ids), ['seller_id', 'city_id']].drop_duplicates(),
        }
        for value in needs.values():
            value['as_of_date'] = date
        histories = self.engine.at(date, needs)
        self._snapshot = (date, histories, set(seller_ids), set(city_ids))
        return len(seller_ids) * len(city_ids)

    def features(self, pairs):
        if self._snapshot is None:
            raise RuntimeError('Call prepare(date) first')
        date, histories, sellers, cities = self._snapshot
        if pairs.duplicated(['seller_id', 'city_id']).any():
            raise ValueError('Duplicate pairs')
        if set(pairs.seller_id) - sellers or set(pairs.city_id) - cities:
            raise ValueError('Pair outside the prepared universe')
        triples = pairs[['seller_id', 'city_id']].copy()
        triples['forecast_date'] = date
        m = self._assemble(triples, self.t, self.geo, self.cities, self.calendar, histories)
        # The sparse history contains past pairs only. New pairs inherit their
        # seller-state prior, exactly as the training builder does for zero counts.
        m['pair_n_delivered'] = m.pair_n_delivered.fillna(0)
        m['pair_delivery_days_smoothed'] = m.pair_delivery_days_smoothed.fillna(
            m.seller_state_delivery_days_smoothed)
        return m

    def iter_grid(self, batch_size=20000):
        if self._snapshot is None:
            raise RuntimeError('Call prepare(date) first')
        _, _, sellers, cities = self._snapshot
        sellers, cities = sorted(sellers), sorted(cities)
        pending = []
        for seller in sellers:
            for city in cities:
                pending.append((seller, city))
                if len(pending) >= batch_size:
                    yield self.features(pd.DataFrame(pending, columns=['seller_id', 'city_id']))
                    pending = []
        if pending:
            yield self.features(pd.DataFrame(pending, columns=['seller_id', 'city_id']))
