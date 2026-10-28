"""Cross-mart invariants and a future-perturbation leakage check."""
from common import *

def validate(t,d,f,rv,sv,so,dd,rr,churn,seg,dem,engine,req,contracts,w):
    checks=[]
    def check(condition,name):
        if not condition: raise AssertionError(name)
        checks.append(name)
    check(len(d)==len(t['orders']),'All source orders retained')
    check(len(f)==t['order_items'].order_id.nunique(),'Exactly one feature row per order with items')
    check(np.isclose(f.total_price.sum(),t['order_items'].price.sum()),'Order revenue reconciles to raw items')
    check(np.isclose(so.revenue.sum(),f.total_price.sum()),'Seller revenue reconciles to orders')
    v=d[d.is_valid_for_training]; stage=v.approval_hours/24+v.handling_days+v.transit_days
    check(np.allclose(stage,v.delivery_days_from_purchase),'Delivery stages reconcile')
    check(dd.delivery_days_from_approval.gt(0).all(),'Delivery targets are positive')
    check(dd.order_id.isin(f[f.n_sellers.eq(1)].order_id).all(),'Delivery training contains single-seller orders only')
    check(sv.loc[sv.is_delivered_at_survey,'days_since_delivery_at_survey'].gt(0).all(),'Survey events strictly precede survey timestamp')
    check(churn.snapshot_date.add(60*DAY).le(engine.cfg.sales_end).all(),'Churn labels have complete future windows')
    check(seg.n_orders_90d.gt(0).all()&~seg.is_low_data.any(),'Segmentation contains eligible active sellers')
    check(dem[['y_h1','y_h2','y_h3','y_h4']].notna().all().all(),'Demand training has four complete target weeks')
    weekly=pd.read_parquet(w.path/'agg_demand_weekly.parquet')
    pair=weekly[weekly.level.eq('pair')].groupby(['category_group','week_start']).n_orders.sum().sort_index(); category=weekly[weekly.level.eq('category')].set_index(['category_group','week_start']).n_orders.sort_index()
    check(pair.equals(category),'Demand pair counts reconcile to categories')
    region=weekly[weekly.level.eq('region')].groupby('week_start').n_orders.sum().sort_index(); total=weekly[weekly.level.eq('total')].set_index('week_start').n_orders.sort_index()
    check(region.equals(total),'Demand region counts reconcile to platform')
    targets={'is_bad_review','review_score_min','label_available_ts','is_churned','revenue_next_60d','n_orders_next_60d','is_returned_later','y_h1','y_h2','y_h3','y_h4'}
    for task,contract in contracts.items():
        check(not set(contract['features'])&targets,f'{task}: labels and future values excluded from X')
    # Perturb only information that arrives after a historical forecast.
    date=pd.Timestamp('2018-01-15'); need={n:q[q.as_of_date.eq(date)].head(8) for n,q in req.items()}; need={n:q for n,q in need.items() if len(q)}
    before=engine.at(date,need); raw=engine.t['order_reviews']; saved=raw.review_score.copy(); future=raw.review_answer_timestamp.ge(date); raw.loc[future,'review_score']=1
    old_d=engine.d; old_so=engine.so; old_ctx=engine.ctx; engine.d=old_d.copy(); engine.so=old_so.copy(); engine.ctx=old_ctx.copy()
    for frame in [engine.d,engine.so,engine.ctx]:
        future=frame.delivered_ts.ge(date)
        for col in ['delivery_days_from_approval','transit_days']: frame.loc[future,col]=9999
    try: after=engine.at(date,need)
    finally: raw['review_score']=saved; engine.d=old_d; engine.so=old_so; engine.ctx=old_ctx
    for name in before: pd.testing.assert_frame_equal(before[name],after[name])
    checks.append('Future review/delivery perturbations do not change past aggregates')
    # Datasets sharing a seller-city-date must use identical pair features.
    mart=pd.read_parquet(w.path/'mart_seller_city_features.parquet'); cols=contracts['delivery']['features']; joined=dd[['seller_id','city_id','forecast_date']+cols].merge(mart,on=['seller_id','city_id','forecast_date'],suffixes=('_train','_serve'),validate='many_to_one')
    for col in cols: pd.testing.assert_series_equal(joined[col+'_train'],joined[col+'_serve'],check_names=False)
    checks.append('Training and serving pair feature values match')
    return {'checks_passed':len(checks),'checks':checks,'bad_review_rate':float(rv.is_bad_review.mean()),'survey_delivered_rate':float(sv.is_delivered_at_survey.mean()),'raw_review_answers_before_purchase':int(t['order_reviews'].merge(d[['order_id','purchase_ts']],on='order_id').eval('review_answer_timestamp < purchase_ts').sum()),'excluded_survey_before_purchase':int(rv.merge(d[['order_id','purchase_ts']],on='order_id').eval('survey_ts < purchase_ts').sum()),'payment_reconciliation_rate':float(np.isclose(f.total_price+f.total_freight,f.total_payment_value,atol=.02).mean())}
