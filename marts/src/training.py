from common import *
from history import KEYS

CAL=['day_of_week','is_weekend','month','is_holiday','business_days_next_14d','days_to_next_holiday','is_black_friday_week','is_xmas_season','is_truckers_strike','platform_orders_1d','platform_orders_7d','platform_orders_7d_vs_28d','platform_in_transit']

def join_history(frame,name,h,date='as_of_date'):
    keys=KEYS[name]; right=h[name]
    return frame.merge(right,on=keys+['as_of_date'],how='left',validate='many_to_one')

def pair_features(triples,t,z,cities,cal,h):
    m=triples.drop_duplicates().copy(); sellers=t['sellers'].merge(z[['zip_code_prefix','lat','lng']],left_on='seller_zip_code_prefix',right_on='zip_code_prefix',how='left')
    sellers=sellers[['seller_id','seller_city','seller_state','lat','lng']].rename(columns={'lat':'seller_lat','lng':'seller_lng'})
    m=m.merge(sellers,on='seller_id',validate='many_to_one').merge(cities.rename(columns={'state':'customer_state','city':'customer_city','region':'customer_region','lat':'customer_lat','lng':'customer_lng','is_capital':'is_customer_capital'}),on='city_id',validate='many_to_one')
    m['seller_region']=m.seller_state.map(REGIONS); m['route_state']=m.seller_state+'-'+m.customer_state; m['distance_km']=distance(m.seller_lat,m.seller_lng,m.customer_lat,m.customer_lng); m['is_same_state']=m.seller_state.eq(m.customer_state); m['is_same_city']=m.is_same_state&m.seller_city.eq(m.customer_city)
    m=m.drop(columns=['seller_lat','seller_lng','customer_lat','customer_lng','seller_city','customer_city']).rename(columns={'forecast_date':'as_of_date'})
    for name in ['pair','state','seller','city','route']:
        hist=h[name]
        if name=='seller': hist=hist[[c for c in hist if not any(s in c for s in ['review','score'])]]
        m=m.merge(hist,on=KEYS[name]+['as_of_date'],how='left',validate='many_to_one')
    m=m.merge(cal[['date']+CAL].rename(columns={'date':'as_of_date'}),on='as_of_date',validate='many_to_one')
    return m.rename(columns={'as_of_date':'forecast_date'})

def delivery(d,f,t,z,cities,cal,h,w):
    sel=d[d.is_valid_for_training].merge(f[['order_id','main_seller_id','city_id','n_sellers','main_payment_type']],on='order_id'); sel=sel[sel.n_sellers.eq(1)]
    triples=sel[['main_seller_id','city_id','purchase_date']].rename(columns={'main_seller_id':'seller_id','purchase_date':'forecast_date'})
    m=w.write('mart_seller_city_features',pair_features(triples,t,z,cities,cal,h),['seller_id','city_id','forecast_date'])
    cols=['order_id','purchase_ts','delivered_ts','delivery_days_from_approval','delivery_days_from_purchase','approval_hours','handling_days','transit_days','olist_estimate_days','main_payment_type']
    ds=sel[cols+['main_seller_id','city_id','purchase_date']].rename(columns={'main_seller_id':'seller_id','purchase_date':'forecast_date','delivered_ts':'label_available_ts'}).merge(m,on=['seller_id','city_id','forecast_date'],validate='many_to_one')
    ds=w.write('ml_delivery_dataset',ds,['order_id'])
    features=[c for c in m if c not in ['seller_id','city_id','forecast_date','route_state']]
    return ds,features

def reviews(d,f,rv,sv,x,cal,h,cfg,w):
    sel=d[d.purchase_ts.ge(cfg.train_from)&d.purchase_ts.lt(cfg.train_to)][['order_id','purchase_ts','purchase_date','customer_unique_id','olist_estimate_days']].merge(f,on='order_id',validate='one_to_one').merge(rv[['order_id','survey_date','survey_ts','is_bad_review','review_score_min','answer_ts']],on='order_id',validate='one_to_one')
    sel=sel[sel.answer_ts.ge(sel.purchase_ts)].drop(columns='answer_ts')
    # A minimum across multiple answers is fully known only after the last answer.
    result={}; contracts={}
    for stage,date in [('t0','purchase_date'),('ts','survey_date')]:
        a=sel.copy(); a['as_of_date']=a[date]
        # Maximum risk, not the most expensive seller/product.
        for name,entity,risk in [('seller','seller_id','seller_bad_review_share_smoothed'),('product','product_id','product_bad_review_share_smoothed')]:
            bridge=x[['order_id',entity]].drop_duplicates().merge(a[['order_id','as_of_date']],on='order_id').merge(h[name],on=[entity,'as_of_date'],how='left',validate='many_to_one')
            worst=bridge.sort_values(['order_id',risk,entity],ascending=[True,False,True]).drop_duplicates('order_id').drop(columns=entity)
            a=a.merge(worst,on=['order_id','as_of_date'],validate='one_to_one')
        a['category']=a.main_category
        for name in ['route','city','category','customer']: a=join_history(a,name,h)
        a=a.drop(columns='category')
        a=a.merge(cal[['date']+CAL].rename(columns={'date':'purchase_date'}),on='purchase_date',validate='many_to_one')
        if stage=='ts':
            a=a.merge(sv.drop(columns='survey_ts'),on='order_id',validate='one_to_one')
            a=a.merge(cal[['date']+CAL].rename(columns={'date':'survey_date',**{c:'survey_'+c for c in CAL}}),on='survey_date',validate='many_to_one')
            a=a[a.survey_ts.ge(a.purchase_ts)]
        metadata=['order_id','purchase_ts','purchase_date','customer_unique_id','survey_date','survey_ts','as_of_date','is_bad_review','review_score_min','city_id','main_seller_id','customer_zip_code_prefix','seller_zip_code_prefix','route_state']
        contracts[stage]=[c for c in a if c not in metadata]
        # survey timestamp is excluded from T0 output, including metadata.
        if stage=='t0': a=a.drop(columns=['survey_date','survey_ts'])
        result[stage]=w.write('ml_review_dataset_'+stage,a,['order_id'])
    return result,contracts
