"""Olist cleaning, geography, calendar and order-level facts."""
from common import *
import unicodedata

def sources(root):
    t={}
    for name in ['orders','order_items','order_payments','order_reviews','customers','sellers','products','category_translation','geolocation']:
        file='product_category_name_translation.csv' if name=='category_translation' else f'olist_{name}_dataset.csv'
        df=pd.read_csv(root/file,encoding='utf-8-sig',dtype={c:'string' for c in ['customer_zip_code_prefix','seller_zip_code_prefix','geolocation_zip_code_prefix']})
        for c in df:
            if c.endswith(('_date','_timestamp')) or c=='order_approved_at': df[c]=pd.to_datetime(df[c])
            if c.endswith('zip_code_prefix'): df[c]=df[c].str.zfill(5)
        t[name]=df
    snapshot=max(df[c].max() for df in t.values() for c in df.select_dtypes('datetime').columns if c not in ['shipping_limit_date','order_estimated_delivery_date'])
    i=t['order_items']; i.loc[i.shipping_limit_date>snapshot+pd.DateOffset(years=1),'shipping_limit_date']=pd.NaT
    p=t['products']; p['product_category_name']=p.product_category_name.fillna('unknown')
    for c in ['product_weight_g','product_length_cm','product_height_cm','product_width_cm']: p[c]=p[c].replace(0,np.nan)
    extra=pd.DataFrame({'product_category_name':['unknown','pc_gamer','portateis_cozinha_e_preparadores_de_alimentos'],'product_category_name_english':['unknown','pc_gamer','kitchen_portables_and_food_preparers']})
    t['category_translation']=pd.concat([t['category_translation'],extra]).drop_duplicates('product_category_name')
    g=t['geolocation'].drop_duplicates(); t['geolocation']=g[g.geolocation_lat.between(-33.8,5.3)&g.geolocation_lng.between(-73.99,-34.79)].copy()
    return t,snapshot

def geo(t,w):
    g=t['geolocation'].rename(columns={f'geolocation_{c}':c for c in ['zip_code_prefix','lat','lng','city','state']})
    z=g.groupby('zip_code_prefix').agg(lat=('lat','median'),lng=('lng','median'),city=('city',mode),state=('state',mode),n_points=('lat','size')).reset_index(); z['geo_source']='zip'
    addresses=pd.concat([t[n][[f'{s}_{c}' for c in ['zip_code_prefix','city','state']]].rename(columns={f'{s}_{c}':c for c in ['zip_code_prefix','city','state']}) for n,s in [('customers','customer'),('sellers','seller')]])
    missing=addresses[~addresses.zip_code_prefix.isin(z.zip_code_prefix)].groupby('zip_code_prefix').agg(city=('city',mode),state=('state',mode)).reset_index()
    norm=lambda s: ''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c)).lower().strip()
    g['norm']=g.city.map(norm); missing['norm']=missing.city.map(norm)
    missing=missing.merge(g.groupby(['norm','state'])[['lat','lng']].median().reset_index(),on=['norm','state'],how='left'); missing['geo_source']=np.where(missing.lat.notna(),'city','state')
    st=g.groupby('state')[['lat','lng']].median()
    for c in ['lat','lng']: missing[c]=missing[c].fillna(missing.state.map(st[c]))
    missing['n_points']=0; missing.loc[missing.lat.isna(),'geo_source']='none'
    z=pd.concat([z,missing.drop(columns='norm')],ignore_index=True); z['region']=z.state.map(REGIONS); z=w.write('dim_geo_zip',z,['zip_code_prefix'])
    c=t['customers'][['customer_city','customer_state','customer_zip_code_prefix']].drop_duplicates().merge(z[['zip_code_prefix','lat','lng']],left_on='customer_zip_code_prefix',right_on='zip_code_prefix')
    c=c.groupby(['customer_state','customer_city']).agg(lat=('lat','median'),lng=('lng','median'),n_zip_prefixes=('zip_code_prefix','nunique')).reset_index().rename(columns={'customer_state':'state','customer_city':'city'})
    c['city_id']=c.state+'|'+c.city; c['region']=c.state.map(REGIONS); c['is_capital']=c.city.eq(c.state.map(CAPITALS)); cap=c[c.is_capital].set_index('state')
    c['dist_to_state_capital_km']=distance(c.lat,c.lng,c.state.map(cap.lat),c.state.map(cap.lng))
    return z,w.write('dim_city',c,['city_id'])

def facts(t,z,w,cfg):
    o=t['orders']; cu=t['customers']; p=t['products'].merge(t['category_translation'],on='product_category_name').rename(columns={'product_category_name_english':'category'})
    d=o.merge(cu[['customer_id','customer_unique_id']],on='customer_id',validate='one_to_one').drop(columns='customer_id').rename(columns={'order_purchase_timestamp':'purchase_ts','order_approved_at':'approved_ts','order_delivered_carrier_date':'carrier_ts','order_delivered_customer_date':'delivered_ts','order_estimated_delivery_date':'estimated_delivery_date'})
    d['purchase_date']=d.purchase_ts.dt.normalize()
    for name,a,b,scale in [('delivery_days_from_purchase','delivered_ts','purchase_ts',1),('delivery_days_from_approval','delivered_ts','approved_ts',1),('approval_hours','approved_ts','purchase_ts',24),('handling_days','carrier_ts','approved_ts',1),('transit_days','delivered_ts','carrier_ts',1),('olist_estimate_days','estimated_delivery_date','purchase_ts',1)]: d[name]=days(d[a],d[b])*scale
    d['delay_days']=days(d.delivered_ts,d.estimated_delivery_date+DAY); d['is_late']=d.delay_days.gt(0).astype('boolean').mask(d.delivered_ts.isna())
    d['is_delivered']=d.order_status.eq('delivered')&d.delivered_ts.notna(); d['is_censored']=~d.is_delivered&~d.order_status.isin(['canceled','unavailable']); d['has_missing_approval']=d.is_delivered&d.approved_ts.isna()
    d['has_items']=d.order_id.isin(t['order_items'].order_id)
    d['has_stage_anomaly']=d[['approval_hours','handling_days','transit_days','delivery_days_from_approval','delivery_days_from_purchase']].lt(0).any(axis=1)
    d['is_valid_for_training']=d.is_delivered&d.has_items&~d.has_stage_anomaly&d.delivery_days_from_purchase.gt(0)&d.delivery_days_from_approval.gt(0)&d.carrier_ts.notna()&d.purchase_ts.ge(cfg.train_from)&d.purchase_ts.lt(cfg.train_to)
    d=w.write('fct_order_delivery',d,['order_id'])
    r=t['order_reviews'].sort_values(['review_answer_timestamp','review_id']); r['shared']=r.review_id.map(r.groupby('review_id').order_id.nunique()).gt(1); r['comment']=r.review_comment_title.notna()|r.review_comment_message.notna()
    rv=r.groupby('order_id').agg(n_reviews=('review_id','size'),review_score_min=('review_score','min'),review_score_last=('review_score','last'),scores=('review_score','nunique'),survey_ts=('review_creation_date','min'),answer_ts=('review_answer_timestamp','min'),has_comment=('comment','max'),is_shared_review=('shared','max')).reset_index()
    rv['has_score_conflict']=rv.pop('scores').gt(1); rv['is_bad_review']=rv.review_score_min.le(2); rv['is_bad_review_last']=rv.review_score_last.le(2); rv['survey_date']=rv.survey_ts.dt.normalize(); rv['answer_lag_days']=days(rv.answer_ts,rv.survey_ts)
    rv=w.write('fct_order_review',rv,['order_id']); a=d.merge(rv[['order_id','survey_ts']],on='order_id'); sv=a[['order_id','survey_ts']].copy()
    for stage,col in [('approved','approved_ts'),('shipped','carrier_ts'),('delivered','delivered_ts')]: sv[f'is_{stage}_at_survey']=a[col].lt(a.survey_ts)
    sv['days_since_purchase_at_survey']=days(a.survey_ts,a.purchase_ts)
    for name,value,mask in [('delivery_days_at_survey',a.delivery_days_from_purchase,sv.is_delivered_at_survey),('handling_days_at_survey',a.handling_days,sv.is_shipped_at_survey&sv.is_approved_at_survey),('days_since_delivery_at_survey',days(a.survey_ts,a.delivered_ts),sv.is_delivered_at_survey)]: sv[name]=value.where(mask)
    sv['days_vs_estimate_at_survey']=days(a.delivered_ts.where(sv.is_delivered_at_survey,a.survey_ts),a.estimated_delivery_date); sv['is_late_at_survey']=sv.days_vs_estimate_at_survey.gt(1)
    sv['order_status_at_survey']=np.select([sv.is_delivered_at_survey,sv.is_shipped_at_survey,sv.is_approved_at_survey],['delivered','shipped','approved'],default='created'); sv=w.write('fct_order_at_survey',sv,['order_id'])
    x=t['order_items'].merge(p,on='product_id',validate='many_to_one').merge(o[['order_id','customer_id']],on='order_id').merge(cu,on='customer_id').merge(t['sellers'],on='seller_id')
    for side in ['customer','seller']:
        x=x.merge(z[['zip_code_prefix','lat','lng','geo_source']].rename(columns={c:f'{side}_{c}' for c in ['lat','lng','geo_source']}),left_on=f'{side}_zip_code_prefix',right_on='zip_code_prefix',how='left').drop(columns='zip_code_prefix')
        x[f'{side}_region']=x[f'{side}_state'].map(REGIONS)
    x['city_id']=x.customer_state+'|'+x.customer_city; x['route_state']=x.seller_state+'-'+x.customer_state
    x['distance_km']=distance(x.seller_lat,x.seller_lng,x.customer_lat,x.customer_lng); x['volume']=x.product_length_cm*x.product_height_cm*x.product_width_cm; x['max_side']=x[['product_length_cm','product_height_cm','product_width_cm']].max(axis=1)
    x['missing_dims']=x[['product_weight_g','product_length_cm','product_height_cm','product_width_cm']].isna().any(axis=1); x['missing_card']=x.product_description_lenght.isna()|x.product_category_name.eq('unknown')
    f=x.groupby('order_id').agg(n_items=('order_item_id','size'),n_products=('product_id','nunique'),n_sellers=('seller_id','nunique'),n_categories=('category','nunique'),total_price=('price','sum'),total_freight=('freight_value','sum'),max_item_price=('price','max'),total_weight_g=('product_weight_g',lambda s:s.sum(min_count=1)),max_weight_g=('product_weight_g','max'),total_volume_cm3=('volume',lambda s:s.sum(min_count=1)),max_side_cm=('max_side','max'),has_missing_dims=('missing_dims','max'),min_photos_qty=('product_photos_qty','min'),min_description_length=('product_description_lenght','min'),has_missing_card=('missing_card','max')).reset_index()
    top=x.sort_values(['order_id','price','order_item_id'],ascending=[True,False,True]).drop_duplicates('order_id'); f=f.merge(top[['order_id','category','seller_id']].rename(columns={'category':'main_category','seller_id':'main_seller_id'}),on='order_id')
    far=x.sort_values(['order_id','distance_km','seller_id'],ascending=[True,False,True]).drop_duplicates('order_id'); cols=[c for c in far if c.startswith(('customer_','seller_')) and c not in ['customer_id','customer_unique_id','seller_id']]+['distance_km','city_id','route_state']
    f=f.merge(far[['order_id']+cols],on='order_id'); f['is_same_state']=f.seller_state.eq(f.customer_state); f['is_same_city']=f.is_same_state&f.seller_city.eq(f.customer_city); f['is_customer_capital']=f.customer_city.eq(f.customer_state.map(CAPITALS))
    rank={'zip':0,'city':1,'state':2,'none':3}; f['geo_quality']=np.maximum(f.customer_geo_source.map(rank),f.seller_geo_source.map(rank)).map({v:k for k,v in rank.items()}); f=f.drop(columns=['customer_geo_source','seller_geo_source'])
    f['freight_share']=f.total_freight/(f.total_price+f.total_freight); f['cubic_weight_kg']=f.total_volume_cm3/6000; f['is_multi_seller']=f.n_sellers.gt(1)
    pay=t['order_payments'].copy(); pay['voucher']=pay.payment_type.eq('voucher'); pg=pay.groupby('order_id').agg(n_payments=('payment_sequential','size'),has_voucher=('voucher','max'),max_installments=('payment_installments','max'),total_payment_value=('payment_value','sum')).reset_index()
    pt=pay.groupby(['order_id','payment_type'],as_index=False).payment_value.sum().sort_values(['order_id','payment_value','payment_type'],ascending=[True,False,True]).drop_duplicates('order_id').rename(columns={'payment_type':'main_payment_type'})
    f=f.merge(pg,on='order_id',how='left').merge(pt[['order_id','main_payment_type']],on='order_id',how='left')
    times=d[['order_id','customer_unique_id','purchase_ts']].copy(); times['customer_n_prev_orders']=times.groupby('customer_unique_id').purchase_ts.transform(lambda s:np.searchsorted(np.sort(s.values),s.values,side='left')); times['purchase_hour']=times.purchase_ts.dt.hour; times['purchase_day_of_week']=times.purchase_ts.dt.dayofweek
    f=f.merge(times.drop(columns=['customer_unique_id','purchase_ts']),on='order_id'); f=w.write('fct_order_features',f,['order_id'])
    so=x.groupby(['seller_id','order_id']).agg(n_items=('order_item_id','size'),n_products=('product_id','nunique'),revenue=('price','sum'),freight=('freight_value','sum'),customer_state=('customer_state','first'),distance_km=('distance_km','first'),city_id=('city_id','first'),route_state=('route_state','first')).reset_index()
    mt=x.sort_values(['price','order_item_id'],ascending=[False,True]).drop_duplicates(['seller_id','order_id']); so=so.merge(mt[['seller_id','order_id','category']].rename(columns={'category':'main_category'}),on=['seller_id','order_id']).merge(d,on='order_id').merge(rv[['order_id','review_score_min','is_bad_review','answer_ts']],on='order_id',how='left').merge(f[['order_id','is_multi_seller']],on='order_id').rename(columns={'is_multi_seller':'is_multi_seller_order'})
    so['is_canceled']=so.order_status.eq('canceled'); so=w.write('fct_seller_order',so,['seller_id','order_id'])
    x=x.drop(columns='customer_unique_id').merge(d,on='order_id')
    return d,f,rv,sv,x,so

def calendar(d,snapshot,w):
    holidays={}; bfs=[]
    for year,moving in [(2016,['02-08','02-09','03-25','05-26']),(2017,['02-27','02-28','04-14','06-15']),(2018,['02-12','02-13','03-30','05-31']),(2019,['03-04','03-05','04-19','06-20'])]:
        for md,name in [('01-01','New Year'),('04-21','Tiradentes'),('05-01','Labour Day'),('09-07','Independence Day'),('10-12','Our Lady of Aparecida'),('11-02','All Souls'),('11-15','Republic Day'),('12-25','Christmas')]+list(zip(moving,['Carnival','Carnival','Good Friday','Corpus Christi'])): holidays[pd.Timestamp(f'{year}-{md}')]=name
        bfs.append(pd.date_range(f'{year}-11-01',f'{year}-11-30',freq='W-FRI')[3])
    idx=pd.date_range(d.purchase_date.min(),snapshot.normalize()+74*DAY); c=pd.DataFrame({'date':idx}); c['day_of_week']=c.date.dt.dayofweek; c['month']=c.date.dt.month; c['is_weekend']=c.day_of_week.ge(5); c['holiday_name']=c.date.map(holidays); c['is_holiday']=c.holiday_name.notna()
    c['business_days_next_14d']=pd.Series((~c.is_weekend&~c.is_holiday).astype(int).values[::-1]).rolling(14,min_periods=1).sum().values[::-1].astype(int)
    hs=np.array(sorted(holidays),dtype='datetime64[ns]'); c['days_to_next_holiday']=(hs[np.searchsorted(hs,c.date.values)]-c.date.values)/np.timedelta64(1,'D'); c['is_black_friday_week']=False
    for b in bfs: c.loc[c.date.ge(b)&c.date.lt(b+7*DAY),'is_black_friday_week']=True
    c['is_xmas_season']=c.month.eq(12); c['is_truckers_strike']=c.date.between('2018-05-21','2018-05-31'); counts=d.groupby('purchase_date').size().reindex(idx,fill_value=0); past=counts.shift(1,fill_value=0)
    c['platform_orders_1d']=past.values; c['platform_orders_7d']=past.rolling(7,min_periods=1).sum().values; denom=past.rolling(28,min_periods=1).sum().values; c['platform_orders_7d_vs_28d']=c.platform_orders_7d*4/np.where(denom==0,np.nan,denom)
    c['platform_in_transit']=np.searchsorted(np.sort(d.carrier_ts.dropna().values),idx.values,side='left')-np.searchsorted(np.sort(d.loc[d.carrier_ts.notna()&d.delivered_ts.notna(),['carrier_ts','delivered_ts']].max(axis=1).values),idx.values,side='left')
    return w.write('dim_calendar',c[c.date.le(snapshot.normalize()+60*DAY)],['date']),bfs
