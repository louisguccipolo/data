from common import *

def build(engine,t,x,so,cal,cfg,w):
    so=so.copy(); so['month']=so.purchase_ts.dt.to_period('M').dt.to_timestamp()
    xm=x.copy(); xm['month']=xm.purchase_ts.dt.to_period('M').dt.to_timestamp()
    mg=so.groupby(['seller_id','month']).agg(n_orders=('order_id','size'),n_items=('n_items','sum'),revenue=('revenue','sum'),n_customers=('customer_unique_id','nunique'),n_customer_states=('customer_state','nunique'),n_canceled=('is_canceled','sum'))
    mg['n_products_sold']=xm.groupby(['seller_id','month']).product_id.nunique()
    first=so.groupby('seller_id').month.min(); end=(cfg.sales_end-DAY).to_period('M').to_timestamp()
    grid=pd.DataFrame([(s,m) for s,start in first.items() for m in pd.date_range(start,end,freq='MS')],columns=['seller_id','month'])
    monthly=grid.merge(mg.reset_index(),on=['seller_id','month'],how='left').fillna(0); monthly['revenue_share_platform']=monthly.revenue/monthly.groupby('month').revenue.transform('sum').replace(0,np.nan)
    w.write('agg_seller_monthly',monthly,['seller_id','month'])
    snapshots=[]; labels=[]
    dates=pd.date_range('2017-04-01',cfg.sales_end,freq='MS'); profile=t['sellers'].set_index('seller_id'); itemcats=x[['seller_id','order_id','category','price','product_id','purchase_ts']]
    for date in dates:
        past=so[so.purchase_ts.lt(date)]; ix=pd.Index(sorted(past.seller_id.unique()),name='seller_id'); a=pd.DataFrame(index=ix)
        a['tenure_days']=(date-past.groupby('seller_id').purchase_ts.min()).dt.total_seconds()/86400; a['recency_days']=(date-past.groupby('seller_id').purchase_ts.max()).dt.total_seconds()/86400
        wins={n:past[past.purchase_ts.ge(date-n*DAY)] for n in [90,180]}; wins['lifetime']=past
        for n,v in wins.items():
            g=v.groupby('seller_id'); a[f'n_orders_{n}'+('d' if isinstance(n,int) else '')]=g.size().reindex(ix,fill_value=0); a[f'revenue_{n}'+('d' if isinstance(n,int) else '')]=g.revenue.sum().reindex(ix,fill_value=0)
        v=wins[180]; g=v.groupby('seller_id'); v90=wins[90]; prev=past[past.purchase_ts.ge(date-180*DAY)&past.purchase_ts.lt(date-90*DAY)].groupby('seller_id').revenue.sum().reindex(ix,fill_value=0)
        a['revenue_growth_90d']=(a.revenue_90d+1)/(prev+1); a['revenue_share_platform_90d']=a.revenue_90d/a.revenue_90d.sum(); a['revenue_rank_pct_90d']=a.loc[a.n_orders_90d.gt(0),'revenue_90d'].rank(pct=True)
        a['n_active_weeks_90d']=v90.assign(week=v90.purchase_ts.dt.to_period('W')).groupby('seller_id').week.nunique().reindex(ix,fill_value=0)
        gaps=v.sort_values('purchase_ts').groupby('seller_id').purchase_ts.diff().dt.total_seconds()/86400; gv=v.assign(gap=gaps).groupby('seller_id').gap.max().reindex(ix)
        start=v.groupby('seller_id').purchase_ts.min(); last=v.groupby('seller_id').purchase_ts.max()
        window_start=past.groupby('seller_id').purchase_ts.min().clip(lower=date-180*DAY)
        # Include edge gaps within the observed window, including the current inactivity spell.
        a['max_gap_days_180d']=pd.concat([gv,(start-window_start).dt.total_seconds()/86400,(date-last).dt.total_seconds()/86400],axis=1).max(axis=1)
        weeks=pd.date_range(date-182*DAY,date,freq='7D',inclusive='left'); counts=v.assign(bin=((v.purchase_ts-(date-182*DAY)).dt.days//7)).groupby(['seller_id','bin']).size().unstack(fill_value=0).reindex(index=ix,columns=range(26),fill_value=0).fillna(0)
        centered=np.arange(26)-12.5; a['orders_trend_slope_180d']=counts.to_numpy()@centered/(centered@centered)
        items=x[x.purchase_ts.ge(date-180*DAY)&x.purchase_ts.lt(date)]; ig=items.groupby('seller_id')
        a['n_products_180d']=ig.product_id.nunique().reindex(ix,fill_value=0); a['n_categories_180d']=ig.category.nunique().reindex(ix,fill_value=0)
        cr=items.groupby(['seller_id','category']).price.sum().reset_index().sort_values(['seller_id','price','category'],ascending=[True,False,True]).drop_duplicates('seller_id').set_index('seller_id')
        a['main_category']=cr.category.reindex(ix).fillna('unknown'); a['category_concentration_180d']=cr.price.reindex(ix)/a.revenue_180d.replace(0,np.nan)
        a['avg_item_price_180d']=ig.price.mean().reindex(ix); a['freight_share_180d']=g.freight.sum().reindex(ix)/(a.revenue_180d+g.freight.sum().reindex(ix))
        for col,out in [('customer_unique_id','n_customers_180d'),('customer_state','n_customer_states_180d')]: a[out]=g[col].nunique().reindex(ix,fill_value=0)
        a['avg_distance_km_180d']=g.distance_km.mean().reindex(ix); a['multi_seller_order_share_180d']=g.is_multi_seller_order.mean().reindex(ix)
        sh=so[so.carrier_ts.ge(date-180*DAY)&so.carrier_ts.lt(date)&so.handling_days.ge(0)&so.approval_hours.ge(0)]; a['handling_days_median_180d']=sh.groupby('seller_id').handling_days.median().reindex(ix)
        dl=so[so.delivered_ts.ge(date-180*DAY)&so.delivered_ts.lt(date)]
        a['late_share_180d']=dl.groupby('seller_id').is_late.mean().reindex(ix).astype(float); a['late_share_single_seller_180d']=dl[~dl.is_multi_seller_order].groupby('seller_id').is_late.mean().reindex(ix).astype(float)
        rev=engine.known_reviews(date); rev=rev[rev.last_answer.ge(date-180*DAY)]; sr=so[['seller_id','order_id']].merge(rev,on='order_id'); cg=engine.oc.merge(rev,on='order_id').groupby('category').bad.mean(); parent=a.main_category.map(cg).fillna(rev.bad.mean())
        z=sr.groupby('seller_id').bad.agg(['count','mean']).reindex(ix); a['n_reviews_180d']=z['count'].fillna(0); a['bad_review_share_180d_smoothed']=smooth(z['count'].fillna(0),z['mean'],parent,cfg.smoothing_k)
        on=sr[sr.ontime]; co=engine.oc.merge(rev[rev.ontime],on='order_id').groupby('category').bad.mean(); po=a.main_category.map(co).fillna(rev[rev.ontime].bad.mean()); zo=on.groupby('seller_id').bad.agg(['count','mean']).reindex(ix)
        a['bad_review_share_on_time_180d_smoothed']=smooth(zo['count'].fillna(0),zo['mean'],po,cfg.smoothing_k); a['bad_review_share_vs_category']=a.bad_review_share_180d_smoothed-parent; a['mean_score_180d']=sr.groupby('seller_id').score.mean().reindex(ix)
        # No cancellation timestamps exist in Olist; the final status is not an as-of feature.
        a['cancel_share_180d']=np.nan
        a['seller_state']=profile.seller_state.reindex(ix); a['seller_region']=a.seller_state.map(REGIONS); a['is_seller_capital']=profile.seller_city.reindex(ix).eq(a.seller_state.map(CAPITALS)); a['platform_orders_7d_vs_28d']=cal.set_index('date').loc[date,'platform_orders_7d_vs_28d']; a['is_low_data']=a.n_orders_lifetime.lt(5)
        a=a.reset_index(); a['snapshot_date']=date; snapshots.append(a)
        if date+cfg.churn_window_days*DAY<=cfg.sales_end:
            active=a[a.n_orders_90d.gt(0)][['seller_id','snapshot_date']].copy(); future=so[so.purchase_ts.ge(date)&so.purchase_ts.lt(date+cfg.churn_window_days*DAY)].groupby('seller_id'); active['n_orders_next_60d']=active.seller_id.map(future.size()).fillna(0).astype(int); active['revenue_next_60d']=active.seller_id.map(future.revenue.sum()).fillna(0); active['is_churned']=active.n_orders_next_60d.eq(0)
            later=so[so.purchase_ts.ge(date+cfg.churn_window_days*DAY)&so.purchase_ts.lt(cfg.sales_end)]; active['is_returned_later']=active.is_churned&active.seller_id.isin(later.seller_id); active['label_available_ts']=date+cfg.churn_window_days*DAY; labels.append(active)
    snap=w.write('mart_seller_snapshot',pd.concat(snapshots,ignore_index=True),['seller_id','snapshot_date']); lab=w.write('fct_seller_churn_label',pd.concat(labels,ignore_index=True),['seller_id','snapshot_date'])
    ds=w.write('ml_seller_churn_dataset',snap.merge(lab,on=['seller_id','snapshot_date'],validate='one_to_one'),['seller_id','snapshot_date'])
    last=snap[snap.snapshot_date.eq(snap.snapshot_date.max())]; seg=w.write('ml_seller_segmentation_dataset',last[last.n_orders_90d.gt(0)&~last.is_low_data],['seller_id','snapshot_date'])
    features=[c for c in snap if c not in ['seller_id','snapshot_date','cancel_share_180d']]
    return ds,seg,features
