from common import *

def expand(frame):
    parts=[]
    for level,cat,region in [('total',False,False),('category',True,False),('region',False,True),('pair',True,True)]:
        a=frame.copy(); a['level']=level
        if not cat: a['category_group']='*'
        if not region: a['region']='*'
        a['series_id']=a.level+'|'+a.category_group+'|'+a.region; parts.append(a)
    return pd.concat(parts,ignore_index=True)

def build(engine,x,cal,bfs,cfg,w):
    fit=x[x.purchase_ts.ge(cfg.train_from)&x.purchase_ts.lt(cfg.category_fit_to)][['order_id','category']].drop_duplicates(); counts=fit.groupby('category').size().sort_values(ascending=False)
    # Stable tie breaking, and a frozen training-only category map.
    rank=counts.rename('n').reset_index().sort_values(['n','category'],ascending=[False,True]); rank['rank_by_orders']=range(1,len(rank)+1)
    cats=pd.DataFrame({'category':sorted(x.category.unique())}).merge(rank[['category','rank_by_orders','n']],on='category',how='left'); cats['rank_by_orders']=cats.rank_by_orders.fillna(len(rank)+1).astype(int); cats['share_of_orders']=cats.n.fillna(0)/fit.order_id.nunique(); cats['category_group']=np.where(cats.rank_by_orders.le(15),cats.category,'other'); cats=cats.drop(columns='n'); w.write('dim_category_group',cats,['category'])
    a=x.merge(cats[['category','category_group']],on='category'); a['region']=a.customer_region; a['week_start']=a.purchase_ts.dt.normalize()-pd.to_timedelta(a.purchase_ts.dt.dayofweek,unit='D')
    first=pd.Timestamp(cfg.demand_from); end=cfg.sales_end; a=a[a.purchase_ts.ge(first)&a.purchase_ts.lt(end)]
    rows=expand(a); weeks=pd.date_range(first,end-DAY,freq='W-MON')
    cg=sorted(cats.category_group.unique()); regions=sorted(set(REGIONS.values())); ids=[('total','*','*')]+[('category',c,'*') for c in cg]+[('region','*',r) for r in regions]+[('pair',c,r) for c in cg for r in regions]
    grid=pd.DataFrame([(level+'|'+c+'|'+r,date,level,c,r) for level,c,r in ids for date in weeks],columns=['series_id','week_start','level','category_group','region'])
    keys=['series_id','week_start']; g=rows.groupby(keys); weekly=g.agg(n_orders=('order_id','nunique'),n_items=('order_item_id','size'),revenue=('price','sum'),n_customers=('customer_unique_id','nunique'),avg_item_price=('price','mean')).reset_index()
    canceled=rows[rows.order_status.eq('canceled')].groupby(keys).order_id.nunique().rename('n_canceled').reset_index(); weekly=grid.merge(weekly,on=keys,how='left').merge(canceled,on=keys,how='left')
    for c in ['n_orders','n_items','revenue','n_customers','n_canceled']: weekly[c]=weekly[c].fillna(0)
    for c in ['n_orders','n_items','n_customers','n_canceled']: weekly[c]=weekly[c].astype(int)
    weekly['is_partial_week']=weekly.week_start.lt(first)|((weekly.week_start+7*DAY)>end); weekly=w.write('agg_demand_weekly',weekly,keys)
    cw=cal.copy(); cw['week_start']=cw.date-pd.to_timedelta(cw.date.dt.dayofweek,unit='D'); cw['business']=~cw.is_weekend&~cw.is_holiday; cw['pre_xmas']=cw.month.eq(12)&cw.date.dt.day.le(24); cw['carnival']=cw.holiday_name.eq('Carnival')
    wk=cw.groupby('week_start').agg(n_holidays=('is_holiday','sum'),n_business_days=('business','sum'),is_pre_xmas=('pre_xmas','max'),is_carnival_week=('carnival','max'),is_truckers_strike=('is_truckers_strike','max')).reset_index()
    wk['week_of_year']=wk.week_start.dt.isocalendar().week.astype(int); wk['month']=(wk.week_start+3*DAY).dt.month; bfweeks=[b-pd.Timedelta(days=b.dayofweek) for b in bfs]; wk['is_black_friday_week']=wk.week_start.isin(bfweeks); wk['is_post_black_friday_week']=wk.week_start.isin([b+7*DAY for b in bfweeks]); wk['days_to_black_friday']=wk.week_start.map(lambda date:min((b-date).days for b in bfs if b>=date))
    wk=w.write('dim_calendar_week',wk,['week_start'])
    m=weekly[['series_id','week_start','level','category_group','region']].copy().rename(columns={'week_start':'origin'}); grp=weekly.groupby('series_id',sort=False); y=weekly.n_orders.where(~weekly.is_partial_week)
    for lag in [1,2,3,4,52]: m[f'y_lag_{lag}']=y.groupby(weekly.series_id).shift(lag)
    prior=y.groupby(weekly.series_id).shift(1)
    for window in [4,8,13]: m[f'y_mean_{window}w']=prior.groupby(weekly.series_id).transform(lambda s:s.rolling(window,min_periods=window).mean())
    m['y_std_8w']=prior.groupby(weekly.series_id).transform(lambda s:s.rolling(8).std())
    m['y_zero_share_13w']=prior.groupby(weekly.series_id).transform(lambda s:s.rolling(13).apply(lambda v:(v==0).mean(),raw=True))
    xv=np.arange(13)-6; m['y_trend_13w']=prior.groupby(weekly.series_id).transform(lambda s:s.rolling(13).apply(lambda v:float(v@xv/(xv@xv)),raw=True))
    prev4=prior.groupby(weekly.series_id).transform(lambda s:s.shift(4).rolling(4).mean()); m['y_growth_4w']=(m.y_mean_4w+1)/(prev4+1)
    for horizon in range(1,5):
        m[f'y_h{horizon}']=y.groupby(weekly.series_id).shift(-(horizon-1))
        target=m.origin+(horizon-1)*7*DAY; mapping=wk.set_index('week_start')
        for c in ['n_holidays','n_business_days','is_black_friday_week','is_post_black_friday_week','is_pre_xmas','is_carnival_week','days_to_black_friday']: m[f'target_h{horizon}_{c}']=target.map(mapping[c])
    m=m[m.origin.ge(first+13*7*DAY)].copy(); lookup=m.set_index(['series_id','origin']); m['parent_id']=np.where(m.level.eq('pair'),'category|'+m.category_group+'|*','total|*|*')
    ix=pd.MultiIndex.from_arrays([m.parent_id,m.origin],names=['series_id','origin']); parent13=lookup.y_mean_13w.reindex(ix).values; m['share_of_parent_13w']=m.y_mean_13w/np.where(parent13==0,np.nan,parent13); m['parent_y_mean_4w']=lookup.y_mean_4w.reindex(ix).values
    totals=m[m.level.eq('total')].set_index('origin'); m['total_y_growth_4w']=m.origin.map(totals.y_growth_4w); m=m.drop(columns='parent_id')
    # Service cohorts use delivery/answer dates, not the purchase date.
    firstseller=x.groupby('seller_id').purchase_ts.min(); allitems=x.merge(cats[['category','category_group']],on='category'); allitems['region']=allitems.customer_region; allrows=expand(allitems)
    memberships=allrows[['series_id','order_id','category_group','region']].drop_duplicates(['series_id','order_id'])
    delivery=memberships.merge(engine.d[['order_id','delivered_ts','delivery_days_from_purchase','is_late']],on='order_id')
    services=[]
    meta=grid[['series_id','category_group','region']].drop_duplicates().set_index('series_id')
    cat_parent=pd.Series(np.where(meta.category_group.eq('*'),'total|*|*','category|'+meta.category_group+'|*'),index=meta.index)
    region_parent=pd.Series(np.where(meta.region.eq('*'),'total|*|*','region|*|'+meta.region),index=meta.index)
    for i,date in enumerate(sorted(m.origin.unique())):
        date=pd.Timestamp(date); current=allrows[allrows.purchase_ts.ge(date-28*DAY)&allrows.purchase_ts.lt(date)]; previous=allrows[allrows.purchase_ts.ge(date-56*DAY)&allrows.purchase_ts.lt(date-28*DAY)]; ix=pd.Index(grid.series_id.unique(),name='series_id'); s=pd.DataFrame(index=ix); g=current.groupby('series_id')
        s['n_active_sellers_28d']=g.seller_id.nunique().reindex(ix,fill_value=0); new=current[current.seller_id.map(firstseller).ge(date-28*DAY)]; s['n_new_sellers_28d']=new.groupby('series_id').seller_id.nunique().reindex(ix,fill_value=0); s['n_products_sold_28d']=g.product_id.nunique().reindex(ix,fill_value=0)
        s['avg_item_price_28d']=g.price.mean().reindex(ix); s['avg_item_price_change_28d']=s.avg_item_price_28d/previous.groupby('series_id').price.mean().reindex(ix).replace(0,np.nan); s['freight_share_28d']=g.freight_value.sum().reindex(ix)/(g.freight_value.sum().reindex(ix)+g.price.sum().reindex(ix)).replace(0,np.nan)
        dl=delivery[delivery.delivered_ts.ge(date-28*DAY)&delivery.delivered_ts.lt(date)&delivery.delivery_days_from_purchase.ge(0)]; s['region_delivery_days_median_28d']=dl.groupby('series_id').delivery_days_from_purchase.median().reindex(ix); s['region_late_share_28d']=dl.groupby('series_id').is_late.mean().reindex(ix).astype(float)
        r=engine.known_reviews(date); r=r[r.last_answer.ge(date-90*DAY)]; rr=memberships[['series_id','order_id']].merge(r,on='order_id'); s['category_bad_review_share_90d']=rr.groupby('series_id').bad.mean().reindex(ix)
        # Supply is category-wide; logistics is region-wide; review quality is category-wide.
        for col in ['n_active_sellers_28d','n_new_sellers_28d','n_products_sold_28d','category_bad_review_share_90d']:
            s[col]=pd.Series(s[col].reindex(cat_parent.reindex(ix).values).values,index=ix)
        for col in ['region_delivery_days_median_28d','region_late_share_28d']:
            s[col]=pd.Series(s[col].reindex(region_parent.reindex(ix).values).values,index=ix)
        s=s.reset_index(); s['origin']=date; services.append(s)
        if i%20==0: print(f'Demand service: origin {i+1}',flush=True)
    m=m.merge(pd.concat(services,ignore_index=True),on=['series_id','origin'],validate='one_to_one'); m['label_available_ts']=m.origin+28*DAY
    m=w.write('mart_demand_features',m,['series_id','origin']); ds=w.write('ml_demand_dataset',m[m[['y_h1','y_h2','y_h3','y_h4']].notna().all(axis=1)],['series_id','origin'])
    features=[c for c in m if c not in ['series_id','origin','y_h1','y_h2','y_h3','y_h4','label_available_ts']]
    return ds,features
