"""Point-in-time aggregates. Every observation is filtered by its event clock."""
from common import *

KEYS={'seller':['seller_id'],'route':['route_state'],'city':['city_id'],'state':['seller_id','customer_state'],'pair':['seller_id','city_id'],'approval':['payment_type'],'category':['category'],'product':['product_id'],'customer':['customer_unique_id']}
NAMES={'seller':'agg_seller_history','route':'agg_route_history','city':'agg_city_history','state':'agg_seller_state_history','pair':'agg_seller_city_history','approval':'agg_approval_delay','category':'agg_category_history','product':'agg_product_history','customer':'agg_customer_history'}

def stats(df,keys,value):
    return df.groupby(keys)[value].agg(['count','mean','median'])

def reindexed(df,keys,idx): return df.reindex(idx)

def count(df,keys,idx): return df.groupby(keys).size().reindex(idx,fill_value=0)

def requests(d,f,rv,x,so,cfg):
    orders=d[['order_id','purchase_date','customer_unique_id']].merge(f[['order_id','route_state','city_id','customer_state','main_payment_type']],on='order_id').merge(rv[['order_id','survey_date']],on='order_id',how='left')
    dates=pd.concat([orders.rename(columns={'purchase_date':'as_of_date'}).drop(columns='survey_date'),orders.rename(columns={'survey_date':'as_of_date'}).drop(columns='purchase_date')]).dropna(subset='as_of_date')
    req={}
    bridge=x[['order_id','seller_id','product_id','category']].drop_duplicates()
    q=dates.merge(bridge,on='order_id')
    for name,keys in KEYS.items():
        if name=='approval':
            req[name]=dates.rename(columns={'main_payment_type':'payment_type'})[['payment_type','as_of_date']].dropna().drop_duplicates()
        else: req[name]=q[keys+['as_of_date']].drop_duplicates()
    return req

class Histories:
    def __init__(self,t,d,f,rv,x,so,cfg):
        self.t,self.d,self.f,self.rv,self.x,self.so,self.cfg=t,d,f,rv,x,so,cfg
        self.ctx=d.merge(f[['order_id','route_state','city_id','customer_state','main_payment_type']],on='order_id')
        self.op=x[['order_id','product_id','category']].drop_duplicates(); self.oc=x[['order_id','category']].drop_duplicates()
        self.sale=x[['product_id','category','purchase_ts']]
    def known_reviews(self,date):
        # The final minimum score must never be attributed to the first answer retroactively.
        r=self.t['order_reviews'].merge(self.d[['order_id','purchase_ts']],on='order_id',validate='many_to_one')
        r=r[r.review_answer_timestamp.lt(date)&r.review_answer_timestamp.ge(r.purchase_ts)&r.purchase_ts.lt(date)]
        r=r.groupby('order_id').agg(score=('review_score','min'),last_answer=('review_answer_timestamp','max')).reset_index()
        r['bad']=r.score.le(2)
        r=r.merge(self.d[['order_id','customer_unique_id','delivered_ts','is_late']],on='order_id')
        r['ontime']=r.delivered_ts.lt(date)&r.is_late.eq(False).fillna(False)
        return r
    def at(self,date,need):
        d=self.d; so=self.so; ctx=self.ctx; k=self.cfg.smoothing_k
        shipped=d[d.carrier_ts.lt(date)&d.handling_days.ge(0)&d.approval_hours.ge(0)]
        delivered=d[d.delivered_ts.lt(date)&d.delivery_days_from_approval.ge(0)&d.delivery_days_from_purchase.gt(0)&~d.has_stage_anomaly]
        validids=set(delivered.order_id); dc=ctx[ctx.order_id.isin(validids)]
        ds=so[so.order_id.isin(validids)]; sh=so[so.order_id.isin(shipped.order_id)]
        past=so[so.purchase_ts.lt(date)]; cp=ctx[ctx.purchase_ts.lt(date)]
        rev=self.known_reviews(date); on=rev[rev.ontime]
        gm=delivered.delivery_days_from_approval.mean(); gt=delivered.transit_days.mean(); gh=shipped.handling_days.mean(); gl=delivered.is_late.astype(float).mean(); gb=rev.bad.mean(); go=on.bad.mean()
        out={}
        def index(name):
            keys=KEYS[name]; z=need[name][keys].drop_duplicates()
            return pd.Index(z[keys[0]],name=keys[0]) if len(keys)==1 else pd.MultiIndex.from_frame(z)
        # Full parent statistics support smoothing even if that parent is not explicitly requested.
        route=stats(dc,'route_state','transit_days'); route_delivery=stats(dc,'route_state','delivery_days_from_approval'); statecity=stats(dc,'customer_state','transit_days'); state_late=dc.groupby('customer_state').is_late.mean()
        route_sm=smooth(route['count'],route['mean'],gt,k); route_dm=smooth(route_delivery['count'],route_delivery['mean'],gm,k)
        statefull=stats(ds,['seller_id','customer_state'],'delivery_days_from_approval')
        state_keys=so[['seller_id','customer_state']].drop_duplicates()
        if 'state' in need:
            state_keys=pd.concat([state_keys,need['state'][['seller_id','customer_state']]]).drop_duplicates()
        seller_states=self.t['sellers'].set_index('seller_id').seller_state
        state_keys['route_state']=state_keys.seller_id.map(seller_states)+'-'+state_keys.customer_state
        route_lookup=state_keys.set_index(['seller_id','customer_state']).route_state
        state_parent=route_lookup.reindex(statefull.index).map(route_dm).fillna(gm)
        state_sm=smooth(statefull['count'],statefull['mean'],state_parent,k)
        # Category priors are computed from unique order-category pairs and known answers only.
        cr=self.oc.merge(rev,on='order_id'); co=cr[cr.ontime]
        cg=cr.groupby('category').bad.agg(['count','mean']); cog=co.groupby('category').bad.agg(['count','mean'])
        cat_sm=smooth(cg['count'],cg['mean'],gb,k); cat_on=smooth(cog['count'],cog['mean'],go,k)
        for name,queries in need.items():
            ix=index(name); a=pd.DataFrame(index=ix)
            if name=='seller':
                for source,metric,prefix,parent in [(sh,'handling_days','seller_handling_days',gh),(ds,'delivery_days_from_approval','seller_delivery_days',gm)]:
                    z=stats(source,'seller_id',metric).reindex(ix); n=z['count'].fillna(0)
                    a[prefix+'_mean']=z['mean']; a[prefix+'_median']=z['median']; a[prefix+'_smoothed']=smooth(n,z['mean'],parent,k)
                a['seller_n_orders_total']=count(past,'seller_id',ix); a['seller_n_shipped']=count(sh,'seller_id',ix); a['seller_n_delivered']=count(ds,'seller_id',ix)
                a['seller_age_days']=(date-past.groupby('seller_id').purchase_ts.min().reindex(ix)).dt.total_seconds()/86400
                late=ds.groupby('seller_id').is_late.mean().reindex(ix).astype(float); a['seller_late_share']=late; a['seller_late_share_smoothed']=smooth(a.seller_n_delivered,late,gl,k)
                for window in [7,30]: a[f'seller_orders_last_{window}d']=count(past[past.purchase_ts.ge(date-window*DAY)],'seller_id',ix)
                a['seller_backlog']=count(past[past.carrier_ts.isna()|past.carrier_ts.ge(date)],'seller_id',ix)
                sr=so[['seller_id','order_id']].merge(rev,on='order_id'); sg=sr.groupby('seller_id').bad.agg(['count','mean']).reindex(ix)
                a['seller_n_reviews']=sg['count'].fillna(0); a['seller_mean_score']=sr.groupby('seller_id').score.mean().reindex(ix); a['seller_bad_review_share']=sg['mean']; a['seller_bad_review_share_smoothed']=smooth(a.seller_n_reviews,sg['mean'],gb,k)
                sg=sr[sr.ontime].groupby('seller_id').bad.agg(['count','mean']).reindex(ix); a['seller_n_reviews_on_time']=sg['count'].fillna(0); a['seller_bad_review_share_on_time_smoothed']=smooth(sg['count'].fillna(0),sg['mean'],go,k)
                a['seller_bad_review_share_last_90d']=sr[sr.last_answer.ge(date-90*DAY)].groupby('seller_id').bad.mean().reindex(ix)
            elif name=='route':
                z=route.reindex(ix); a['route_n_delivered']=count(dc,'route_state',ix)
                for suffix,col in [('mean','mean'),('median','median')]: a['route_transit_days_'+suffix]=z[col]
                a['route_transit_days_p90']=dc.groupby('route_state').transit_days.quantile(.9).reindex(ix)
                a['route_transit_days_smoothed']=smooth(z['count'].fillna(0),z['mean'],gt,k)
                a['route_transit_days_last_30d']=dc[dc.delivered_ts.ge(date-30*DAY)].groupby('route_state').transit_days.mean().reindex(ix)
                a['route_late_share']=dc.groupby('route_state').is_late.mean().reindex(ix).astype(float)
            elif name=='city':
                z=stats(dc,'city_id','transit_days').reindex(ix); st=pd.Series(ix,index=ix).str.split('|').str[0]
                state_prior=smooth(statecity['count'],statecity['mean'],gt,k); state_n=dc.groupby('customer_state').size(); late_prior=smooth(state_n,state_late.astype(float),gl,k)
                parent=st.map(state_prior).fillna(gt); lateparent=st.map(late_prior).fillna(gl)
                a['city_n_delivered']=count(dc,'city_id',ix); a['city_transit_days_median']=z['median']; a['city_transit_days_smoothed']=smooth(z['count'].fillna(0),z['mean'],parent,k); a['city_transit_vs_state']=a.city_transit_days_smoothed-parent
                late=dc.groupby('city_id').is_late.mean().reindex(ix).astype(float); a['city_late_share_smoothed']=smooth(a.city_n_delivered,late,lateparent,k)
                a['city_transit_days_last_30d']=dc[dc.delivered_ts.ge(date-30*DAY)].groupby('city_id').transit_days.mean().reindex(ix)
                a['city_orders_last_30d']=count(cp[cp.purchase_ts.ge(date-30*DAY)],'city_id',ix)
            elif name=='state':
                z=statefull.reindex(ix); parent=route_lookup.reindex(ix).map(route_dm).fillna(gm)
                a['seller_state_n_delivered']=count(ds,KEYS[name],ix); a['seller_state_delivery_days_median']=z['median']; a['seller_state_delivery_days_smoothed']=smooth(z['count'].fillna(0),z['mean'],parent,k)
                tr=stats(ds,KEYS[name],'transit_days').reindex(ix); tp=route_lookup.reindex(ix).map(route_sm).fillna(gt)
                a['seller_state_transit_vs_route']=smooth(tr['count'].fillna(0),tr['mean'],tp,k)-tp
            elif name=='pair':
                z=stats(ds,KEYS[name],'delivery_days_from_approval').reindex(ix)
                stateix=pd.MultiIndex.from_arrays([ix.get_level_values('seller_id'),ix.get_level_values('city_id').str.split('|').str[0]],names=['seller_id','customer_state'])
                par=state_sm.reindex(stateix).values
                # Missing seller-state falls back through the route, then global mean.
                sellers=self.t['sellers'].set_index('seller_id').seller_state; routes=ix.get_level_values('seller_id').map(sellers)+'-'+stateix.get_level_values('customer_state')
                fallback=pd.Series(routes.map(route_dm).fillna(gm).values,index=ix)
                par=pd.Series(par,index=ix).fillna(fallback)
                a['pair_n_delivered']=count(ds,KEYS[name],ix); a['pair_delivery_days_median']=z['median']; a['pair_delivery_days_smoothed']=smooth(z['count'].fillna(0),z['mean'],par,k)
                last=ds.sort_values(['delivered_ts','order_id']).groupby(KEYS[name]).last().reindex(ix)
                a['pair_last_delivery_days']=last.delivery_days_from_approval; a['pair_days_since_last_delivery']=(date-last.delivered_ts).dt.total_seconds()/86400
            elif name=='approval':
                v=ctx[ctx.approved_ts.lt(date)&ctx.approved_ts.ge(date-30*DAY)&ctx.approval_hours.ge(0)].rename(columns={'main_payment_type':'payment_type'})
                a['approval_n_30d']=count(v,'payment_type',ix)
                for q in [.5,.9]: a[f'approval_hours_p{int(q*100)}_30d']=v.groupby('payment_type').approval_hours.quantile(q).reindex(ix)
            elif name=='category':
                z=cg.reindex(ix); zo=cog.reindex(ix); a['category_n_reviews']=z['count'].fillna(0); a['category_mean_score']=cr.groupby('category').score.mean().reindex(ix)
                a['category_bad_review_share_smoothed']=smooth(z['count'].fillna(0),z['mean'],gb,k); a['category_bad_review_share_on_time_smoothed']=smooth(zo['count'].fillna(0),zo['mean'],go,k)
                a['category_late_share']=self.oc.merge(delivered[['order_id','is_late']],on='order_id').groupby('category').is_late.mean().reindex(ix).astype(float)
            elif name=='product':
                pr=self.op.merge(rev,on='order_id'); cats=self.op.drop_duplicates('product_id').set_index('product_id').category.reindex(ix)
                for mask,suffix,prior in [(pr.index==pr.index,'',cats.map(cat_sm).fillna(gb)),(pr.ontime,'_on_time',cats.map(cat_on).fillna(go))]:
                    z=pr[mask].groupby('product_id').bad.agg(['count','mean']).reindex(ix)
                    if suffix=='': a['product_n_reviews']=z['count'].fillna(0)
                    a[f'product_bad_review_share{suffix}_smoothed']=smooth(z['count'].fillna(0),z['mean'],prior,k)
                a['product_n_sold']=count(self.sale[self.sale.purchase_ts.lt(date)],'product_id',ix)
            elif name=='customer':
                g=rev.groupby('customer_unique_id'); a['customer_n_prev_reviews']=g.size().reindex(ix,fill_value=0); a['customer_prev_min_score']=g.score.min().reindex(ix); a['customer_prev_mean_score']=g.score.mean().reindex(ix)
                late=d[d.delivered_ts.lt(date)&d.is_late.eq(True)]; a['customer_had_late_order']=ix.isin(late.customer_unique_id)
            a=a.reset_index(); a['as_of_date']=date; out[name]=a
        return out

def build(engine,req,w):
    groups={name:{date:g for date,g in df.groupby('as_of_date')} for name,df in req.items()}; dates=sorted(set().union(*(set(v) for v in groups.values()))); collected={name:[] for name in req}
    for i,date in enumerate(dates):
        need={n:gg[date] for n,gg in groups.items() if date in gg}
        for name,df in engine.at(date,need).items(): collected[name].append(df)
        if i%50==0: print(f'History: {i+1}/{len(dates)} days ({date.date()})',flush=True)
    result={}
    for name,frames in collected.items():
        df=pd.concat(frames,ignore_index=True); result[name]=df
        # Preserve cold-start rows in memory for feature assembly; sparse pair export follows the spec.
        export=df[df.pair_n_delivered.gt(0)] if name=='pair' else df
        w.write(NAMES[name],export,KEYS[name]+['as_of_date'])
    return result
