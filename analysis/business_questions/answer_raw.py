"""Calculate slide 3 answers from checksum-verified data/scripts/download_data.sh CSVs."""
import csv, sys, json
from pathlib import Path
from collections import Counter, defaultdict
from decimal import Decimal as D
root=Path(sys.argv[1]); out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
def read(name):
 with (root/(name+'.csv')).open() as f:return list(csv.DictReader(f))
def save(name,rows):
 with (out/(name+'.csv')).open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
orders={r['order_id']:r for r in read('olist_orders_dataset')}
items=read('olist_order_items_dataset'); customers=read('olist_customers_dataset')
sellers={r['seller_id']:r for r in read('olist_sellers_dataset')}
products={r['product_id']:r for r in read('olist_products_dataset')}
months={f'{y}-{m:02}':[set(),D(0)] for y in (2016,2017,2018) for m in range(1,13) if '2016-09'<=f'{y}-{m:02}'<='2018-08'}
seller=defaultdict(lambda:[set(),0,D(0)]);cat=defaultdict(lambda:D(0));checks=defaultdict(lambda:[D(0),0])
for r in items:
 o=orders[r['order_id']]
 if o['order_status']!='delivered':continue
 price=D(r['price']);oid=r['order_id'];month=o['order_purchase_timestamp'][:7]
 if month in months:months[month][0].add(oid);months[month][1]+=price
 v=seller[r['seller_id']];v[0].add(oid);v[1]+=1;v[2]+=price
 category=products.get(r['product_id'],{}).get('product_category_name') or 'unknown'
 cat[category]+=price
 checks[oid][0]+=price+D(r['freight_value']);checks[oid][1]+=1
save('monthly_sales',[dict(month=k,orders=len(v[0]),revenue=str(v[1])) for k,v in months.items()])
save('top_sellers',[dict(seller_id=k,city=sellers[k]['seller_city'],state=sellers[k]['seller_state'],orders=len(v[0]),units=v[1],revenue=str(v[2])) for k,v in sorted(seller.items(),key=lambda x:x[1][2],reverse=True)[:10]])
total=sum(cat.values());save('categories',[dict(category=k,revenue=str(v),share_pct=str(round(v/total*100,4))) for k,v in sorted(cat.items(),key=lambda x:x[1],reverse=True)])
cnt=Counter(r['customer_unique_id'] for r in customers);repeat=sum(v>1 for v in cnt.values())
summary=dict(orders=len(orders),items=len(items),statuses=dict(Counter(r['order_status'] for r in orders.values())),delivered_orders_with_items=len(checks),revenue=str(total),average_order_with_freight=str(round(sum(v[0] for v in checks.values())/len(checks),2)),average_lines_and_units=str(sum(v[1] for v in checks.values())/len(checks)),customers=len(cnt),repeat_customers=repeat,repeat_pct=round(repeat/len(cnt)*100,4),categories=len(cat),monthly_revenue=str(sum(v[1] for v in months.values())))
assert len(months)==24 and months['2016-11'][1]==0
assert total==D('13221498.11')==sum(v[1] for v in months.values())
assert max(v[2] for v in seller.values())==D('226987.93')
assert summary['average_order_with_freight']=='159.88'
assert len(cnt)==96096 and repeat==2997 and len(cat)==74
assert max(cat.values())==D('1233131.72')
assert len(checks)==sum(o['order_status']=='delivered' for o in orders.values())
(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
