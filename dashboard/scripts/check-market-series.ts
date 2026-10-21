import { chartSeries } from '../src/v3/MarketPage';
import type { MarketPoint } from '../src/v3/types';
function check(value: boolean, message: string) { if (!value) throw new Error(message); }
const point = (ts: number, extra: Partial<MarketPoint> = {}) => ({ ts, valid:true, mid_basis_bps:'1.25', reference_bps:'1.1', reference_usable:true, ...extra } as MarketPoint);
const sparse = [point(100), point(500), point(1000)];
check(chartSeries(sparse, true).length === 3, 'Downsampling intervals must not create capture gaps');
const gap = chartSeries([point(100), point(500, {continuity_break:true})], true);
check(gap.length === 3 && gap[1].mid === null, 'A recorded capture gap must break the line');
const unusable = chartSeries([point(100, {reference_usable:false}), point(200, {valid:false})], true);
check(unusable[0].mid === 1.25 && unusable[0].reference === null, 'Unusable reference cannot be plotted');
check(unusable[1].mid === null, 'Invalid source must remain blank');
check(chartSeries([point(100, {session_id:'a'}), point(200, {session_id:'b'})], true).length === 3, 'Recorded session boundary must remain visible');
check(chartSeries([point(100), point(200)], true).length === 2, 'Unrecorded sessions must not be fabricated');
check(chartSeries([point(100, {long_edge_bps:'3'}), point(200, {valid:false,long_edge_bps:'9'})])[1].long === null, 'Invalid fixed quantity quote cannot be plotted');
console.log('PASS: 7 market series checks; sampling gaps, genuine boundaries, invalid quotes and unknown references');
