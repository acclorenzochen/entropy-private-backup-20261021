// Browser plugin not available. Existing Playwright runtime; every control request is intercepted.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.DASHBOARD_URL || 'http://127.0.0.1:5186/';
const output = process.env.EXIT_EVIDENCE_DIR || '/tmp/entropy-exit-delivery';
const overview = JSON.parse(fs.readFileSync(`${output}/live-overview.json`));
const deployments = JSON.parse(fs.readFileSync(`${output}/live-deployments.json`));
const browser = await chromium.launch({ headless: true });
const evidence = [];
const url = `${base}#overview?scope=all`;
const mockCycle = { ...overview.data.profit.cycles[0], cycle_id: 'MOCK-EXIT-SNDK', source_id: 'basis', symbol: 'SNDK',
  entropy_units: 257, hedge_units: -257, quantity_step: '.0001', phase: 'OPEN', unrealized_usd: null };
const mockOverview = structuredClone(overview);
mockOverview.data.profit.holding = { cycle_count: 1, unrealized_usd: null, cycles: [mockCycle] };
for (const deployment of mockOverview.data.deployments) if (deployment.source_id === 'basis') {
  deployment.manual_exit = null; deployment.status.current_state = 'HOLDING'; deployment.status.source_ts = Date.now()/1000;
}
const errors = [];
const context = await browser.newContext({viewport:{width:1440,height:960}});
const page = await context.newPage();
page.on('pageerror', e => errors.push(e.message));
page.on('console', m => {if(m.type() === 'error' || m.type() === 'warning') errors.push(m.text());});
async function shot(name) {const path=`${output}/${name}.png`; await page.screenshot({path,fullPage:false});evidence.push(path);}
try {
  // Real read-only display, with the control boundary physically intercepted.
  await page.route('**/api/v3-control/**', route=>{throw new Error('Real display must not request control');});
  await page.goto(url); await page.getByRole('heading',{name:'交易总览',exact:true}).waitFor();
  await page.getByLabel('当前运行状态').getByText('退出时已平仓 · 新开仓关闭',{exact:true}).waitFor();
  assert.equal(await page.locator('.v3-exit-history').count(),0,'Position card stays simple');
  assert.ok(await page.getByRole('button',{name:'双腿平仓',exact:true}).first().isDisabled());
  await shot('readonly-complete');
  await page.unroute('**/api/v3-control/**');
  await page.route('**/api/v3/**', route => {
    assert.equal(route.request().method(),'GET');
    const value=route.request().url().includes('/deployments')?deployments:mockOverview;
    return route.fulfill({json:value});
  });
  const mock = {phase:'executing',previews:0,confirms:0,executions:0,operation:null,preview:null,drop:false,disabled:false,expired:false,gross:'.0012'};
  const receivedKeys = new Set();
  await page.route('**/api/v3-control/**',async route=>{
    const request=route.request(), path=new URL(request.url()).pathname;
    if(path.endsWith('/session')) return route.fulfill({json:{enabled:!mock.disabled,reason:'模拟：本地服务未启用',csrf_token:'MOCK-CSRF',operation:mock.operation}});
    if(path.endsWith('/preview')) {
      assert.equal(request.method(),'POST');assert.equal(request.headers()['x-entropy-csrf'],'MOCK-CSRF');mock.previews++;
      const body=request.postDataJSON();assert.equal(body.cycle_id,mockCycle.cycle_id);
      mock.preview = {preview_id:'00000000-0000-4000-8000-000000000001',cycle_id:mockCycle.cycle_id,batch_id:'MOCK-BATCH',symbol:'SNDK',
        account_binding:'MOCK-ACCOUNT',authoritative:true,quantity_step:'.0001',entropy_units:257,hedge_units:-257,
        position_observed_ts:Date.now()/1000,expires_at:Date.now()/1000+(mock.expired? -1:30),executable_gross_usd:mock.gross,valuation_ts:mock.gross?Date.now()/1000:null,fees_usd:null,funding_usd:null};
      return route.fulfill({json:mock.preview});
    }
    if(path.endsWith('/confirm')) {
      assert.equal(request.method(),'POST');const body=request.postDataJSON();assert.equal(body.action,'close_both_legs');mock.confirms++;
      if(!receivedKeys.has(body.idempotency_key)){receivedKeys.add(body.idempotency_key);mock.executions++;}
      mock.operation={operation_id:'00000000-0000-4000-8000-000000000002',cycle_id:mockCycle.cycle_id,preview:mock.preview};
      if(mock.drop){mock.drop=false;return route.abort('failed');}
      return route.fulfill({json:{operation_id:mock.operation.operation_id}});
    }
    if(path.includes('/operations/')) {
      const closed=mock.phase==='complete'||mock.phase==='accounting_pending';
      return route.fulfill({json:{operation_id:mock.operation.operation_id,state:mock.phase,active:mock.phase!=='complete',
        accounting_final:mock.phase==='complete',net_pnl_usd:mock.phase==='complete'? '-0.004772':null,
        progress_basis:closed?'authoritative_exit':'ledger',legs:[
          {venue:'entropy',state:'closed',initial_units:257,closed_units:257,remaining_units:0},
          {venue:'hedge',state:closed?'closed':mock.phase==='unknown'?'unknown':'partial',initial_units:257,closed_units:closed?257:120,remaining_units:closed?0:-137}]}});
    }
    throw new Error(`Unexpected control route ${path}`);
  });
  await page.reload();await page.getByRole('button',{name:'双腿平仓',exact:true}).click();
  await page.getByRole('button',{name:'确认平仓两腿',exact:true}).waitFor();
  await page.getByText('多 0.0257',{exact:true}).waitFor();
  await page.getByText('+0.0012 U',{exact:true}).waitFor();
  assert.ok(await page.getByRole('dialog').getByText('待核算',{exact:true}).isVisible());
  assert.equal(mock.executions,0,'Opening confirmation must never execute');
  assert.equal(await page.title(),'收益总览 · Entropy V3');assert.equal(page.url(),url);
  await page.evaluate(()=>window.__overviewNode=document.querySelector('[data-testid=profit-overview]'));
  await shot('mock-confirm-desktop');
  await page.getByRole('button',{name:'取消',exact:true}).click();assert.equal(mock.executions,0);
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();
  await page.getByText('多 0.0257',{exact:true}).waitFor();
  await page.getByRole('button',{name:'确认平仓两腿',exact:true}).dblclick();
  await page.getByText('两腿执行中',{exact:true}).waitFor();assert.equal(mock.executions,1);assert.equal(mock.confirms,1);
  mock.phase='partial';await page.getByText('部分成交 · 仍有剩余敞口',{exact:true}).waitFor();await shot('mock-partial');
  mock.phase='unknown';await page.getByText('订单结果待确认 · UNKNOWN',{exact:true}).waitFor();await shot('mock-unknown');
  await page.getByRole('button',{name:'收起',exact:true}).click();
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();
  await page.getByText('订单结果待确认 · UNKNOWN',{exact:true}).waitFor();assert.equal(mock.executions,1,'Reopen must resume original operation');
  mock.phase='accounting_pending';await page.getByText('已平仓 · 完成待核算',{exact:true}).waitFor();await shot('mock-accounting-pending');
  assert.ok(await page.getByText('净收益 — · 待成交与费用核算',{exact:true}).isVisible());
  mock.phase='complete';await page.getByText('最终净收益 -0.004772 U',{exact:true}).waitFor();await shot('mock-complete');
  assert.equal(await page.evaluate(()=>window.__overviewNode===document.querySelector('[data-testid=profit-overview]')),true);
  assert.equal(page.url(),url,'Exit must not navigate');
  await page.getByRole('button',{name:'收起',exact:true}).click();
  mock.operation=null; mock.expired=true; mock.gross=null;
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();await page.getByRole('button',{name:'重新核对',exact:true}).waitFor();
  assert.equal(mock.executions,1,'Expired receipt must not execute');
  await page.getByRole('button',{name:'取消',exact:true}).click();mock.expired=false;
  // Mobile + keyboard dismissal, no layout overflow.
  await page.setViewportSize({width:390,height:844});
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();await page.getByText('多 0.0257',{exact:true}).waitFor();
  assert.ok(await page.getByText('无法估算',{exact:true}).isVisible());
  assert.ok(await page.getByRole('button',{name:'确认平仓两腿',exact:true}).isEnabled());
  await shot('mock-confirm-mobile');
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  assert.ok(await page.locator('dialog').evaluate(el=>el.scrollWidth<=el.clientWidth+1));
  await page.keyboard.press('Escape');assert.ok(await page.getByRole('button',{name:'双腿平仓',exact:true}).evaluate(el=>el===document.activeElement));
  await page.setViewportSize({width:320,height:844});
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();await page.getByText('多 0.0257',{exact:true}).waitFor();
  assert.ok(await page.locator('dialog').evaluate(el=>el.scrollWidth<=el.clientWidth+1));
  await page.keyboard.press('Escape');
  mock.disabled=true;await page.getByRole('button',{name:'双腿平仓',exact:true}).click();await page.getByText('模拟：本地服务未启用',{exact:true}).waitFor();
  assert.ok(await page.getByRole('button',{name:'确认平仓两腿',exact:true}).isDisabled());await page.getByRole('button',{name:'取消',exact:true}).click();mock.disabled=false;
  // Lost confirmation response keeps the same key, even after receipt expires.
  await page.setViewportSize({width:1440,height:960});mock.phase='unknown';mock.drop=true;
  await page.getByRole('button',{name:'双腿平仓',exact:true}).click();await page.getByText('多 0.0257',{exact:true}).waitFor();
  await page.getByRole('button',{name:'确认平仓两腿',exact:true}).click();await page.getByRole('button',{name:'核对本次确认结果',exact:true}).waitFor();
  await page.clock.install();await page.clock.fastForward(31_000);
  assert.ok(await page.getByRole('button',{name:'核对本次确认结果',exact:true}).isEnabled());
  await page.getByRole('button',{name:'核对本次确认结果',exact:true}).click();await page.getByText('订单结果待确认 · UNKNOWN',{exact:true}).waitFor();
  assert.equal(mock.executions,2,'Lost response retry reuses its key and cannot create a third exit');
  assert.equal(mock.confirms,3);
  assert.equal(await page.evaluate(()=>window.__overviewNode===document.querySelector('[data-testid=profit-overview]')),true);
  const relevant=errors.filter(x=>!x.includes('net::ERR_FAILED'));
  assert.deepEqual(relevant,[]);
  assert.equal(await page.locator('vite-error-overlay').count(),0);
  fs.writeFileSync(`${output}/browser-results.json`,JSON.stringify({pass:true,viewports:['1440×960','390×844','320×844'],mockExecutions:mock.executions,
    realControlRequests:0,checks:['identity','not blank','no overlay','console','screenshots','no navigation/remount','cancel/no execute','double click','partial','UNKNOWN','resume','pending/final accounting','expiry','disabled','lost response idempotency','gross/unknown fee basis','mobile/focus'],evidence},null,2));
  console.log(JSON.stringify({pass:true,evidence,mockExecutions:mock.executions,realControlRequests:0}));
} finally {await browser.close();}
