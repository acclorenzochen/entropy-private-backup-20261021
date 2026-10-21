import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { join } from "node:path";

const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");
const base = process.env.DASHBOARD_URL || "http://127.0.0.1:8768/";
const evidenceDir = process.env.QA_EVIDENCE_DIR;
assert.ok(evidenceDir, "Set QA_EVIDENCE_DIR to a temporary directory outside the repository");
const browser = await chromium.launch({ headless: true });
const evidence = [];
const errors = [];
const checkPage = async page => {
  await page.getByRole("heading", { name: "NBIS 资金费对冲", exact: true }).waitFor();
  assert.match(await page.title(), /Entropy V3/);
  assert.match(page.url(), new RegExp(new URL(base).host.replaceAll(".", "\\.")));
  assert.equal(await page.locator("vite-error-overlay").count(), 0);
  const width = await page.evaluate(() => ({ viewport: innerWidth, scroll: document.documentElement.scrollWidth }));
  assert.ok(width.scroll <= width.viewport + 1, `horizontal overflow: ${JSON.stringify(width)}`);
};
const watch = page => {
  page.on("pageerror", error => errors.push(error.message));
  page.on("console", message => { if (["error", "warning"].includes(message.type())) errors.push(message.text()); });
  page.on("request", request => assert.equal(request.method(), "GET", "Dashboard must remain read-only"));
};
try {
  for (const [width, height] of [[1440, 1000], [390, 844]]) {
    const page = await browser.newPage({ viewport: { width, height } });
    watch(page);
    await page.goto(base, { waitUntil: "networkidle" });
    await checkPage(page);
    await page.getByRole("heading", { name: "双腿仓位与强平风险" }).waitFor();
    const shot = join(evidenceDir, `nbis-live-${width}.png`);
    await page.screenshot({ path: shot });
    evidence.push(shot);
    await page.getByText("查看实际结算记录与公共预检", { exact: true }).click();
    await page.getByText("公共预检通过 · 不等于实盘就绪", { exact: false }).waitFor();
    await page.getByRole("button", { name: "暂停页面刷新", exact: true }).click();
    await page.getByRole("button", { name: "恢复页面刷新", exact: true }).waitFor();
    await page.getByText("页面刷新已暂停 · 最后已知状态", { exact: true }).waitFor();
    await page.getByRole("button", { name: "恢复页面刷新", exact: true }).click();
    if (width === 1440) {
      await page.getByRole("combobox", { name: "时间范围", exact: true }).selectOption("batch");
      await page.getByRole("heading", { name: "NBIS 资金费对冲", exact: true }).waitFor();
      await page.locator('[data-testid="funding-panel"]').waitFor();
      await page.getByRole("combobox", { name: "时间范围", exact: true }).selectOption("session");
      await page.locator('[data-testid="funding-panel"]').waitFor();
      await page.getByRole("link", { name: /查看全部交易/ }).click();
      await page.getByRole("columnheader", { name: "实际结算 / 资金费" }).waitFor();
      await page.getByRole("link", { name: "查看交易", exact: true }).first().click();
      await page.getByRole("heading", { name: "本轮资金费结算", exact: true }).waitFor();
      await page.getByRole("button", { name: "关闭交易详情", exact: true }).click();
    }
    await page.close();
  }

  const legacy = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  watch(legacy);
  await legacy.goto(process.env.LEGACY_DASHBOARD_URL || "http://127.0.0.1:8767/", { waitUntil: "networkidle" });
  await legacy.getByRole("heading", { name: "现在", exact: true }).waitFor();
  assert.equal(await legacy.locator('[data-testid="funding-panel"]').count(), 0);
  await legacy.close();

  // Synthetic edge cases live only in this browser route, never in production files.
  const response = await fetch(new URL("api/v3/overview", base));
  assert.equal(response.status, 200);
  const source = await response.json();
  for (const scenario of ["preflight-only", "stale", "early-exit", "zero-rate"]) {
    const fixture = structuredClone(source);
    fixture.meta.snapshot_id = `browser-fixture-${scenario}`;
    const data = fixture.data;
    const funding = data.funding;
    if (scenario === "preflight-only") {
      data.live = null;
      data.active_cycle = null;
      data.recent_cycles = [];
      funding.state = funding.cycle = null;
      funding.live_quality = "unavailable";
      funding.net_usd_per_hour = null;
    } else if (scenario === "stale") {
      funding.live_quality = "stale";
      funding.net_usd_per_hour = null;
    } else if (scenario === "early-exit") {
      data.live.current_state = "EXITING";
      funding.state.exit_reason = "funding_reversal_cost";
      funding.state.completed = false;
      funding.state.negative_sample_count = 3;
      funding.expected_remaining_cost_usd = "0.51";
    } else {
      funding.net_usd_per_hour = "0";
      funding.rate_quality = "available";
    }
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    watch(page);
    await page.route("**/api/v3/overview?*", route => route.fulfill({ json: fixture }));
    await page.goto(base, { waitUntil: "networkidle" });
    await checkPage(page);
    if (scenario === "preflight-only") {
      await page.getByText("尚无实盘状态", { exact: true }).waitFor();
      const progress = await page.locator('[data-testid="funding-panel"] .v3-metric strong').first().innerText();
      assert.match(progress, /—/);
    } else if (scenario === "stale") {
      await page.getByText("实盘状态已过期 · 仅供历史核对", { exact: true }).waitFor();
    } else if (scenario === "early-exit") {
      await page.getByText("预计剩余净资金费支出超限", { exact: true }).first().waitFor();
      await page.getByText("两周期未完成", { exact: true }).waitFor();
      await page.getByText("0.51 USD", { exact: true }).waitFor();
    } else {
      await page.getByText("0 USD/h", { exact: true }).waitFor();
    }
    await page.close();
  }
  assert.deepEqual(errors, [], "Relevant console warnings/errors or runtime exceptions");
  process.stdout.write(JSON.stringify({ status: "passed", viewports: [1440, 390],
    scenarios: ["live", "preflight-only", "stale", "early-exit", "zero-rate"], evidence }) + "\n");
} finally {
  await browser.close();
}
