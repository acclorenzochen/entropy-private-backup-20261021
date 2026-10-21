import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");
const base = process.env.DASHBOARD_URL || "http://127.0.0.1:5174/";
const browser = await chromium.launch({ headless: true });
const evidence = [];

try {
  for (const [width, height] of [[1440, 900], [1280, 800], [390, 844]]) {
    const context = await browser.newContext({ viewport: { width, height },
      permissions: ["clipboard-read", "clipboard-write"] });
    const page = await context.newPage();
    const errors = [];
    const apiErrors = [];
    page.on("pageerror", error => errors.push(error.message));
    page.on("response", response => {
      if (response.url().includes("/api/v3/") && response.status() >= 400) {
        apiErrors.push(`${response.status()} ${response.url()}`);
      }
    });
    const response = await page.goto(base, { waitUntil: "networkidle" });
    assert.equal(response?.status(), 200);
    await page.getByRole("heading", { name: "运行总览", exact: true }).waitFor();
    await page.getByText("40 campaigns", { exact: false }).waitFor();
    const widthState = await page.evaluate(() => ({ viewport: innerWidth,
      scroll: document.documentElement.scrollWidth }));
    assert.ok(widthState.scroll <= widthState.viewport + 1,
      `horizontal overflow at ${width}: ${JSON.stringify(widthState)}`);
    const shot = `/tmp/entropy-v3-real-${width}.png`;
    await page.screenshot({ path: shot, fullPage: true });
    evidence.push(shot);
    assert.deepEqual(errors, [], `page errors at ${width}`);
    assert.deepEqual(apiErrors, [], `V3 API errors at ${width}`);

    if (width === 1440) {
      await page.getByRole("link", { name: /交易与执行/ }).click();
      await page.getByRole("heading", { name: "交易与执行" }).waitFor();
      await page.locator(".v3-link-button").first().click();
      await page.locator(".v3-drawer").waitFor();
      await page.locator(".v3-group-list button").first().click();
      await page.getByText("order_ref / 来源").waitFor();
      await page.getByRole("button", { name: "复制只读诊断" }).click();
      await page.getByText("已复制只读诊断", { exact: false }).waitFor();
      const copied = await page.evaluate(() => navigator.clipboard.readText());
      const diagnostic = JSON.parse(copied);
      assert.ok(diagnostic.snapshot_id);
      assert.equal(diagnostic.records.length, 1);
      const detail = "/tmp/entropy-v3-real-cycle.png";
      await page.screenshot({ path: detail, fullPage: true });
      evidence.push(detail);
      await page.getByRole("button", { name: "关闭 cycle 详情" }).click();
      await page.getByRole("link", { name: /价差与机会/ }).click();
      await page.getByRole("heading", { name: "价差与机会" }).waitFor();
      await page.locator('.v3-chart svg[role="application"]').waitFor({ timeout: 20_000 });
      await page.getByText("当前固定 q").waitFor();
      const market = "/tmp/entropy-v3-real-market.png";
      await page.screenshot({ path: market, fullPage: true });
      evidence.push(market);
      await page.getByRole("link", { name: /系统与配置/ }).click();
      await page.getByRole("heading", { name: "系统与配置" }).waitFor();
      await page.getByRole("link", { name: /收益与研究/ }).click();
      await page.getByRole("heading", { name: "收益与研究" }).waitFor();
      await page.getByText("INSUFFICIENT_DATA", { exact: false }).first().waitFor();
      assert.deepEqual(errors, [], "page errors in navigation flow");
      assert.deepEqual(apiErrors, [], "V3 API errors in navigation flow");
    }
    await context.close();
  }
  process.stdout.write(JSON.stringify({ status: "passed", evidence }) + "\n");
} finally {
  await browser.close();
}
