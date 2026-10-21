import assert from "node:assert/strict";
import test from "node:test";
import worker from "./index.mjs";

const env = { ASSETS: { fetch: async () => new Response("not found", { status: 404 }) } };

test("V3 worker refuses writes and non-allowlisted paths", async () => {
  assert.equal((await worker.fetch(new Request("https://example.test/api/v3/overview", {
    method: "POST",
  }), env)).status, 405);
  assert.equal((await worker.fetch(new Request("https://example.test/api/private"), env)).status, 404);
});

test("V3 worker fails closed without visitor access protection", async () => {
  const response = await worker.fetch(new Request("https://example.test/api/v3/overview"), env);
  assert.equal(response.status, 503);
  assert.match(await response.text(), /访问保护/);
});
