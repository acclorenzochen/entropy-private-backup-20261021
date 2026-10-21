import { build } from 'vite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { join } from 'node:path';
const root = fileURLToPath(new URL('../', import.meta.url));
const outDir = await mkdtemp(join(tmpdir(), 'entropy-market-check-'));
try {
  await build({ configFile:false, root, logLevel:'error', build:{
    lib:{entry:join(root,'scripts/check-market-series.ts'),formats:['es'],fileName:'check-market-series'},
    outDir, emptyOutDir:true,
  }});
  await import(pathToFileURL(join(outDir,'check-market-series.js')).href);
} finally { await rm(outDir, { recursive:true, force:true }); }
