import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const supabaseUrl = (process.env['SUPABASE_URL'] ?? '').trim();
const supabaseAnonKey = (process.env['SUPABASE_ANON_KEY'] ?? process.env['SUPABASE_PUBLISHABLE_KEY'] ?? '').trim();
const requestedMode = process.env['SHOP_DEMO_MODE'];
const demoMode = requestedMode === undefined ? !(supabaseUrl || supabaseAnonKey) : requestedMode === 'true';

if (requestedMode !== undefined && !['true', 'false'].includes(requestedMode)) throw new Error('SHOP_DEMO_MODE must be true or false.');
if (!demoMode) {
  if (!/^https:\/\/[^/]+\.supabase\.co\/?$/.test(supabaseUrl) || !supabaseAnonKey) throw new Error('Live mode requires SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY (or the legacy SUPABASE_ANON_KEY).');
  if (!supabaseAnonKey.startsWith('sb_publishable_')) {
    let role;
    try { role = JSON.parse(Buffer.from(supabaseAnonKey.split('.')[1] ?? '', 'base64url').toString('utf8'))['role']; } catch { /* Validate below without printing the key. */ }
    if (role !== 'anon') throw new Error('Only a public publishable key or legacy anon key may be included. Never use service_role or sb_secret_ keys.');
  }
}
const config = { shopName: (process.env['SHOP_NAME'] ?? 'Madhu’s Crochet').trim().slice(0, 100), currency: 'INR', currencyLocale: 'en-IN', supabaseUrl: demoMode ? '' : supabaseUrl, supabaseAnonKey: demoMode ? '' : supabaseAnonKey, demoMode };
const path = resolve('public/shop-config.json');
await mkdir(dirname(path), { recursive: true });
await writeFile(path, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
console.log(`Shop configuration written: ${demoMode ? 'browser-only demo' : 'Supabase live mode'}. No private credentials are included.`);
