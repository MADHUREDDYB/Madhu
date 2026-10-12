# Crochet shop

A responsive Angular storefront and protected admin workspace for selling handmade crochet pieces. Prices use INR. The shop name is configurable and currently uses “Madhu’s Crochet” as a placeholder.

## View the website

Visitors need only a browser. Do not deploy or open `main.html` or `src/app/storefront/storefront.component.html`: these are Angular source templates and contain instructions such as `@if`.

To run the project locally as a developer, use Node.js 24.19.0 and run:

```bash
npm ci
npm start
```

Open the address printed by the development server. For hosting, build the application and deploy the **contents of `dist/madhu/browser`**. The compiled `index.html` must be the site entry point. Opening source templates or serving the repository root will not render the application.

This cloud onboarding interface does not provide a supported public web preview. The prepared development server is useful for internal browser validation; a static host supplies a public website address.

## Storefront

- Product photos, descriptions, categories, search, sorting, availability and INR prices with percentage discounts.
- Product detail dialogs, persistent cart, quantity controls, stock checks and checkout validation.
- Guest checkout collects full name, Indian mobile number, address, city and PIN; email and order notes are optional.
- Payments are arranged with the seller. Orders start with payment pending, and the owner records received payments manually. There is no online payment gateway or automatic payment verification.
- No delivery fee is added by the current checkout. Set the desired shipping policy before launching if you need to charge separately.

## Admin

Open the `/admin` route. In live mode it requires email/password sign-in with a Supabase account explicitly granted shop-admin membership.

The dashboard manages product images, original price, discount percentage, stock, category and storefront visibility. Orders show private customer contact/delivery details, item snapshots and totals. Statuses are New, Confirmed, Making, Shipped, Delivered and Cancelled. Payment is Pending or Received. The owner can search/filter loaded orders and load older pages of 100 orders.

Cancellation does not automatically return stock or process a refund. Adjust stock deliberately when appropriate.

## Demo and Supabase setup

With no backend environment configuration, `npm start` and `npm run build` generate a clearly labelled browser-only demo. Demo admin is available only in demo mode. Sample products/photos and fictional orders are for demonstration; replace them with your actual crochet products before selling. Use invented contact details when testing demo checkout. Demo orders stay in the same browser and are not real sales.

For a live shop, follow [Supabase setup](supabase/README.md): create a project, apply the supplied SQL migration, create your admin account, grant its membership, and configure:

```text
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_PUBLISHABLE_KEY=your-public-publishable-key
SHOP_DEMO_MODE=false
SHOP_NAME=your-final-shop-name
```

A legacy public `SUPABASE_ANON_KEY` also works. Never include a service-role key, private secret key, admin password or database password in frontend configuration. `scripts/write-shop-config.mjs` validates and writes public runtime config before start/build. Partially configured live builds fail rather than silently using sample data. A missing or failed config request shows an error and retry.

Live customer orders are stored in Supabase PostgreSQL with row-level security. Public customers can read available products and submit validated orders, but cannot read private orders or grant themselves admin access. The checkout transaction calculates current prices on the server, snapshots product details, prevents duplicate retries, and locks stock against concurrent purchases. Product images use Supabase Storage; uploads are restricted to admins.

No live Supabase project has been connected or deployed from this workspace yet. Local browser and isolated PostgreSQL tests do not establish live-project readiness.

## Low-latency choices

Product photos use locally bundled, compressed WebP; live uploads resize to 1,600 pixels and at most 400 KB. Images reserve layout space and load lazily. Admin code and the Supabase SDK load separately, instead of adding their full payload to storefront startup. State uses Angular signals, order lists paginate, submissions disable duplicate clicks, and network operations time out after 15 seconds with retry handling. Actual latency depends on hosting, the Supabase region, network conditions and catalog size.

## Validation

```bash
npm run test:unit
npm run test:e2e
bash supabase/tests/run-security-tests.sh
npm run build
```

Playwright uses `/usr/bin/chromium` when present; otherwise install its browser using `npx playwright install chromium`. Browser tests start/reuse the dev server. Their API integration test mocks Supabase; they do not connect to a live project.

The backend regression script needs Docker. It runs the actual migration against an isolated PostgreSQL container with minimal Supabase auth/storage schemas, checking access policies, server pricing, stock rollback, idempotent requests and concurrent checkout. It does not contact a live database.

## Free starting hosting

Cloudflare Pages or Netlify can host the compiled application. Configure build command `npm run build`, publish directory `dist/madhu/browser`, Node.js 24.19.0 and the backend public build variables above. `_redirects` serves Angular routes such as `/admin`, and `_headers` supplies caching and browser security headers.

A ready-to-upload demo build is packaged outside the checkout at `/workspace/crochet-shop-website.zip`. Uploading it to a static host produces an interactive public demo; it will not receive real customer orders until you build with your live Supabase configuration. Supabase’s free plan is a starting option; review its current quotas/inactivity policy before launch.

Stop the development server before `npm ci`, because reinstalling replaces its dependency files. In this cloud environment use `npm ci --cache /workspace/.npm-cache`. Work in the existing checkout; tasks are already isolated and do not need another worktree.
