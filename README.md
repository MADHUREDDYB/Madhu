# next30

A personal planner for the next 30 days, built with Angular. Add plans to a day, mark them complete, and keep your schedule in your browser with `localStorage`.

## Run locally

Use Node.js 24.19.0 (see `.nvmrc`) or another version supported by the `engines` field in `package.json`.

```bash
npm ci
npm start
```

Open <http://localhost:4200>. The development server listens on `0.0.0.0`. No API server, account, credentials, or environment variables are required.

Stop the development server before reinstalling dependencies with `npm ci`, then restart it. The installer replaces `node_modules`, which invalidates a running server's dependency cache.

## Check the website

```bash
npm run test:unit
npx playwright install chromium
npm run test:e2e
npm run build
```

The browser tests start their own development server, or reuse one already running. They use `/usr/bin/chromium` when available; otherwise, install Playwright's Chromium with the command above. If Chromium system libraries are missing, install them using the supported Playwright command `npx playwright install --with-deps chromium`.

For the cloud environment, keep npm's cache inside the writable workspace: `npm ci --cache /workspace/.npm-cache`. Work in the existing checkout; each cloud task already has an isolated environment and needs no additional worktree.

## Static hosting

`npm run build` produces the static website in `dist/madhu/browser`. Cloudflare Pages or Netlify can host it with the build command `npm run build` and publish directory `dist/madhu/browser`.

The current website needs no database: plans stay in the current browser and do not synchronize between devices. Clearing site data removes saved plans. Future accounts and device synchronization could use Supabase, but would require new application code, a configured service, and credentials.

The application entry point is `src/main.ts`. Its root component, template, and styles are `main.component.ts`, `main.html`, and `main.component.css`; global styles live in `src/styles.css`.
