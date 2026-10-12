import { readFile } from 'node:fs/promises';
import { expect, test, type Page } from '@playwright/test';

const pageErrors = new WeakMap<Page, string[]>();

test.beforeEach(async ({ page }) => {
  const errors: string[] = [];
  pageErrors.set(page, errors);
  page.on('pageerror', error => errors.push(error.message));
  await page.clock.install({ time: new Date('2026-10-12T09:00:00Z') });
});

test.afterEach(async ({ page }) => {
  expect(pageErrors.get(page)).toEqual([]);
});

test('shows thirty available days and an accessible note editor', async ({ page }) => {
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Make room for what matters.' })).toBeVisible();
  const days = page.getByTestId('calendar-day');
  await expect(days).toHaveCount(20);
  await page.getByRole('button', { name: /Next month/i }).click();
  await expect(days).toHaveCount(10);
  await page.getByRole('button', { name: /Previous month/i }).click();
  await expect(days).toHaveCount(20);

  const selectedDay = days.nth(4);
  await expect(selectedDay).toHaveAttribute('aria-label', /\d{4}/);
  await selectedDay.focus();
  await page.keyboard.press('Enter');
  await expect(page.getByTestId('day-note')).toBeVisible();
  await expect(page.getByRole('button', { name: /Save note/ })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Mark complete', exact: true })).toBeVisible();
});

test('saves notes and completion across reloads, and allows undoing completion', async ({ page }) => {
  await page.goto('/');
  const day = page.getByTestId('calendar-day').nth(2);
  const dayLabel = await day.getAttribute('aria-label');
  expect(dayLabel).toBeTruthy();
  await day.click();

  await page.getByTestId('day-note').fill('Book a quiet hour to read.');
  await page.getByRole('button', { name: /Save note/ }).click();
  await page.getByRole('button', { name: 'Mark complete', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Mark incomplete', exact: true })).toBeVisible();

  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('next30.plan.v1') || '{}'));
  expect(Object.values(saved)).toContainEqual({ note: 'Book a quiet hour to read.', completed: true });

  await page.reload();
  await page.getByTestId('calendar-day').nth(2).click();
  await expect(page.getByTestId('day-note')).toHaveValue('Book a quiet hour to read.');
  await expect(page.getByRole('button', { name: 'Mark incomplete', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Mark incomplete', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Mark complete', exact: true })).toBeVisible();
});

test('switches views without losing a saved note and exports the plan', async ({ page }) => {
  await page.goto('/');
  await page.getByTestId('calendar-day').first().click();
  await page.getByTestId('day-note').fill('Take a walk, then write "three ideas".');
  await page.getByRole('button', { name: /Save note/ }).click();

  await page.getByRole('button', { name: 'List view', exact: true }).click();
  await expect(page.getByText('Take a walk, then write "three ideas".', { exact: true }).first()).toBeVisible();
  await page.getByRole('button', { name: 'Calendar view', exact: true }).click();
  await expect(page.getByTestId('calendar-day')).toHaveCount(20);
  await expect(page.getByTestId('day-note')).toHaveValue('Take a walk, then write "three ideas".');

  const downloadPromise = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Export plan', exact: true }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toMatch(/\.csv$/);
  const downloadPath = await download.path();
  expect(downloadPath).toBeTruthy();
  const csv = await readFile(downloadPath!, 'utf8');
  expect(csv.replace(/^\uFEFF/, '')).toMatch(/^Date,Note,Completed\r?\n/);
  expect(csv).toContain('Take a walk, then write ""three ideas"".');
});

test('recovers from corrupted saved data and stays within a mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('next30.plan.v1', '{invalid JSON'));
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Make room for what matters.' })).toBeVisible();
  await expect(page.getByTestId('calendar-day')).toHaveCount(20);
  const hasOverflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
  expect(hasOverflow).toBe(false);

  await page.getByTestId('calendar-day').last().click();
  await page.getByTestId('day-note').fill('A fresh start.');
  await page.getByRole('button', { name: /Save note/ }).click();
  await expect(page.getByTestId('day-note')).toHaveValue('A fresh start.');
});

test('refreshes the thirty-day window when the local date changes at midnight', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByTestId('calendar-day').first()).toHaveAttribute('aria-label', /Monday, October 12, 2026/);

  await page.clock.fastForward(15 * 60 * 60 * 1000 + 150);

  await expect(page.getByTestId('calendar-day').first()).toHaveAttribute('aria-label', /Tuesday, October 13, 2026/);
  await expect(page.getByTestId('calendar-day')).toHaveCount(19);
  await page.getByRole('button', { name: /Next month/i }).click();
  await expect(page.getByTestId('calendar-day')).toHaveCount(11);
});
