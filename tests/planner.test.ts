import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildDays,
  buildMonthCells,
  dateKey,
  sanitizeSavedPlan,
  serializePlanCsv,
} from '../planner.ts';

function readCsv(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let quoted = false;
  const input = csv.replace(/^\uFEFF/, '');
  for (let index = 0; index < input.length; index += 1) {
    const character = input[index];
    if (character === '"') {
      if (quoted && input[index + 1] === '"') {
        field += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
    } else if (character === ',' && !quoted) {
      row.push(field);
      field = '';
    } else if ((character === '\n' || character === '\r') && !quoted) {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      if (character === '\r' && input[index + 1] === '\n') index += 1;
    } else {
      field += character;
    }
  }
  assert.equal(quoted, false, 'CSV fields must have balanced quotes');
  if (row.length || field) rows.push([...row, field]);
  return rows;
}

test('uses local date keys and handles year and leap-year boundaries', () => {
  assert.equal(dateKey(new Date(2026, 0, 1, 23, 59)), '2026-01-01');
  assert.equal(dateKey(new Date(2024, 1, 29)), '2024-02-29');

  const start = new Date(2026, 11, 25, 15, 30);
  const days = buildDays(start);
  assert.equal(days.length, 30);
  assert.equal(days[0].key, '2026-12-25');
  assert.equal(days[29].key, '2027-01-23');
  assert.equal(new Set(days.map(day => day.key)).size, 30);
  assert.deepEqual(days.map(day => day.offset), Array.from({ length: 30 }, (_, index) => index));
  assert.equal(start.getHours(), 15, 'building a plan must not mutate its start date');
  assert.equal(start.getDate(), 25);

  const leapDays = buildDays(new Date(2024, 1, 28), 3);
  assert.deepEqual(leapDays.map(day => day.key), ['2024-02-28', '2024-02-29', '2024-03-01']);
  const daylightSavingDays = buildDays(new Date(2026, 9, 31), 4);
  assert.deepEqual(daylightSavingDays.map(day => day.key), ['2026-10-31', '2026-11-01', '2026-11-02', '2026-11-03']);
  assert.deepEqual(buildDays(start, 0), []);
});

test('identifies today using the local calendar date', () => {
  const days = buildDays(new Date(), 2);
  assert.equal(days[0].isToday, true);
  assert.equal(days[1].isToday, false);
});

test('lays out complete Monday-first weeks across month boundaries', () => {
  const plan = buildDays(new Date(2026, 10, 25));
  const november = buildMonthCells(new Date(2026, 10, 1), plan);
  assert.equal(november.length, 42);
  assert.equal(november[0].key, '2026-10-26');
  assert.equal(november.at(-1)?.key, '2026-12-06');
  assert.equal(november[0].date.getDay(), 1);
  assert.equal(november.at(-1)?.date.getDay(), 0);
  assert.equal(november.filter(cell => cell.inMonth).length, 30);
  assert.equal(new Set(november.map(cell => cell.key)).size, november.length);
  assert.equal(november.find(cell => cell.key === '2026-11-25')?.day?.key, '2026-11-25');
  assert.equal(november.find(cell => cell.key === '2026-11-24')?.day, null);

  const december = buildMonthCells(new Date(2026, 11, 1), plan);
  assert.equal(december.length, 35);
  assert.equal(december[0].key, '2026-11-30');
  assert.equal(december.at(-1)?.key, '2027-01-03');
  assert.equal(december.filter(cell => cell.inMonth).length, 31);
  assert.equal(december.find(cell => cell.key === '2026-12-24')?.day?.key, '2026-12-24');
  assert.equal(december.find(cell => cell.key === '2026-12-25')?.day, null);
});

test('sanitizes untrusted saved plans without accepting invalid calendar dates', () => {
  const saved = sanitizeSavedPlan(JSON.parse(JSON.stringify({
    '2026-10-12': { note: 'Call home.', completed: true },
    '2026-10-13': { note: 'Read.', completed: 'true' },
    '2026-10-14': { note: 'a'.repeat(2_500), completed: false },
    '2026-02-29': { note: 'Invalid leap day', completed: true },
    '2026-13-01': { note: 'Invalid month', completed: false },
    '2026-10-32': { note: 'Invalid day', completed: false },
    'not-a-date': { note: 'Bad key', completed: true },
  })));

  assert.deepEqual(saved['2026-10-12'], { note: 'Call home.', completed: true });
  assert.equal(saved['2026-10-13']?.completed, false);
  assert.equal(saved['2026-10-14']?.note.length, 2_000);
  assert.equal(Object.hasOwn(saved, '2026-02-29'), false);
  assert.equal(Object.hasOwn(saved, '2026-13-01'), false);
  assert.equal(Object.hasOwn(saved, '2026-10-32'), false);
  assert.equal(Object.hasOwn(saved, 'not-a-date'), false);

  for (const invalid of [null, undefined, true, 42, 'text', []]) {
    assert.deepEqual(sanitizeSavedPlan(invalid), {});
  }
  const malicious = sanitizeSavedPlan(JSON.parse('{"__proto__":{"note":"pollution","completed":true}}'));
  assert.equal(Object.hasOwn(malicious, '__proto__'), false);
  assert.equal(({} as { completed?: boolean }).completed, undefined);
});

test('exports every planned day and preserves CSV note punctuation and newlines', () => {
  const days = buildDays(new Date(2026, 9, 12), 3);
  const note = 'Take a walk, then write "three ideas".\nMake time for a friend.';
  const rows = readCsv(serializePlanCsv(days, {
    '2026-10-12': { note, completed: true },
  }));

  assert.deepEqual(rows[0], ['Date', 'Note', 'Completed']);
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.slice(1).map(row => row[0]), ['2026-10-12', '2026-10-13', '2026-10-14']);
  assert.equal(rows[1][1], note);
  assert.equal(rows[2][1], '');
  assert.equal(rows[1][2], 'Yes');
  assert.equal(rows[2][2], 'No');
  assert.ok(rows.every(row => row.length === 3));
});

test('makes spreadsheet formulas inert when exporting notes', () => {
  const notes = ['=SUM(1,2)', '+1+2', '-1+2', '@SUM(A1:A2)', '\t=1+1', '\r=1+1'];
  const days = buildDays(new Date(2026, 9, 12), notes.length);
  const entries = Object.fromEntries(days.map((day, index) => [day.key, { note: notes[index], completed: false }]));
  const rows = readCsv(serializePlanCsv(days, entries)).slice(1);

  rows.forEach((row, index) => {
    assert.equal(row[1], `'${notes[index]}`, 'prefix formula-like values before CSV escaping');
  });
});
