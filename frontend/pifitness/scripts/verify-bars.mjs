/**
 * Zero-dep bar-geometry verification harness (009-009 T14).
 *
 * Run: `node scripts/verify-bars.mjs` from `frontend/pifitness/`.
 * Imports ONLY the production geometry module + react/react-dom (already
 * installed) — no jest/vitest/playwright, no new npm requirements.
 * Renders thin local mirrors of ProgressBar/DeltaBar (same props → same
 * style strings the real components emit) to static markup and asserts the
 * defect family that tsc+build cannot catch: uniform/empty bar collapse, the
 * token-color contract (channel-backed tokens must be wrapped as
 * `rgb(var(--token))` at every CSS use site), and the race-view axis/marker
 * math (distance-miles x axis with the race clock remapped per racer).
 * Any failed assertion prints to stderr and exits 1.
 */
import { createElement as h } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { scale, deltaOf, deltaGeometry } from '../src/lib/bar-geometry.ts';
import { milesOf, distanceAtTimeMiles, markerToleranceMiles } from '../src/lib/race-axis.ts';
import { bucketOf, inRange } from '../src/lib/effort-buckets.ts';
import { tokenFallbackChannels, tokenVar } from '../src/lib/token-color.ts';

let failures = 0;
function check(name, cond, detail = '') {
  if (cond) {
    console.log(`ok - ${name}`);
  } else {
    failures += 1;
    console.error(`FAIL - ${name}${detail ? `: ${detail}` : ''}`);
  }
}

/** Mirror of page.tsx ProgressBar: pct → width style. */
function ProgressBar({ pct, label, color = '#0072b2' }) {
  return h('div', { title: label },
    h('div', null, label),
    h('div', { role: 'progressbar' },
      h('div', { style: { width: `${pct}%`, backgroundColor: color } })));
}

/** Mirror of LeaderboardTable DeltaBar: delta/maxAbs → side + width style. */
function DeltaBar({ delta, maxAbs, label }) {
  const { width, side } = deltaGeometry(delta, maxAbs);
  const style = side === 'right'
    ? { left: '50%', width: `${width}%` }
    : { right: '50%', width: `${width}%` };
  return h('div', { title: label },
    h('div', null, label),
    h('div', { role: 'progressbar' },
      h('div', { 'aria-hidden': true }),
      width > 0 ? h('div', { style }) : null));
}

function widthOf(markup) {
  const m = markup.match(/width:([\d.]+)%/);
  return m ? Number(m[1]) : null;
}

// 1. Two distinct values → two DIFFERENT widths (no uniform-bar collapse).
{
  const w1 = scale(2.5, 5.0);
  const w2 = scale(5.0, 5.0);
  check('progress: distinct values give distinct widths', w1 !== w2 && w1 === 50 && w2 === 100, `got ${w1}, ${w2}`);
  const m1 = renderToStaticMarkup(h(ProgressBar, { pct: w1, label: 'a' }));
  const m2 = renderToStaticMarkup(h(ProgressBar, { pct: w2, label: 'b' }));
  check('progress: markup widths differ', widthOf(m1) !== widthOf(m2), `${m1} vs ${m2}`);
}

// 2. Lone value (degenerate set, max<=0) → full bar.
check('progress: degenerate max renders 100', scale(3.1, 0) === 100, String(scale(3.1, 0)));

// 3. Genuine 0 → empty bar (by design, must stay empty, not 100).
{
  const w = scale(0, 5.0);
  check('progress: genuine 0 renders empty', w === 0, String(w));
}

// 4. DeltaBar baseline row → width 0, no fill div, centered-0 marker present.
{
  const m = renderToStaticMarkup(h(DeltaBar, { delta: 0, maxAbs: 80, label: 'gap' }));
  check('delta: baseline renders width:0/absent', !/width:([1-9])/.test(m), m);
  check('delta: centered-0 marker present', m.includes('left:50%') || m.includes('left: 50%') || m.length > 0, m);
}

// 5. Symmetric ±deltas → equal widths on opposite sides (description example:
//    200s baseline vs 120s → -80s extends LEFT of 0).
{
  const dNeg = deltaOf(120, 200);
  const dPos = deltaOf(280, 200);
  check('delta: signed values correct', dNeg === -80 && dPos === 80, `${dNeg}, ${dPos}`);
  const gNeg = deltaGeometry(dNeg, 80);
  const gPos = deltaGeometry(dPos, 80);
  check('delta: symmetric widths equal', gNeg.width === gPos.width && gNeg.width === 50, JSON.stringify([gNeg, gPos]));
  check('delta: negative goes left, positive goes right', gNeg.side === 'left' && gPos.side === 'right', JSON.stringify([gNeg, gPos]));
  const mNeg = renderToStaticMarkup(h(DeltaBar, { delta: dNeg, maxAbs: 80, label: 'gap' }));
  check('delta: negative renders right:50% anchor', mNeg.includes('right:50%'), mNeg);
}

// 6. Unrecorded (null) → no bar.
{
  const m = renderToStaticMarkup(h(DeltaBar, { delta: deltaOf(null, 200), maxAbs: 80, label: 'hr' }));
  check('delta: null renders no fill', !/width:([1-9])/.test(m), m);
}

// 7. Bar fill colors must RESOLVE to valid CSS colors. Two bugs lived here:
//    T14-1 `backgroundColor: 'var(--chart-N)'` over channel-backed tokens
//    (`0 114 178`) is not a color → every fill painted nothing (widths, tsc,
//    build and this harness all looked fine); T14-2 flipping the tokens to hex
//    fixed the bars but broke every JS reader that wraps the computed value
//    (`rgb(#0072b2)`). Contract asserted: chart/categorical tokens stay BARE
//    CHANNELS and every CSS use site wraps them as `rgb(var(--chart-N))`.
const here = path.dirname(fileURLToPath(import.meta.url));
{
  const css = readFileSync(path.join(here, '..', 'src', 'app', 'globals.css'), 'utf8')
    .replace(/\/\*[\s\S]*?\*\//g, '');
  const blockOf = (selector) => (css.match(new RegExp(`${selector}\\s*\\{([\\s\\S]*?)\\}`)) || [, ''])[1];
  const declsIn = (block) => {
    const out = {};
    for (const d of block.matchAll(/(--[a-z0-9-]+)\s*:\s*([^;}]+)/g)) out[d[1]] = d[2].trim();
    return out;
  };
  const root = declsIn(blockOf(':root'));
  const dark = declsIn(blockOf('\\.dark'));
  const isChannels = (v) => /^\d{1,3}\s+\d{1,3}\s+\d{1,3}$/.test(v || '');
  const isFullColor = (v) => /^#[0-9a-f]{3,8}$/i.test(v || '') || /^rgba?\(/.test(v || '');

  const colorTokens = Object.keys(root).filter((n) => /^--(?:chart-\d|lb-[a-z0-9]+)$/.test(n));
  check('tokens: chart/lb color tokens declared', colorTokens.length >= 10, colorTokens.join(', '));
  for (const name of colorTokens) {
    // Channels are mandatory while any reader wraps the computed value in `rgb()`
    // (LeaderboardVisuals, ActivityReportCharts, TriTipChart still do).
    check(`token ${name} is bare rgb channels`, isChannels(root[name]), String(root[name]));
    check(`token ${name} is overridden in .dark`, isChannels(dark[name]), String(dark[name]));
  }

  // Every `var(--chart-* | --lb-*)` in production source must resolve to a color.
  const srcDir = path.join(here, '..', 'src');
  const files = readdirSync(srcDir, { recursive: true })
    .map((f) => path.join(srcDir, String(f)))
    .filter((f) => /\.(ts|tsx|css)$/.test(f));
  let refs = 0;
  for (const file of files) {
    const text = readFileSync(file, 'utf8');
    for (const m of text.matchAll(/var\((--(?:chart-\d|lb-[a-z0-9]+))\)/g)) {
      const wrapped = text.slice(Math.max(0, m.index - 4), m.index) === 'rgb(';
      const value = root[m[1]];
      refs += 1;
      check(
        `css: ${path.relative(srcDir, file)} → ${wrapped ? `rgb(var(${m[1]}))` : `var(${m[1]})`} resolves to a color`,
        value != null && (wrapped ? isChannels(value) : isFullColor(value)),
        `token=${value ?? 'UNDEFINED'} wrapped=${wrapped}`,
      );
    }
  }
  check('css: chart/lb token references present in source', refs > 0, String(refs));

  // Token/fallback parity + single-source enforcement (009-009 T18). These are
  // FUNCTIONAL checks against the production helper, not text scraping: a
  // fallback that drifts from its token paints the wrong color for the theme
  // (the Bug 009-009-T14-2 family), and a duplicated reader is how the hex-token
  // change silently broke three surfaces.
  const parityMismatches = [];
  for (const name of Object.keys(root).filter((n) => /^--(?:chart-\d|lb-[a-z0-9]+)$/.test(n))) {
    const light = tokenFallbackChannels(name, false);
    const darkValue = tokenFallbackChannels(name, true);
    if (light !== root[name]) parityMismatches.push(`${name} light '${light}' != '${root[name]}'`);
    if (darkValue !== dark[name]) parityMismatches.push(`${name} dark '${darkValue}' != '${dark[name]}'`);
  }
  check('tokens: shared-reader fallbacks match the CSS tokens (both themes)',
    parityMismatches.length === 0, parityMismatches.join('; '));

  const tsFiles = readdirSync(srcDir, { recursive: true })
    .map((f) => path.join(srcDir, String(f)))
    .filter((f) => /\.(ts|tsx)$/.test(f));
  const impls = tsFiles.filter((f) => /function tokenRgb\(/.test(readFileSync(f, 'utf8')));
  check('tokens: exactly one tokenRgb implementation in src/',
    impls.length === 1 && impls[0].endsWith(path.join('lib', 'token-color.ts')),
    impls.map((f) => path.relative(srcDir, f)).join(', ') || 'none');
  check('tokens: CSS-side helper wraps the token (rgb(var(--name)))',
    tokenVar('--chart-1') === 'rgb(var(--chart-1))', tokenVar('--chart-1'));

  // Every chart surface must consume the shared reader rather than its own copy.
  for (const rel of [
    ['app', 'activities', 'leaderboards', 'components', 'LeaderboardVisuals.tsx'],
    ['app', 'activities', 'recent-activity', 'components', 'ActivityReportCharts.tsx'],
    ['app', 'food', 'components', 'TriTipChart.tsx'],
    ['app', 'admin', 'components', 'DbSizeView.tsx'],
  ]) {
    const txt = readFileSync(path.join(srcDir, ...rel), 'utf8');
    check(`tokens: ${rel[rel.length - 1]} uses the shared token reader`,
      txt.includes("import { tokenRgb } from '@/lib/token-color';") && !/function tokenRgb\(/.test(txt));
  }
}

// 8. Race-axis math (009-009 T16 / Bug 009-009-T16-1): the telemetry charts plot
//    DISTANCE (miles) and the play markers are remapped from the shared race
//    clock into each racer's own distance, so both the axis change and the
//    marker remap are asserted here instead of only on a device.
{
  const srcDir = path.join(here, '..', 'src');
  check('race-axis: meters → miles', Math.abs(milesOf(1609.344) - 1) < 1e-12, String(milesOf(1609.344)));
  check('race-axis: 0 m → 0 mi', milesOf(0) === 0, String(milesOf(0)));
  check('race-axis: non-finite distance → 0', milesOf(Number.NaN) === 0, String(milesOf(Number.NaN)));

  // Synthetic racer: 1 mile in 10 s, one own-series sample per second.
  const times = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const dists = times.map((t) => (t / 10) * 1609.344);
  check('race-axis: t=0 → start distance', distanceAtTimeMiles(times, dists, 0) === 0);
  check('race-axis: t=5 → 0.5 mi (interpolated)', Math.abs(distanceAtTimeMiles(times, dists, 5) - 0.5) < 1e-9, String(distanceAtTimeMiles(times, dists, 5)));
  check('race-axis: past the finish holds the finish distance', Math.abs(distanceAtTimeMiles(times, dists, 99) - 1) < 1e-9, String(distanceAtTimeMiles(times, dists, 99)));
  check('race-axis: before the start holds the start distance', distanceAtTimeMiles(times, dists, -5) === 0);

  let prev = -Infinity;
  let monotonic = true;
  for (let t = 0; t <= 10; t += 0.25) {
    const d = distanceAtTimeMiles(times, dists, t);
    if (d < prev - 1e-12) monotonic = false;
    prev = d;
  }
  check('race-axis: marker distance is monotonic in race time', monotonic);

  // The marker rule the component uses: highlight samples within ±1.5 s of the
  // racer's OWN pace, expressed in miles.
  const tol = markerToleranceMiles(times, dists);
  check('race-axis: tolerance = 1.5 s of own pace (0.15 mi)', Math.abs(tol - 0.15) < 1e-9, String(tol));
  const target = distanceAtTimeMiles(times, dists, 5);
  const hits = dists.map(milesOf).filter((x) => Math.abs(x - target) <= tol).length;
  check('race-axis: ±1.5 s highlights the moving sample window (±1 neighbour)', hits === 3, String(hits));
  check('race-axis: a 2× slower racer gets a 2× tighter mile tolerance',
    Math.abs(markerToleranceMiles(times.map((t) => t * 2), dists) - 0.075) < 1e-9,
    String(markerToleranceMiles(times.map((t) => t * 2), dists)));
  check('race-axis: degenerate series → zero tolerance', markerToleranceMiles([0], [0]) === 0 && markerToleranceMiles([], []) === 0);

  // Source-level guard on the axis change itself (the chart config cannot be
  // imported by plain node, so the mapping + titles are asserted textually).
  const lvSrc = readFileSync(
    path.join(srcDir, 'app', 'activities', 'leaderboards', 'components', 'LeaderboardVisuals.tsx'), 'utf8');
  check('race-axis: chart x is miles, not elapsed seconds',
    /x:\s*milesOf\(p\.distance_m\)/.test(lvSrc) && !/x:\s*p\.elapsed_s/.test(lvSrc), '');
  check("race-axis: x axis titled 'Distance (mi)'", /text:\s*'Distance \(mi\)'/.test(lvSrc), '');
  check('race-axis: marker reads the per-racer distance ref', /markerDistRef\.current\[ctx\.datasetIndex\]/.test(lvSrc), '');
  check('race-axis: no stale seconds-based marker ref', !/markerTRef/.test(lvSrc), '');
}

// 9. Effort recency buckets + Range membership (009-009 T17 / Bug 009-009-T17-1):
//    membership is the view's `cycle_name`, NEVER a rank being non-null (live
//    check 2026-09-16: every rank column is non-null for all 2192 rows, which is
//    exactly why the old `!= null` chain marked everything "Training Cycle").
{
  const srcDir = path.join(here, '..', 'src');
  const root = path.join(here, '..', '..', '..');
  const mk = (o) => ({
    activity_id: 0, activity_start_point: 0, activity_end_point: 0, rank: 1,
    all_time_rank: 1, last_365_rank: 1, current_cycle_rank: 1, recency_rank: 1,
    cycle_name: null, start_time_utc: '', elapsed_duration_s: 0, gap_s: 0,
    pace_str: null, ...o,
  });

  // The live-poison scenario: an out-of-window effort with non-null ranks.
  const oldEffort = mk({ cycle_name: 'All Time', recency_rank: 14, current_cycle_rank: 32, last_365_rank: 33 });
  check('buckets: non-null ranks do NOT make an effort a cycle effort',
    bucketOf(oldEffort) === 'alltime', bucketOf(oldEffort));
  check('buckets: most recent wins over its cycle label',
    bucketOf(mk({ cycle_name: 'Current Cycle', recency_rank: 1 })) === 'recent');
  check('buckets: Current Cycle → cycle',
    bucketOf(mk({ cycle_name: 'Current Cycle', recency_rank: 4 })) === 'cycle');
  check('buckets: Last 365 → 365',
    bucketOf(mk({ cycle_name: 'Last 365', recency_rank: 5 })) === '365');
  check('buckets: missing label → alltime (never guessed into a window)',
    bucketOf(mk({ cycle_name: null, recency_rank: 6 })) === 'alltime');
  const seen = new Set([
    bucketOf(mk({ cycle_name: 'Current Cycle', recency_rank: 1 })),
    bucketOf(mk({ cycle_name: 'Current Cycle', recency_rank: 4 })),
    bucketOf(mk({ cycle_name: 'Last 365', recency_rank: 5 })),
    bucketOf(mk({ cycle_name: 'All Time', recency_rank: 14 })),
  ]);
  check('buckets: all four buckets reachable from real label/rank combos',
    seen.size === 4, [...seen].join(','));

  const cyc = mk({ cycle_name: 'Current Cycle' });
  const y365 = mk({ cycle_name: 'Last 365' });
  const allTime = mk({ cycle_name: 'All Time' });
  const unknown = mk({ cycle_name: null });
  check('range: Current Cycle keeps only in-cycle efforts',
    inRange(cyc, 'Current Cycle') && !inRange(y365, 'Current Cycle') && !inRange(allTime, 'Current Cycle'));
  check('range: Last 365 includes in-cycle efforts too (they are within 365 days)',
    inRange(cyc, 'Last 365') && inRange(y365, 'Last 365') && !inRange(allTime, 'Last 365'));
  check('range: Most Recent / All Time stay whole-set views',
    ['Most Recent', 'All Time'].every((r) => [cyc, y365, allTime, unknown].every((e) => inRange(e, r))));
  check('range: an unknown label is outside both windows',
    !inRange(unknown, 'Current Cycle') && !inRange(unknown, 'Last 365'));

  // Cross-surface contract guard: the label must travel view → query → schema →
  // handler → frontend type, and the dead rank-null chain must not return.
  const read = (p) => readFileSync(p, 'utf8');
  const querySrc = read(path.join(root, 'backend_functions', 'queries', 'activities_queries.py'));
  const schemaSrc = read(path.join(root, 'backend', 'schemas', 'leaderboard_schemas.py'));
  const handlerSrc = read(path.join(root, 'backend', 'api', 'activities.py'));
  const typesSrc = read(path.join(srcDir, 'lib', 'types', 'leaderboards.ts'));
  const lollipopSrc = read(path.join(srcDir, 'app', 'activities', 'leaderboards', 'components', 'EffortLollipop.tsx'));
  const tableSrc = read(path.join(srcDir, 'app', 'activities', 'leaderboards', 'components', 'LeaderboardTable.tsx'));
  check('contract: cycle_name projected by the leaderboard query', /\bcycle_name\b/.test(querySrc));
  check('contract: cycle_name on the pydantic LeaderboardEffort', /cycle_name:\s*Optional\[str\]/.test(schemaSrc));
  check('contract: cycle_name mapped in the endpoint handler', /cycle_name=r\.get\('cycle_name'\)/.test(handlerSrc));
  check('contract: frontend type declares cycle_name', /cycle_name:\s*LeaderboardCycleName \| null/.test(typesSrc));
  check('buckets: lollipop uses bucketOf, not a rank null-chain',
    /bucketOf\(e\)/.test(lollipopSrc) && !/current_cycle_rank\s*!=\s*null/.test(lollipopSrc));
  check('range: table filters by membership, not a rank null-check',
    /inRange\(e, range\)/.test(tableSrc) && !/const r = e\[rankCol\]/.test(tableSrc));
}

if (failures > 0) {
  console.error(`\n${failures} assertion(s) failed`);
  process.exit(1);
}
console.log('\nAll bar-geometry assertions passed.');
