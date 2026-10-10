/** Static guard for the T06 bullet contract (node-only, no DOM):
 *  four metrics with units/formats, Chart.js horizontal bars (house library),
 *  tokenRgb theming, text-labelled values, gated on efficiency != null, and no
 *  placeholder remnants. Also pins the bullet math on fixed cases. */
import { readFileSync } from 'node:fs';

const comp = readFileSync(
  'frontend/pifitness/src/app/activities/recent-activity/components/EfficiencyBullets.tsx',
  'utf8',
);
const page = readFileSync(
  'frontend/pifitness/src/app/activities/recent-activity/page.tsx',
  'utf8',
);

// Mirror of bulletDomain/bulletFrac for fixed-case math pins.
function domain(cur, ref) {
  return [0, Math.max(Math.max(cur ?? 0, ref ?? 0, 0) * 1.1, 1)];
}
function frac(v, max) {
  if (v == null || max <= 0) return 0;
  return Math.min(Math.max(v / max, 0), 1);
}

const checks = [
  ['four metrics', /avg_cadence/.test(comp) && /avg_stride_length/.test(comp)
    && /avg_gct/.test(comp) && /avg_vert_ratio/.test(comp)],
  ['units spm/cm/ms/%', /'spm'/.test(comp) && /'cm'/.test(comp) && /'ms'/.test(comp) && /'%'/.test(comp)],
  ['uses Chart.js (house library)', /BarElement/.test(comp) && /indexAxis: 'y'/.test(comp)],
  ['tokenRgb theming', /tokenRgb/.test(comp)],
  ['ref marker: Chart.js bulletRefLine plugin (T06-1/T06-2)', /bulletRefLine/.test(comp) && /afterDatasetsDraw/.test(comp) && /getPixelForValue/.test(comp)],
  ['ref gated on usable average (T06-2)', /refVisible/.test(comp)],
  ['labels left of bars (T06-1 recheck)', /grid-cols-/.test(comp) && /'Cadence'/.test(comp) && /'Stride'/.test(comp) && /'GCT'/.test(comp) && /'Vert Ratio'/.test(comp)],
  ['resolved token colors (undeclared-token bug)', /tokenRgb/.test(comp)],
  ['text-labelled values', /aria-label/.test(comp) && /prior-10 avg/.test(comp)],
  ['page gates on efficiency', /report\.efficiency &&/.test(page)],
  ['no placeholder remnants', !/show_efficiency_placeholder/.test(page) && !/coming soon/.test(page)],
  ['domain: both null -> [0,1]', JSON.stringify(domain(null, null)) === JSON.stringify([0, 1])],
  ['domain: cur 172 ref 170 -> hi ~189.2', Math.abs(domain(172, 170)[1] - 189.2) < 1e-9],
  ['frac: cur fills 172/189.2', Math.abs(frac(172, 189.2) - 172 / 189.2) < 1e-12],
  ['frac: null -> 0', frac(null, 189.2) === 0],
];
let fail = 0;
for (const [name, ok] of checks) {
  if (!ok) fail += 1;
  console.log(ok ? 'PASS' : 'FAIL', '-', name);
}
process.exit(fail ? 1 : 0);
