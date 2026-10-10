/** Static guard for the T05 stacked-chart contract (node-only, no DOM):
 *  both charts share x 0→distance_m, no axis display/grid/border, HR keeps
 *  its median/max reference lines, and the pace series is gone. */
import { readFileSync } from 'node:fs';

const charts = readFileSync(
  'frontend/pifitness/src/app/activities/recent-activity/components/ActivityReportCharts.tsx',
  'utf8',
);
const page = readFileSync(
  'frontend/pifitness/src/app/activities/recent-activity/page.tsx',
  'utf8',
);
const contract = readFileSync(
  'frontend/pifitness/src/lib/types/activity-report.ts',
  'utf8',
);
const checks = [
  ['no PaceChart export', !/export function PaceChart/.test(charts)],
  ['no ActivityPacePoint import', !/ActivityPacePoint/.test(charts)],
  ['no minute keys', !/p\.minute/.test(charts)],
  ['distance x mapping', /p\.distance_m/.test(charts)],
  ['xMax plumbed', /max: xMax/.test(charts) && /xMax=\{report\.header\.distance_m\}/.test(page)],
  ['axes hidden (display:false)', (charts.match(/display: false/g) || []).length >= 4],
  ['no grid/ticks/border shown', !/display: true/.test(charts) && /border: \{ display: false \}/.test(charts)],
  ['HR median/max ref lines kept', /Median/.test(charts) && /Max/.test(charts)],
  ['pace gone from contract', !/ActivityPacePoint/.test(contract) && !/pace: ActivityPacePoint/.test(contract)],
  ['minute gone from contract', !/minute: number/.test(contract)],
  ['page: no pace branch', !/charts\.pace/.test(page) && !/PaceChart/.test(page)],
  ['page: 0-gap stack classes', /space-y-0/.test(page) && /leading-none/.test(page)],
];
let fail = 0;
for (const [name, ok] of checks) {
  if (!ok) fail += 1;
  console.log(ok ? 'PASS' : 'FAIL', '-', name);
}
process.exit(fail ? 1 : 0);
