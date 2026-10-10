/** Smoke-check the AC-3 CourseInfoBar text branches (node-only, no JSX). */
function formatAbsMSS(delta) {
  if (delta == null) return '\u2014';
  const total = Math.round(Math.abs(delta));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`;
}
function formatInfoDelta(delta, noun) {
  if (delta == null) return `\u2014 ${noun}`;
  if (delta === 0) return `0:00 ${noun}`;
  return `${formatAbsMSS(delta)} ${delta > 0 ? 'slower' : 'faster'} ${noun}`;
}
function infoText(course) {
  const rank = course.all_time_rank;
  if (rank === 1)
    return course.best_delta_s != null
      ? `faster by ${formatAbsMSS(course.best_delta_s)} vs best`
      : 'fastest attempt';
  if (course.prior_delta_s != null && course.best_delta_s != null
      && course.prior_delta_s === course.best_delta_s)
    return `${formatAbsMSS(course.prior_delta_s)} ${course.prior_delta_s > 0 ? 'slower' : 'faster'} than prior/best`;
  return `${formatInfoDelta(course.prior_delta_s, 'than prior')}, ${formatInfoDelta(course.best_delta_s, 'than best')}`;
}
const cases = [
  [{ all_time_rank: 1, prior_delta_s: 211.74, best_delta_s: 211.74 }, 'faster by 3:32 vs best'],
  [{ all_time_rank: 2, prior_delta_s: -12.5, best_delta_s: 8.25 }, '0:13 faster than prior, 0:08 slower than best'],
  [{ all_time_rank: 3, prior_delta_s: 30, best_delta_s: 30 }, '0:30 slower than prior/best'],
  [{ all_time_rank: 5, prior_delta_s: 75, best_delta_s: -130 }, '1:15 slower than prior, 2:10 faster than best'],
];
let fail = 0;
for (const [course, want] of cases) {
  const got = infoText(course);
  const ok = got === want;
  if (!ok) fail += 1;
  console.log(ok ? 'PASS' : 'FAIL', JSON.stringify(course), '=>', got, ok ? '' : `(want ${want})`);
}
process.exit(fail ? 1 : 0);
