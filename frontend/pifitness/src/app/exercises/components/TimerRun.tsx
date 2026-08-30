/**
 * Timer Run Engine
 *
 * Exercises -> Timer Activation: the live timer. After the countdown
 * (5,4,3,2,1) the app counts up in real time, incrementing an on-pace rep
 * counter once per interval, with a metronome-style pacing cue (OQ-3):
 *   - a visual pulse on every interval boundary (always), and
 *   - an optional soft 'beep' via the Web Audio API, controlled by a per-run
 *     toggle; the AudioContext is created/resumed from the Start gesture.
 *
 * Timing correctness:
 *   - Count-up elapsed is driven by requestAnimationFrame using a fixed
 *     startedAtMs anchor (performance.now()), never accumulated setInterval, so
 *     tab-away / throttled rAF does not drift the pace.
 *   - On-pace rep count = floor(elapsedSeconds / intervalSeconds); it ticks
 *     exactly on interval boundaries (including fractional intervals like 2.5).
 *   - started_at (ISO, after countdown) and ended_at (ISO, on Stop) are captured
 *     so the save prompt (T09) can persist the confirmed attempt.
 *
 * Renders the large mobile-first countdown and the rep counter. The ring/bar
 * progress visualization is T08 (RunProgress) — this component exposes phase,
 * elapsedMs, and pacedCount for it.
 */

'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useViewportStore } from '../../../stores/viewportStore';
import type { ExerciseTimerSummary } from '../../../lib/types/exercises';
import RunProgress from './RunProgress';

export type RunPhase = 'countdown' | 'counting' | 'stopped';

export interface RunState {
  phase: RunPhase;
  /** Elapsed count-up time so far in milliseconds (0 during countdown). */
  elapsedMs: number;
  /** On-pace rep count = floor(elapsed seconds / interval). */
  pacedCount: number;
}

const COUNTDOWN_SECONDS = 5;

interface TimerRunProps {
  timer: ExerciseTimerSummary;
  onStop: (run: { startedAtIso: string; endedAtIso: string; elapsedMs: number }) => void;
  onBack: () => void;
}

export default function TimerRun({ timer, onStop, onBack }: TimerRunProps) {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';

  const interval = timer.interval_seconds;

  const [phase, setPhase] = useState<RunPhase>('countdown');
  const [countdownLeft, setCountdownLeft] = useState(COUNTDOWN_SECONDS);
  const [elapsedMs, setElapsedMs] = useState(0);
  const [pulseOn, setPulseOn] = useState(false);
  const [soundOn, setSoundOn] = useState(false);
  const [stopped, setStopped] = useState(false);

  // Anchor + refs so the rAF loop can read stable values.
  const startedAtMsRef = useRef<number | null>(null);
  const lastBoundaryRef = useRef(0);
  const phaseRef = useRef<RunPhase>('countdown');
  const soundRef = useRef(false);
  const audioCtxRef = useRef<AudioContext | null>(null);
  const rafRef = useRef<number | null>(null);
  const startedAtIsoRef = useRef<string | null>(null);

  phaseRef.current = phase;
  soundRef.current = soundOn;

  const pulseResetTimer = useRef<number | null>(null);
// ---------------------------------------------------------------------------
  // Countdown (5 → 1)
  // ---------------------------------------------------------------------------
  useEffect(() => {
    if (phase !== 'countdown') return;
    if (countdownLeft <= 0) {
      // Countdown finished — begin the count-up.
      startedAtMsRef.current = performance.now();
      startedAtIsoRef.current = new Date().toISOString();
      lastBoundaryRef.current = 0;
      setElapsedMs(0);
      setPhase('counting');
      return;
    }
    const t = setTimeout(() => setCountdownLeft((c) => c - 1), 1000);
    return () => clearTimeout(t);
  }, [phase, countdownLeft]);

  // ---------------------------------------------------------------------------
  // Web Audio: create/resume AudioContext from the Start gesture (OQ-3)
  // ---------------------------------------------------------------------------
  const ensureAudio = useCallback(() => {
    if (typeof window === 'undefined') return;
    const Ctx =
      window.AudioContext ||
      (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctx) return;
    if (!audioCtxRef.current) {
      audioCtxRef.current = new Ctx();
    }
    if (audioCtxRef.current.state === 'suspended') {
      audioCtxRef.current.resume().catch(() => {});
    }
  }, []);

  const strokeBeep = useCallback(() => {
    const ctx = audioCtxRef.current;
    if (!ctx) return;
    // Soft beep: short, low-gain oscillator — deliberately not a metronome tick.
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.value = 880;
    gain.gain.setValueAtTime(0.0001, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.06, ctx.currentTime + 0.005);
    gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.09);
    osc.connect(gain).connect(ctx.destination);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.1);
  }, []);

  const beat = useCallback(() => {
    // Visual pulse always (OQ-3); audio only when sound is enabled.
    setPulseOn(true);
    if (pulseResetTimer.current) window.clearTimeout(pulseResetTimer.current);
    pulseResetTimer.current = window.setTimeout(() => setPulseOn(false), 140);
    if (soundRef.current) strokeBeep();
  }, [strokeBeep]);

  // ---------------------------------------------------------------------------
  // Count-up tick loop (requestAnimationFrame, timestamp-delta anchored)
  // ---------------------------------------------------------------------------
  useEffect(() => {
    if (phase !== 'counting') return;

    const tick = () => {
      const anchor = startedAtMsRef.current;
      const now = performance.now();
      if (anchor == null) return;

      const elapsed = now - anchor; // real wall-clock delta in ms
      const elapsedSec = elapsed / 1000;

      // Which interval boundary are we at?
      const boundary = Math.floor(elapsedSec / interval);

      // Fire the pacing cue exactly once per boundary crossing.
      if (boundary > lastBoundaryRef.current) {
        for (let b = lastBoundaryRef.current + 1; b <= boundary; b += 1) beat();
        lastBoundaryRef.current = boundary;
      }

      setElapsedMs(elapsed);
      rafRef.current = requestAnimationFrame(tick);
    };

    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    };
  }, [phase, interval, beat]);
// ---------------------------------------------------------------------------
  // Stop / back
  // ---------------------------------------------------------------------------
  const handleStop = useCallback(() => {
    if (phaseRef.current !== 'counting') return;
    const endedAt = new Date().toISOString();
    // Clean up the rAF loop + audio.
    if (rafRef.current != null) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    }
    if (pulseResetTimer.current) window.clearTimeout(pulseResetTimer.current);
    if (audioCtxRef.current) {
      audioCtxRef.current.close().catch(() => {});
      audioCtxRef.current = null;
    }
    setPhase('stopped');
    setStopped(true);
    onStop({
      startedAtIso: startedAtIsoRef.current ?? new Date().toISOString(),
      endedAtIso: endedAt,
      elapsedMs: performance.now() - (startedAtMsRef.current ?? performance.now()),
    });
  }, [onStop]);

  const handleBack = useCallback(() => {
    const running = phaseRef.current === 'counting';
    if (running && !window.confirm('A timer is running. Leave without saving?')) return;
    if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
    if (audioCtxRef.current) {
      audioCtxRef.current.close().catch(() => {});
      audioCtxRef.current = null;
    }
    onBack();
  }, [onBack]);

  // Guard against accidental tab close / reload during a run.
  useEffect(() => {
    const handler = (e: BeforeUnloadEvent) => {
      if (phaseRef.current !== 'counting') return undefined;
      e.preventDefault();
      e.returnValue = '';
      return '';
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, []);

  // Cleanup on unmount.
  useEffect(() => {
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
      if (pulseResetTimer.current) window.clearTimeout(pulseResetTimer.current);
      if (audioCtxRef.current) {
        audioCtxRef.current.close().catch(() => {});
        audioCtxRef.current = null;
      }
    };
  }, []);
const paced = Math.floor(elapsedMs / 1000 / interval);
  // Fraction elapsed into the current interval (0..1); the ring retraces it.
  const intervalFraction = (elapsedMs / 1000 / interval) % 1;

  // Sound toggle — must come from a user gesture for AudioContext (OQ-3).
  const handleSoundToggle = () => {
    if (!soundOn) ensureAudio();
    setSoundOn((s) => !s);
  };

  return (
    <div className={isDesktop ? 'max-w-2xl mx-auto' : ''}>
      <div className="flex flex-col items-center gap-6 py-6">
        <div className="text-center">
          <h2 className="text-xl font-bold text-gray-900 dark:text-white">{timer.name}</h2>
          <p className="mt-0.5 text-sm text-gray-500 dark:text-gray-400">
            {timer.interval_seconds} sec/rep
          </p>
        </div>

        {/* Sound toggle (per-run, OQ-3) */}
        <label className="inline-flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
          <input
            type="checkbox"
            checked={soundOn}
            onChange={handleSoundToggle}
            className="h-4 w-4 rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500"
          />
          Sound
        </label>

        {phase === 'countdown' ? (
          <div className="text-center" aria-live="assertive">
            <p className="text-7xl sm:text-8xl font-black tabular-nums text-gray-900 dark:text-white">
              {countdownLeft}
            </p>
            <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">Get ready…</p>
          </div>
        ) : (
          <div className="text-center">
            <p
              className={`text-7xl sm:text-8xl font-black tabular-nums text-gray-900 dark:text-white transition-transform ${
                pulseOn ? 'scale-110' : 'scale-100'
              }`}
            >
              {paced}
            </p>
            <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">on-pace reps </p>
          </div>
        )}

        {/* Stop */}
        {phase === 'counting' && (
          <button
            type="button"
            onClick={handleStop}
            className="inline-flex items-center justify-center rounded-full bg-red-600 px-10 py-4 text-2xl font-semibold text-white hover:bg-red-700 focus:outline-none focus-visible:ring-4 focus-visible:ring-red-500"
          >
            Stop
          </button>
        )}

        {phase === 'stopped' && (
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Run stopped — saving…
          </p>
        )}

        <button
          type="button"
          onClick={handleBack}
          className="text-sm font-medium text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded-md px-3 py-2"
        >
          ← Back
        </button>

        {/* Progress visualization (T08) — interval ring + prior-attempt bar */}
        {phase === 'counting' && (
          <RunProgress
            intervalFraction={intervalFraction}
            pacedCount={paced}
            intervalSeconds={interval}
            priorPacedCount={timer.last_attempt_paced_count}
            active
          />
        )}
      </div>
    </div>
  );
}