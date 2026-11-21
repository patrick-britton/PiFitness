import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

describe('PWA configuration (UI Principles R10 / B26)', () => {
  const publicDir = path.resolve(__dirname, '../../../../public');

  it('provides a valid manifest.json with required fields', () => {
    const manifestPath = path.join(publicDir, 'manifest.json');
    expect(fs.existsSync(manifestPath)).toBe(true);

    const content = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
    expect(content.name).toBe('PiFitness');
    expect(content.short_name).toBe('PiFitness');
    expect(content.start_url).toBe('/');
    expect(content.display).toBe('standalone');
    expect(content.theme_color).toBe('#2563eb');
    expect(Array.isArray(content.icons)).toBe(true);
    expect(content.icons.length).toBeGreaterThanOrEqual(2);
  });

  it('provides service worker file sw.js with cache management', () => {
    const swPath = path.join(publicDir, 'sw.js');
    expect(fs.existsSync(swPath)).toBe(true);

    const swContent = fs.readFileSync(swPath, 'utf-8');
    expect(swContent).toContain('CACHE_NAME');
    expect(swContent).toContain('addEventListener(\'install\'');
    expect(swContent).toContain('addEventListener(\'activate\'');
    expect(swContent).toContain('addEventListener(\'fetch\'');
  });

  it('provides icon assets in public directory', () => {
    expect(fs.existsSync(path.join(publicDir, 'icon.svg'))).toBe(true);
    expect(fs.existsSync(path.join(publicDir, 'icon-192.png'))).toBe(true);
    expect(fs.existsSync(path.join(publicDir, 'icon-512.png'))).toBe(true);
  });
});
