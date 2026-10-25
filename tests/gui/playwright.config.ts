import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: 'specs',
  outputDir: 'results',
  globalSetup: './run.ts',
  // A test starts a leader and often workers, and waits for heartbeats and deploys to land.
  timeout: 120_000,
  // Every test starts a leader of its own, so they run side by side.
  fullyParallel: true,
  // Mostly waiting, so one per CPU rather than the default half. Twice that starves the leaders and times tests out.
  workers: '100%',
  reporter: [['list'], ['html', { outputFolder: 'report', open: 'never' }]],
  use: {
    // A leader serves its own self-signed certificate.
    ignoreHTTPSErrors: true,
    viewport: { width: 1440, height: 900 },
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
    // Run as root, as in a container, Chromium refuses to start sandboxed.
    launchOptions: { args: ['--no-sandbox'] },
  },
  // The fuzzer's coverage is judged once all its seeds have run, so it is a project of its own after them.
  projects: [
    { name: 'stories', testIgnore: /-fuzz/ },
    // A step that cannot finish -- signing in, choosing -- fails its seed at once, rather than when the seed runs out
    // of time.
    { name: 'fuzz', testMatch: /gui-fuzz\.spec\.ts/, use: { actionTimeout: 10_000 } },
    { name: 'fuzz-coverage', testMatch: /gui-fuzz-coverage\.spec\.ts/, dependencies: ['fuzz'] },
    { name: 'journey', testMatch: /journey-fuzz\.spec\.ts/, use: { actionTimeout: 15_000 } },
    {
      name: 'journey-coverage',
      testMatch: /journey-fuzz-coverage\.spec\.ts/,
      dependencies: ['journey'],
    },
    { name: 'pipeline', testMatch: /pipeline-fuzz\.spec\.ts/, use: { actionTimeout: 10_000 } },
    {
      name: 'pipeline-coverage',
      testMatch: /pipeline-fuzz-coverage\.spec\.ts/,
      dependencies: ['pipeline'],
    },
  ],
});
