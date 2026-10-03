import { defineConfig } from "vitest/config";

// The library's integration suites, on durable-engine serve. One spawned nats-server for the run
// (global-setup.ts) and one engine per prefix a test uses (engine.ts), stopped after each test
// (setup-each.ts), which then deletes the streams.
export default defineConfig({
  test: {
    include: ["test/integration/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    fileParallelism: false,
    globalSetup: ["test/harness/global-setup.ts"],
    setupFiles: ["test/harness/setup-each.ts"],
  },
});
