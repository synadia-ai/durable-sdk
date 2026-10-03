import { defineConfig } from "vitest/config";

// The unit suites: the pure core, no server. The integration suites have their own config
// (vitest.integration.config.ts), which spawns a nats-server and durable-engine serve.
export default defineConfig({
  test: {
    include: ["test/unit/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
