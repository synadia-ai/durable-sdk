// Saga / compensation — a distributed transaction with no coordinator.
//
// `book-trip` runs three forward steps (reserve-flight → charge-card → reserve-hotel). If a later
// step can't complete, the already-committed steps are undone by running their COMPENSATIONS in
// reverse order — and because each compensation is itself a durable ctx.run, a crash mid-unwind
// resumes the unwind from the journal. The rollback is as crash-safe as the forward path.
//
//   nats-server -js &
//   NATS_URL=nats://127.0.0.1:4222 npm run example:saga
//   # then:  cd apps/ui && NATS_URL=nats://127.0.0.1:4222 bun run server   → :8088
//
// Seeds two runs: `trip-booked` (all three steps succeed) and `trip-rolledback` (no rooms → unwind).
// See docs/walkthroughs/saga.md.

import { readFileSync } from "node:fs";
import { connect, credsAuthenticator } from "@nats-io/transport-node";
import {
  createClient,
  createWorker,
  defineWorkflow,
  withReconnectDefaults,
  type Ctx,
  type Logger,
} from "../src/index.js";

interface Trip {
  trip: string;
  rooms: number; // how many rooms are free at the hotel (0 → the booking can't complete)
}

const bookTrip = defineWorkflow(
  "book-trip",
  async (ctx: Ctx, input: Trip): Promise<{ status: string; detail: string }> => {
    // A LIFO stack of compensations: each forward step, once committed, pushes how to undo itself.
    // On any failure we pop them in reverse. Each undo is a durable step in its own right.
    const undo: { name: string; run: () => string }[] = [];

    try {
      const flight = await ctx.run("reserve-flight", () => {
        console.log(`  ✈  reserve-flight    ${input.trip}: seat held → FL-${input.trip}`);
        return `FL-${input.trip}`;
      });
      undo.push({
        name: "cancel-flight",
        run: () => {
          console.log(`     ↩  cancel-flight     released ${flight}`);
          return `released ${flight}`;
        },
      });

      const charge = await ctx.run("charge-card", () => {
        console.log(`  💳  charge-card       ${input.trip}: $1,200 captured → CHG-${input.trip}`);
        return `CHG-${input.trip}`;
      });
      undo.push({
        name: "refund-card",
        run: () => {
          console.log(`     ↩  refund-card       refunded ${charge}`);
          return `refunded ${charge}`;
        },
      });

      const hotel = await ctx.run("reserve-hotel", () => {
        const ok = input.rooms > 0;
        console.log(
          `  🏨  reserve-hotel     ${input.trip}: ${ok ? `room held → HT-${input.trip}` : "NO ROOMS AVAILABLE"}`,
        );
        return { ok, ref: ok ? `HT-${input.trip}` : null };
      });
      if (!hotel.ok) throw new Error("hotel sold out");

      return { status: "booked", detail: `FL-${input.trip} · CHG-${input.trip} · ${hotel.ref}` };
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      console.log(`  ✖  ${input.trip}: ${reason} — unwinding ${undo.length} step(s) in reverse:`);
      // Compensate in REVERSE. Each undo is a ctx.run, so the unwind is journaled and crash-safe.
      for (const c of undo.reverse()) await ctx.run(c.name, c.run);
      return { status: "rolled-back", detail: `${reason} → ${undo.length} step(s) compensated` };
    }
  },
);

const logger: Logger = {
  debug() {},
  info() {},
  warn: (m, f) => console.warn(m, f ?? ""),
  error: (m, f) => console.error(m, f ?? ""),
};

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  // NATS_CREDS: a creds file, for the fabric (docs/FABRIC.md); absent, a plain local server.
  const creds = process.env.NATS_CREDS;
  const auth = creds ? { authenticator: credsAuthenticator(readFileSync(creds)) } : {};
  const nc = await connect(withReconnectDefaults({ servers: url, ...auth }));
  const worker = createWorker({ nc, workflows: [bookTrip], logger });
  await worker.start();
  const client = createClient({ nc });

  console.log("\n— trip-booked (4 rooms free) —");
  const ok = await client.start(bookTrip, { trip: "PAR", rooms: 4 }, { runId: "trip-booked" });

  console.log("\n— trip-rolledback (0 rooms free) —");
  const bad = await client.start(bookTrip, { trip: "TYO", rooms: 0 }, { runId: "trip-rolledback" });

  console.log("\noutcomes:");
  console.log("  trip-booked     →", await ok.result(10_000));
  console.log("  trip-rolledback →", await bad.result(10_000));

  await worker.stop();
  await nc.drain();
  console.log(`\n✅ seeded saga demo at ${url} — open the dashboard to inspect both journals`);
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
