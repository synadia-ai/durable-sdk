// The bandwidth term of a large-value test's budget (docs/DEVELOPING.md "Against an existing server"). The
// round-trip factor (existing.ts `rttFactor`) scales a test's waits with the link's latency; a
// test that moves values over the inline limit also waits for their bytes, which over a WAN take
// longer than its round trips: the dev NGS run of 2026-10-04 moved one test's ~42 MiB in 81 s at a
// 51 ms round trip. So such a test's budget adds the bytes it moves over the throughput measured
// at the start (existing.ts `measureThroughput`: a put and a get through the engine), × HEADROOM.
// Vitest-free: global-setup.ts prints the headroom next to the throughput.

/** How many times the probe's time a test's bytes are given: the probe moves one value once up
 * and once down, a test's values also cross the engine's own hops to the store more than once. */
export const HEADROOM = 2;

/** The ms `bytes` take at `bps` (bytes per second, up and down together), × HEADROOM; 0 when no
 * throughput was measured (the spawned server: its link is the loopback). */
export function bandwidthMs(bytes: number, bps: number): number {
  if (!(bps > 0) || !(bytes > 0)) return 0;
  return Math.ceil(((bytes * 1000) / bps) * HEADROOM);
}
