// The simulation's only source of randomness: a seeded PRNG (mulberry32). Everything the simulator
// decides — the program, the interleaving, the faults — is drawn from one of these, so a seed IS a
// reproduction.

export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  /** A float in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** An integer in [0, n). */
  int(n: number): number {
    return Math.floor(this.next() * n);
  }

  chance(p: number): boolean {
    return this.next() < p;
  }

  pick<T>(items: readonly T[]): T {
    const item = items[this.int(items.length)];
    if (item === undefined) throw new Error("Rng.pick: empty list");
    return item;
  }

  /** Pick an index by weight (weights need not sum to 1). */
  weighted(weights: readonly number[]): number {
    let total = 0;
    for (const w of weights) total += w;
    let at = this.next() * total;
    for (let i = 0; i < weights.length; i++) {
      at -= weights[i] ?? 0;
      if (at < 0) return i;
    }
    return weights.length - 1;
  }

  /** An independent stream, so that one decision space can be varied while another is held. */
  fork(salt: number): Rng {
    return new Rng((Math.imul(this.int(0x7fffffff), 0x9e3779b1) ^ salt) >>> 0);
  }
}
