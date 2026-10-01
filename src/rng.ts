/**
 * Seeded pseudo-random numbers (mulberry32). Every source of randomness in
 * the simulator flows through one of these, so a run is fully determined by
 * its seed and can be replayed exactly.
 */
export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0 || 0x9e3779b9;
  }

  /** A float in [0, 1). */
  next(): number {
    let t = (this.state = (this.state + 0x6d2b79f5) >>> 0);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** An integer in [lo, hi], both inclusive. */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.next() * (hi - lo + 1));
  }

  chance(p: number): boolean {
    return this.next() < p;
  }

  pick<T>(items: readonly T[]): T {
    return items[Math.floor(this.next() * items.length)];
  }

  /**
   * A shuffled copy (Fisher–Yates). Never shuffle with
   * `sort(() => rng.next() - 0.5)`: how often sort calls the comparator
   * depends on the JavaScript engine, so the same seed would produce
   * different results in Node and in a browser.
   */
  shuffle<T>(items: readonly T[]): T[] {
    const a = [...items];
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(this.next() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  }

  /** A child generator, so separate concerns don't disturb each other's sequences. */
  fork(): Rng {
    return new Rng(Math.floor(this.next() * 4294967296));
  }
}
