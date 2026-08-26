/**
 * Keeper layer — secure secret / key safekeeping for the StellarFlow backend.
 *
 * The Keeper is the trusted root of key material in the backend. It stores
 * high-value secret bytes in zeroisable {@link Buffer} instances, derives
 * stable per-secret signing keys from a root key (the root key never leaves
 * the Keeper), and produces tamper-evident state snapshots that contain no
 * secret material — only HMAC fingerprints — for audit/verification.
 *
 * Dependency-free (Node built-ins only) so it runs in any deployment context,
 * including the integration test harness where no external KMS is available.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { PathLike } from "node:fs";

export interface SecretEnrollment {
  name: string;
  fingerprint: string;
  algorithm: string;
  createdSeq: number;
}

export class KeeperError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KeeperError";
  }
}

export class SecretNotFoundError extends KeeperError {
  constructor(name: string) {
    super(`secret not found: ${name}`);
    this.name = "SecretNotFoundError";
  }
}

/**
 * A zeroisable container for secret bytes. The underlying Buffer is
 * overwritten with zeroes on {@link SecureBytes.zeroise}.
 */
export class SecureBytes {
  private buf: Buffer;
  readonly name: string;

  constructor(data: Buffer | Uint8Array, name = "<secret>") {
    this.buf = Buffer.from(data);
    this.name = name;
  }

  get length(): number {
    return this.buf.length;
  }

  /** Return a fresh copy of the secret bytes for a single use. */
  expose(): Buffer {
    return Buffer.from(this.buf);
  }

  /** Overwrite the buffer with zeroes (cryptographic erase). */
  zeroise(): void {
    this.buf.fill(0);
    this.buf = Buffer.alloc(0);
  }
}

export class KeyKeeper {
  private root: Buffer;
  private readonly statePath: PathLike | null;
  private secrets = new Map<string, SecureBytes>();
  private meta = new Map<string, SecretEnrollment>();
  private seq = 0;

  constructor(rootKey?: Buffer | null, statePath?: PathLike | null) {
    this.root = rootKey && rootKey.length > 0 ? Buffer.from(rootKey) : Buffer.alloc(0);
    this.statePath = statePath ?? null;
  }

  put(name: string, secret: Buffer | Uint8Array, algorithm = "hmac-sha256"): SecretEnrollment {
    if (!name) throw new KeeperError("secret name must be non-empty");
    const fp = this.fingerprint(name, Buffer.from(secret));
    const existing = this.secrets.get(name);
    if (existing) existing.zeroise();
    this.seq += 1;
    this.secrets.set(name, new SecureBytes(Buffer.from(secret), name));
    const enrollment: SecretEnrollment = {
      name,
      fingerprint: fp,
      algorithm,
      createdSeq: this.seq,
    };
    this.meta.set(name, enrollment);
    return enrollment;
  }

  delete(name: string): void {
    const secret = this.secrets.get(name);
    if (!secret) throw new SecretNotFoundError(name);
    secret.zeroise();
    this.secrets.delete(name);
    this.meta.delete(name);
  }

  has(name: string): boolean {
    return this.secrets.has(name);
  }

  listEnrollments(): SecretEnrollment[] {
    return Array.from(this.meta.values());
  }

  private derivedKey(name: string): Buffer {
    return crypto.createHmac("sha256", this.root).update(`stellarflow-keeper|${name}`).digest();
  }

  sign(name: string, message: Buffer | Uint8Array): Buffer {
    if (!this.has(name)) throw new SecretNotFoundError(name);
    const key = this.derivedKey(name);
    return crypto.createHmac("sha256", key).update(message).digest();
  }

  verify(name: string, message: Buffer | Uint8Array, signature: Buffer | Uint8Array): boolean {
    if (!this.has(name)) return false;
    const key = this.derivedKey(name);
    const expected = crypto.createHmac("sha256", key).update(message).digest();
    return crypto.timingSafeEqual(expected, Buffer.from(signature));
  }

  rotateRootKey(newRootKey: Buffer | Uint8Array): void {
    this.root = Buffer.from(newRootKey);
  }

  secureWipe(): void {
    for (const secret of this.secrets.values()) secret.zeroise();
    this.secrets.clear();
    this.meta.clear();
  }

  snapshotState(): Record<string, unknown> {
    return {
      rootFingerprint: this.fingerprint("__root__", this.root),
      seq: this.seq,
      enrollments: this.listEnrollments(),
    };
  }

  persistState(target?: PathLike): PathLike {
    const out = target ?? this.statePath;
    if (!out) throw new KeeperError("no state path configured");
    const dir = path.dirname(out.toString());
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(out.toString(), JSON.stringify(this.snapshotState(), null, 2), "utf-8");
    return out;
  }

  private fingerprint(name: string, data: Buffer): string {
    return crypto
      .createHmac("sha256", Buffer.from("stellarflow-keeper-fp"))
      .update(`${name}|`)
      .update(data)
      .digest("hex");
  }
}

export default KeyKeeper;
