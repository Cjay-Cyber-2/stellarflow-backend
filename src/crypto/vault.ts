import * as crypto from 'crypto';
import { logger } from '../utils/logger';

/**
 * VaultContext — Scoped access token issued exclusively by VaultManager.
 */
export class VaultContext {
  private readonly _scope: string;
  private readonly _token: Buffer;

  constructor(sentinel: symbol, scope: string, token: Buffer) {
    if (sentinel !== VAULT_CONTEXT_SENTINEL) {
      throw new Error('VaultContext must be created via VaultManager.openContext().');
    }
    this._scope = scope;
    this._token = token;
  }

  get scope(): string {
    return this._scope;
  }

  /** @internal */
  get token(): Buffer {
    return this._token;
  }
}

const VAULT_CONTEXT_SENTINEL = Symbol('VaultContextSentinel');

/**
 * VaultManager — Isolated in-memory store for sensitive keys with strict access barriers.
 * Inspired by vault_manager.py
 */
export class VaultManager {
  private static _instance: VaultManager;
  private readonly _keys: Map<string, Buffer> = new Map();
  private readonly _contexts: Map<string, string> = new Map(); // tokenHex -> scope

  private constructor() {}

  public static getInstance(): VaultManager {
    if (!VaultManager._instance) {
      VaultManager._instance = new VaultManager();
    }
    return VaultManager._instance;
  }

  /**
   * Store a sensitive key under keyId.
   */
  public register(keyId: string, rawKey: string | Buffer): void {
    if (!keyId) throw new Error('keyId must be a non-empty string.');
    if (!rawKey) throw new Error('rawKey must be non-empty.');

    const keyBuffer = Buffer.isBuffer(rawKey) ? Buffer.from(rawKey) : Buffer.from(rawKey, 'utf8');

    if (this._keys.has(keyId)) {
      throw new Error(`Key '${keyId}' is already registered. Call revoke() first.`);
    }

    this._keys.set(keyId, keyBuffer);
    logger.info(`[VaultManager] Key registered: ${keyId}`);
  }

  /**
   * Issue a scoped access token.
   */
  public openContext(scope: string): VaultContext {
    if (!scope) throw new Error('scope must be a non-empty string.');

    const token = crypto.randomBytes(32);
    this._contexts.set(token.toString('hex'), scope);

    logger.info(`[VaultManager] Context opened for scope: ${scope}`);
    return new VaultContext(VAULT_CONTEXT_SENTINEL, scope, token);
  }

  /**
   * Retrieve a key after validating context.
   */
  public retrieve(keyId: string, context: VaultContext): string {
    if (!(context instanceof VaultContext)) {
      throw new Error('context must be a VaultContext issued by VaultManager.');
    }

    const tokenHex = context.token.toString('hex');
    const storedScope = this._contexts.get(tokenHex);

    if (!storedScope) {
      throw new Error('Invalid or unrecognised vault context.');
    }

    if (storedScope !== context.scope) {
      throw new Error('Vault context scope mismatch.');
    }

    const keyBuffer = this._keys.get(keyId);
    if (!keyBuffer) {
      throw new Error(`No key registered for id '${keyId}'.`);
    }

    // Return a string copy to avoid sharing the underlying buffer
    return keyBuffer.toString('utf8');
  }

  /**
   * Invalidate a context token.
   */
  public closeContext(context: VaultContext): void {
    if (!(context instanceof VaultContext)) {
      throw new Error('context must be a VaultContext instance.');
    }

    this._contexts.delete(context.token.toString('hex'));
    logger.info(`[VaultManager] Context closed for scope: ${context.scope}`);
  }

  /**
   * Zero-wipe and remove keyId from the vault.
   */
  public revoke(keyId: string): void {
    const keyBuffer = this._keys.get(keyId);
    if (keyBuffer) {
      keyBuffer.fill(0);
      this._keys.delete(keyId);
      logger.info(`[VaultManager] Key revoked: ${keyId}`);
    } else {
      logger.warn(`[VaultManager] revoke() called for unknown key: ${keyId}`);
    }
  }

  /**
   * Zero-wipe all keys and invalidate all contexts.
   */
  public purge(): void {
    for (const [keyId, buffer] of this._keys.entries()) {
      buffer.fill(0);
    }
    this._keys.clear();
    this._contexts.clear();
    logger.info('[VaultManager] All keys purged and all contexts invalidated.');
  }
}

export const vault = VaultManager.getInstance();
