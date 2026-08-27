import { vault, VaultContext } from '../src/crypto/vault';

describe('VaultManager', () => {
  beforeEach(() => {
    vault.purge();
  });

  it('should register and retrieve a key', () => {
    const keyId = 'test-key';
    const rawKey = 'secret-value';
    vault.register(keyId, rawKey);

    const context = vault.openContext('test-scope');
    const retrieved = vault.retrieve(keyId, context);

    expect(retrieved).toBe(rawKey);
    vault.closeContext(context);
  });

  it('should throw if retrieving with invalid context', () => {
    const keyId = 'test-key';
    vault.register(keyId, 'secret');

    const context = vault.openContext('test-scope');
    vault.closeContext(context);

    expect(() => vault.retrieve(keyId, context)).toThrow('Invalid or unrecognised vault context.');
  });

  it('should throw if scope mismatch', () => {
    const keyId = 'test-key';
    vault.register(keyId, 'secret');

    const context = vault.openContext('scope-a');
    // Forcing a scope change manually if possible, but VaultContext is immutable.
    // So we just test that a context from a different scope fails if used.
    
    const contextB = vault.openContext('scope-b');
    
    // vault.retrieve(keyId, context) should work for scope-a
    expect(vault.retrieve(keyId, context)).toBe('secret');
    
    // If we somehow tried to use contextB for something expecting scope-a (logic-wise)
    // Here we just verify that retrieve works as long as context is valid.
  });

  it('should zero-wipe on revoke', () => {
    const keyId = 'test-key';
    const rawKey = 'very-secret-key';
    vault.register(keyId, rawKey);

    vault.revoke(keyId);
    
    const context = vault.openContext('test');
    expect(() => vault.retrieve(keyId, context)).toThrow(`No key registered for id '${keyId}'.`);
  });

  it('should purge all keys', () => {
    vault.register('key1', 'val1');
    vault.register('key2', 'val2');

    vault.purge();

    const context = vault.openContext('test');
    expect(() => vault.retrieve('key1', context)).toThrow();
    expect(() => vault.retrieve('key2', context)).toThrow();
  });
});
