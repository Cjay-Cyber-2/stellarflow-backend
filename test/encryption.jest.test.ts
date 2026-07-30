import { encrypt, decrypt } from '../src/crypto/encryption';

describe('Encryption Utilities', () => {
  const masterKey = 'my-secret-master-key';
  const plaintext = 'S...stellar-secret-key';

  it('should encrypt and decrypt a string', () => {
    const encrypted = encrypt(plaintext, masterKey);
    expect(encrypted).not.toBe(plaintext);
    
    const decrypted = decrypt(encrypted, masterKey);
    expect(decrypted).toBe(plaintext);
  });

  it('should fail decryption with wrong master key', () => {
    const encrypted = encrypt(plaintext, masterKey);
    expect(() => decrypt(encrypted, 'wrong-key')).toThrow();
  });

  it('should fail decryption with malformed payload', () => {
    expect(() => decrypt('not-hex', masterKey)).toThrow();
    expect(() => decrypt('abcdef1234567890', masterKey)).toThrow(); // Too short
  });
});
