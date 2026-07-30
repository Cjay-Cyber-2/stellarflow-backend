import * as crypto from 'crypto';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12;
const TAG_LENGTH = 16;

/**
 * Decrypts a string that was encrypted using AES-256-GCM.
 * Format: iv (hex) + ciphertext (hex) + authTag (hex)
 * 
 * @param encryptedText - The encrypted string in hex format
 * @param masterKey - The 32-byte master key (as hex or string)
 * @returns The decrypted plaintext string
 */
export function decrypt(encryptedText: string, masterKey: string): string {
  if (!encryptedText) throw new Error('encryptedText must be provided');
  if (!masterKey) throw new Error('masterKey must be provided');

  // Convert masterKey to 32-byte buffer
  const key = crypto.createHash('sha256').update(masterKey).digest();

  try {
    const buffer = Buffer.from(encryptedText, 'hex');
    
    const iv = buffer.subarray(0, IV_LENGTH);
    const tag = buffer.subarray(buffer.length - TAG_LENGTH);
    const ciphertext = buffer.subarray(IV_LENGTH, buffer.length - TAG_LENGTH);

    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
    decipher.setAuthTag(tag);

    let decrypted = decipher.update(ciphertext, undefined, 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
  } catch (err: any) {
    throw new Error(`Decryption failed: ${err.message}`);
  }
}

/**
 * Encrypts a string using AES-256-GCM.
 * Used primarily for generating test data or manual key preparation.
 * 
 * @param text - The plaintext string to encrypt
 * @param masterKey - The 32-byte master key
 * @returns Format: iv (hex) + ciphertext (hex) + authTag (hex)
 */
export function encrypt(text: string, masterKey: string): string {
  const key = crypto.createHash('sha256').update(masterKey).digest();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  const tag = cipher.getAuthTag();

  return iv.toString('hex') + encrypted + tag.toString('hex');
}
