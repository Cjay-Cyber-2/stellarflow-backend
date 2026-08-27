import dotenv from 'dotenv';
import { createSigner, SignerConfig } from './signer.factory';
import { ISigner } from './signer.interface';
import { getSecretKey, getPublicKey } from '../services/secretManager';

dotenv.config();

const config: SignerConfig = {
  backend: (process.env.SIGNER_BACKEND as 'kms' | 'local') || 'local',
  kmsKeyId: process.env.AWS_KMS_KEY_ID,
  kmsRegion: process.env.AWS_REGION,
  stellarPublicKey: getPublicKey(),
  localSecret: process.env.SIGNER_BACKEND === 'kms' ? undefined : getSecretKey(),
};

export const signer: ISigner = createSigner(config);
export * from './signer.interface';
export * from './kms-signer.service';
export * from './local-signer.service';
export * from './signer.factory';
