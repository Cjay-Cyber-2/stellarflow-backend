import "dotenv/config";
import axios, { AxiosInstance } from "axios";

const PROOF_SERVICE_URL =
  process.env.PROOF_SERVICE_URL || "http://localhost:8000";

const httpClient: AxiosInstance = axios.create({
  baseURL: PROOF_SERVICE_URL,
  timeout: parseInt(process.env.PROOF_SERVICE_TIMEOUT_MS || "30000", 10),
  headers: {
    "Content-Type": "application/json",
  },
});

export interface ProofPayload {
  proof_hex: string;
  public_inputs: string[];
  contract_params: Record<string, any>;
  transaction_hash: string;
  proof_scheme: "groth16" | "plonk" | "marlin" | "flonk";
}

export interface ProofVerificationRequest {
  proof: ProofPayload;
  simulate_contract?: boolean;
}

export interface ProofValidationResult {
  valid: boolean;
  proofHash: string;
  verificationTimeMs: number;
  cached: boolean;
  error?: string;
  contractSimulationReady: boolean;
  publicInputsCount: number;
}

export interface ProofVerificationResponse {
  success: boolean;
  result?: ProofValidationResult;
  message?: string;
}

export class ProofVerificationService {
  private static instance: ProofVerificationService;
  private client: AxiosInstance;

  private constructor(client: AxiosInstance) {
    this.client = client;
  }

  static getInstance(): ProofVerificationService {
    if (!ProofVerificationService.instance) {
      ProofVerificationService.instance = new ProofVerificationService(
        httpClient,
      );
    }
    return ProofVerificationService.instance;
  }

  async verifyProof(
    request: ProofVerificationRequest,
  ): Promise<ProofVerificationResponse> {
    try {
      const response = await this.client.post<ProofVerificationResponse>(
        "/proof/verify",
        request,
      );
      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const message =
          error.response?.data?.detail ||
          error.message ||
          "Proof verification service error";
        return {
          success: false,
          message,
        };
      }
      return {
        success: false,
        message: error instanceof Error ? error.message : "Unknown error",
      };
    }
  }

  async verifyProofBatch(
    requests: ProofVerificationRequest[],
  ): Promise<{ success: boolean; results: ProofValidationResult[] }> {
    try {
      const response = await this.client.post<{
        success: boolean;
        results: ProofValidationResult[];
      }>("/proof/verify-batch", requests);
      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return {
          success: false,
          results: [],
        };
      }
      return {
        success: false,
        results: [],
      };
    }
  }

  async checkHealth(): Promise<boolean> {
    try {
      const response = await this.client.get("/health");
      return response.data.success === true;
    } catch {
      return false;
    }
  }
}

export const proofVerificationService = ProofVerificationService.getInstance();
