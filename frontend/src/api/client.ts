export type ApiEnvelope<T> = {
  success: boolean;
  message: string;
  details: T;
  correlationId: string;
};

export type IdentityMapping = {
  oid: string;
  upn?: string;
};

export type PolicyPayload = {
  customerId?: string;
  schemaName: string;
  tableName: string;
  allowedColumns: string[];
  rowFilter?: {
    column: string;
    operator: string;
    value: string;
  };
  tableAccess: boolean;
  identities: IdentityMapping[];
};

export type MetadataColumn = {
  columnName: string;
  dataType: string;
  ordinalPosition: number;
};

export type MetadataTable = {
  tableName: string;
  columns: MetadataColumn[];
};

export type MetadataSchema = {
  schemaName: string;
  tables: MetadataTable[];
};

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL?.toString().trim() || "http://localhost:7071/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {})
    },
    ...init
  });
  const data = (await response.json()) as ApiEnvelope<T>;
  if (!response.ok || !data.success) {
    throw new Error(data.message || "API request failed");
  }
  return data.details;
}

export const apiClient = {
  async createPolicy(payload: PolicyPayload) {
    return request<{ executed: boolean; statementCount: number }>("/policies", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  },

  async updatePolicy(customerId: string, payload: PolicyPayload) {
    return request<{ executed: boolean; statementCount: number }>(`/policies/${customerId}`, {
      method: "PUT",
      body: JSON.stringify(payload)
    });
  },

  async deletePolicy(customerId: string) {
    return request<{ executed: boolean; statementCount: number }>(`/policies/${customerId}`, {
      method: "DELETE"
    });
  },

  async dryRunPolicy(customerId: string, payload: PolicyPayload) {
    return request<{ executed: boolean; statements: Array<{ sql: string; params: unknown[] }> }>(
      `/policies/${customerId}/dry-run`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      }
    );
  },

  async getPolicy(customerId: string) {
    return request<{ customerId: string; policies: unknown[]; identities: unknown[] }>(
      `/policies/${customerId}`
    );
  },

  async getMetadata() {
    return request<{ schemas: MetadataSchema[] }>("/introspect/metadata");
  },

  async getSampleRows(schemaName: string, tableName: string, top = 20) {
    return request<{ schemaName: string; tableName: string; top: number; rows: unknown[] }>(
      `/introspect/sample?schemaName=${encodeURIComponent(schemaName)}&tableName=${encodeURIComponent(tableName)}&top=${top}`
    );
  },

  async getPolicyOverlay(customerId: string) {
    return request<{ customerId: string; policies: unknown[]; identities: unknown[] }>(
      `/introspect/policy-overlay/${encodeURIComponent(customerId)}`
    );
  },

  async getFilterFields(schemaName: string, tableName: string) {
    return request<{ schemaName: string; tableName: string; fields: MetadataColumn[] }>(
      `/introspect/filter-fields?schemaName=${encodeURIComponent(schemaName)}&tableName=${encodeURIComponent(tableName)}`
    );
  }
};
