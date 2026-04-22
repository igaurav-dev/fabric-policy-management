import { useEffect, useMemo, useState } from "react";
import { apiClient, type MetadataColumn, type MetadataSchema, type PolicyPayload } from "./api/client";
import { ColumnChecklist } from "./components/ColumnChecklist";
import { FilterBuilder } from "./components/FilterBuilder";
import { PolicyActions } from "./components/PolicyActions";
import { PolicyOverlay } from "./components/PolicyOverlay";
import { SampleTablePreview } from "./components/SampleTablePreview";
import { SchemaTablePicker } from "./components/SchemaTablePicker";

type Overlay = {
  customerId: string;
  policies: unknown[];
  identities: unknown[];
} | null;

function App() {
  const [metadata, setMetadata] = useState<MetadataSchema[]>([]);
  const [customerId, setCustomerId] = useState("A");
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAccess, setTableAccess] = useState(true);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [filterFields, setFilterFields] = useState<MetadataColumn[]>([]);
  const [filter, setFilter] = useState({ column: "CustomerID", operator: "=", value: "A" });
  const [identityOid, setIdentityOid] = useState("");
  const [identityUpn, setIdentityUpn] = useState("");
  const [sampleRows, setSampleRows] = useState<Array<Record<string, unknown>>>([]);
  const [overlay, setOverlay] = useState<Overlay>(null);
  const [status, setStatus] = useState("Ready.");
  const [busy, setBusy] = useState(false);

  const selectedTable = useMemo(() => {
    const schema = metadata.find((s) => s.schemaName === schemaName);
    return schema?.tables.find((t) => t.tableName === tableName);
  }, [metadata, schemaName, tableName]);

  useEffect(() => {
    const loadMetadata = async () => {
      try {
        const result = await apiClient.getMetadata();
        setMetadata(result.schemas);
      } catch (error) {
        setStatus(`Metadata load failed: ${(error as Error).message}`);
      }
    };
    void loadMetadata();
  }, []);

  useEffect(() => {
    setSelectedColumns([]);
    setSampleRows([]);
    if (!schemaName || !tableName) {
      setFilterFields([]);
      return;
    }

    const loadForTable = async () => {
      try {
        const [fieldResult, sampleResult] = await Promise.all([
          apiClient.getFilterFields(schemaName, tableName),
          apiClient.getSampleRows(schemaName, tableName, 20)
        ]);
        setFilterFields(fieldResult.fields);
        setSampleRows((sampleResult.rows as Array<Record<string, unknown>>) ?? []);
        if (fieldResult.fields.length > 0 && !fieldResult.fields.some((f) => f.columnName === filter.column)) {
          setFilter((prev) => ({ ...prev, column: fieldResult.fields[0].columnName }));
        }
      } catch (error) {
        setStatus(`Table load failed: ${(error as Error).message}`);
      }
    };

    void loadForTable();
  }, [schemaName, tableName, filter.column]);

  const refreshOverlay = async () => {
    if (!customerId) {
      return;
    }
    try {
      const result = await apiClient.getPolicyOverlay(customerId);
      setOverlay(result);
    } catch (error) {
      setStatus(`Overlay load failed: ${(error as Error).message}`);
    }
  };

  const buildPayload = (): PolicyPayload => {
    if (!schemaName || !tableName) {
      throw new Error("Please select schema and table.");
    }
    if (!customerId) {
      throw new Error("customerId is required.");
    }
    if (!identityOid) {
      throw new Error("Identity OID is required.");
    }
    return {
      customerId,
      schemaName,
      tableName,
      allowedColumns: selectedColumns,
      rowFilter: filter.column ? { ...filter } : undefined,
      tableAccess,
      identities: [{ oid: identityOid, upn: identityUpn || undefined }]
    };
  };

  const runAction = async (action: () => Promise<unknown>, doneMessage: string) => {
    setBusy(true);
    try {
      const result = await action();
      setStatus(`${doneMessage}\n${JSON.stringify(result, null, 2)}`);
      await refreshOverlay();
    } catch (error) {
      setStatus(`Action failed: ${(error as Error).message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <h1>Fabric Access Control Admin</h1>
      <div className="card">
        <div className="section-title">Customer + Identity</div>
        <div className="row">
          <label>Customer ID</label>
          <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} />
          <button onClick={() => void refreshOverlay()}>Load Overlay</button>
        </div>
        <div className="row">
          <label>Identity OID</label>
          <input value={identityOid} onChange={(e) => setIdentityOid(e.target.value)} />
        </div>
        <div className="row">
          <label>Identity UPN</label>
          <input value={identityUpn} onChange={(e) => setIdentityUpn(e.target.value)} />
        </div>
        <div className="row">
          <label>Table Access</label>
          <input
            type="checkbox"
            checked={tableAccess}
            onChange={(e) => setTableAccess(e.target.checked)}
          />
        </div>
      </div>

      <div className="layout">
        <SchemaTablePicker
          metadata={metadata}
          schemaName={schemaName}
          tableName={tableName}
          onSchemaChange={(schema) => {
            setSchemaName(schema);
            setTableName("");
          }}
          onTableChange={setTableName}
        />
        <PolicyOverlay overlay={overlay} />
        <ColumnChecklist
          columns={selectedTable?.columns ?? []}
          selectedColumns={selectedColumns}
          onChange={setSelectedColumns}
        />
        <FilterBuilder fields={filterFields} filter={filter} onChange={setFilter} />
        <SampleTablePreview rows={sampleRows} />
        <PolicyActions
          disabled={busy}
          onCreate={() =>
            void runAction(async () => apiClient.createPolicy(buildPayload()), "Policy created.")
          }
          onUpdate={() =>
            void runAction(
              async () => apiClient.updatePolicy(customerId, buildPayload()),
              "Policy updated."
            )
          }
          onDryRun={() =>
            void runAction(
              async () => apiClient.dryRunPolicy(customerId, buildPayload()),
              "Dry-run generated."
            )
          }
          onDelete={() =>
            void runAction(async () => apiClient.deletePolicy(customerId), "Policy deleted.")
          }
        />
      </div>

      <div className="card">
        <div className="section-title">Status</div>
        <div className="status">{status}</div>
      </div>
    </div>
  );
}

export default App;
