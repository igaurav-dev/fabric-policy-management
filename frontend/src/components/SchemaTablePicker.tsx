import type { MetadataSchema } from "../api/client";

type Props = {
  metadata: MetadataSchema[];
  schemaName: string;
  tableName: string;
  onSchemaChange: (schema: string) => void;
  onTableChange: (table: string) => void;
};

export function SchemaTablePicker({
  metadata,
  schemaName,
  tableName,
  onSchemaChange,
  onTableChange
}: Props) {
  const selectedSchema = metadata.find((s) => s.schemaName === schemaName);
  const tables = selectedSchema?.tables ?? [];

  return (
    <div className="card">
      <div className="section-title">Schema / Table</div>
      <div className="row">
        <label>Schema</label>
        <select value={schemaName} onChange={(e) => onSchemaChange(e.target.value)}>
          <option value="">Select schema</option>
          {metadata.map((schema) => (
            <option key={schema.schemaName} value={schema.schemaName}>
              {schema.schemaName}
            </option>
          ))}
        </select>
      </div>
      <div className="row">
        <label>Table</label>
        <select value={tableName} onChange={(e) => onTableChange(e.target.value)}>
          <option value="">Select table</option>
          {tables.map((table) => (
            <option key={table.tableName} value={table.tableName}>
              {table.tableName}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
