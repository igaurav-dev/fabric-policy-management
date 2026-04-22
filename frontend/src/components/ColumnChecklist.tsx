import type { MetadataColumn } from "../api/client";

type Props = {
  columns: MetadataColumn[];
  selectedColumns: string[];
  onChange: (columns: string[]) => void;
};

export function ColumnChecklist({ columns, selectedColumns, onChange }: Props) {
  const toggle = (name: string) => {
    if (selectedColumns.includes(name)) {
      onChange(selectedColumns.filter((c) => c !== name));
      return;
    }
    onChange([...selectedColumns, name]);
  };

  const toggleAll = (checked: boolean) => {
    onChange(checked ? columns.map((c) => c.columnName) : []);
  };

  return (
    <div className="card">
      <div className="section-title">Column Access (CLS)</div>
      <div className="row">
        <label>
          <input
            type="checkbox"
            checked={columns.length > 0 && selectedColumns.length === columns.length}
            onChange={(e) => toggleAll(e.target.checked)}
          />{" "}
          Select all
        </label>
      </div>
      <div className="scroll">
        {columns.map((col) => (
          <div key={col.columnName} className="row">
            <label>
              <input
                type="checkbox"
                checked={selectedColumns.includes(col.columnName)}
                onChange={() => toggle(col.columnName)}
              />{" "}
              {col.columnName} ({col.dataType})
            </label>
          </div>
        ))}
        {columns.length === 0 && <div>No columns available.</div>}
      </div>
    </div>
  );
}
