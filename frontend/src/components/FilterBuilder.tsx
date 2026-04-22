import type { MetadataColumn } from "../api/client";

type Filter = {
  column: string;
  operator: string;
  value: string;
};

type Props = {
  fields: MetadataColumn[];
  filter: Filter;
  onChange: (filter: Filter) => void;
};

const OPERATORS = ["=", "!=", "<>", ">", "<", ">=", "<="];

export function FilterBuilder({ fields, filter, onChange }: Props) {
  return (
    <div className="card">
      <div className="section-title">Row Filter (RLS)</div>
      <div className="row">
        <label>Column</label>
        <select
          value={filter.column}
          onChange={(e) => onChange({ ...filter, column: e.target.value })}
        >
          <option value="">Select column</option>
          {fields.map((f) => (
            <option key={f.columnName} value={f.columnName}>
              {f.columnName}
            </option>
          ))}
        </select>
      </div>
      <div className="row">
        <label>Operator</label>
        <select
          value={filter.operator}
          onChange={(e) => onChange({ ...filter, operator: e.target.value })}
        >
          {OPERATORS.map((op) => (
            <option key={op} value={op}>
              {op}
            </option>
          ))}
        </select>
      </div>
      <div className="row">
        <label>Value</label>
        <input
          value={filter.value}
          onChange={(e) => onChange({ ...filter, value: e.target.value })}
          placeholder="A"
        />
      </div>
    </div>
  );
}
