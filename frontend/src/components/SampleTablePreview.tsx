type Props = {
  rows: Array<Record<string, unknown>>;
};

export function SampleTablePreview({ rows }: Props) {
  const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
  return (
    <div className="card">
      <div className="section-title">Sample Rows</div>
      <div className="scroll">
        {rows.length === 0 ? (
          <div>No sample rows loaded.</div>
        ) : (
          <table>
            <thead>
              <tr>
                {headers.map((h) => (
                  <th key={h}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={`r-${i}`}>
                  {headers.map((h) => (
                    <td key={`${i}-${h}`}>{String(row[h] ?? "")}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
