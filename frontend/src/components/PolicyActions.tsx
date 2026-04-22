type Props = {
  onCreate: () => void;
  onUpdate: () => void;
  onDelete: () => void;
  onDryRun: () => void;
  disabled?: boolean;
};

export function PolicyActions({ onCreate, onUpdate, onDelete, onDryRun, disabled }: Props) {
  return (
    <div className="card">
      <div className="section-title">Policy Actions</div>
      <div className="row">
        <button onClick={onCreate} disabled={disabled}>
          Create
        </button>
        <button onClick={onUpdate} disabled={disabled}>
          Update
        </button>
        <button onClick={onDryRun} disabled={disabled}>
          Dry Run
        </button>
        <button onClick={onDelete} disabled={disabled}>
          Delete
        </button>
      </div>
    </div>
  );
}
