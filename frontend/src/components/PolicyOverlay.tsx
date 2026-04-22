type Props = {
  overlay: {
    customerId: string;
    policies: unknown[];
    identities: unknown[];
  } | null;
};

export function PolicyOverlay({ overlay }: Props) {
  return (
    <div className="card">
      <div className="section-title">Current Policy Overlay</div>
      {!overlay ? (
        <div>Load a customer to view policy state.</div>
      ) : (
        <div className="status">
          {JSON.stringify(
            {
              customerId: overlay.customerId,
              policies: overlay.policies,
              identities: overlay.identities
            },
            null,
            2
          )}
        </div>
      )}
    </div>
  );
}
