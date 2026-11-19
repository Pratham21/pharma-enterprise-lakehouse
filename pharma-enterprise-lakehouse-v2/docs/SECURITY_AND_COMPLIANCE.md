# Security and compliance boundaries

- All names, prescription events and product names are generated. No patient identities, actual pharmaceutical transactions or external connections are included.
- This demo does not certify HIPAA, GDPR, GxP or 21 CFR Part 11 compliance. Those controls require formal application-specific assessments, validation, policies and evidence.
- Use least-privilege Unity Catalog grants; grant access to approved groups only. See `sql/governance_example.sql` for an illustrative template.
- In production, evaluate regional residency, cross-border transfers, encryption at rest/in transit, KMS key management, protected network paths, service principals and secret handling.
- Never commit workspace URLs, cloud tokens, passwords, connection strings, personal records or private bucket names to GitHub.
- Delta time travel / regular event-history tables are not substitutes for immutable, validated, policy-governed regulated record retention.
- For agentic AI on governed data, enforce caller identity at query and retrieval layers, log tool actions, and obtain approval before production writes.
