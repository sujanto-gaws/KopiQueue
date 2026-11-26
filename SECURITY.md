# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for security problems.
Report them privately through GitHub's **"Report a vulnerability"** button (Security → Advisories) on this repository.

Please include: affected versions, the database (PostgreSQL / SQL Server and version), reproduction steps, and the impact you expect.

We aim to acknowledge reports within 3 working days and to agree on a disclosure timeline with you.

## Supported versions

Before 1.0, only the latest preview release receives fixes. After 1.0, the latest minor version of each supported major version does.

## Scope notes

Areas of particular interest are SQL construction (NFR-SEC-01), args deserialisation (NFR-SEC-02) and data written to job error history (NFR-SEC-03).
See `docs/01-requirements.md` §7.5.
