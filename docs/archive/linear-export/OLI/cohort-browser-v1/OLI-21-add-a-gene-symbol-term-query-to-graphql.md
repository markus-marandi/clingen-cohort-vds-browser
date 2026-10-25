# OLI-21: Add a gene_symbol term query to GraphQL

- Status: Implemented in main
- Exported status: Backlog
- Reviewed: 2026-10-25
- Priority: High
- Estimate: 3
- Labels: graphql, browser
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-21/add-a-gene-symbol-term-query-to-graphql

## Current assessment

ce2823a adds direct gene_symbol term queries and wires gene-symbol and multi-gene filtering into cohort GraphQL. This does not implement sample carrier lists; those remain OLI-23/26.

This is a repository assessment, not a live Linear update. See
[current implementation status](../../../IMPLEMENTATION_STATUS.md) for the
release scope and evidence boundaries.

## Original exported description

The gene query currently resolves via region overlap from the gene table, not a gene_symbol term filter. Needed for UC-2 and as the base for the multi-gene use cases. See browser/TODO.md > GraphQL.

## Comments

None.
