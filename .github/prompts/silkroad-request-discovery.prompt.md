---
agent: agent
description: This prompt is used to generate a discovery document.
model: Claude Haiku 4.5
tools: [execute, read, edit, search, web, agent, todo]
---

Lets investigate and create the discovery artifact for the next phase on our roadmap_execution. No coding for now. This exploration must generate the requested discovery artifact following this template:



Generate a discovery document for the next phase of our roadmap_execution. The discovery should include the following sections:

# Plan: EXECUTION EP-2

## Project coding standards and guidelines
1. tests go under the folder src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.

## Goal
Describe the specific goal of this phase in clear and measurable terms.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/<ROADMAP_NAME>/discovery/discovery-EXECUTION-EP<SEQUENCE>.md](../discovery/discovery-EXECUTION-EP<SEQUENCE>.md)
2. Roadmap: [roadmaps/SILK_ROAD/<ROADMAP_NAME>/ROADMAP.md](../ROADMAP.md)
3. Matrix target: [roadmaps/<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md](../../../<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/<ROADMAP_NAME>_EP<PREVIOUS_SEQUENCE>_MATRIX.md](../../../<ROADMAP_NAME>_EP<PREVIOUS_SEQUENCE>_MATRIX.md)

## Scope Boundaries
In scope:


Out of scope:
- 

## Guardrails
- Enumerate any guardrails or constraints that should be observed during the execution of this phase.

## Workstreams

### Workstream <SEQUENCE>: A Clean title for each workstream
Objectives:
- Enumerated objectives for this workstream.

Tasks:
- Enumerated tasks required tasks

Deliverables:
- enumerate deliverables for this workstream.

## Canonical Mode Decision Table

## Criteria-To-Workstream Traceability


## Sequence And Dependencies
- Enumerate the logical sequence of workstreams and tasks, noting any dependencies or prerequisites between them. For example:

## Milestones
- Enumerate key milestones for each workstream, such as:
1. Workstream A: Truth model defined and legacy message removal checklist by [date].

## Risks And Mitigations
- Enumerate Risks and Mitigations for each workstream, with a focus on cross-cutting risks that could impact multiple criteria or the overall phase timeline.

## E2E Scenarios
- Provide a high level description of the critical end-to-end scenarios expected in this phase, along with expected outcomes and any specific edge cases that should be covered.


## Matrix Packaging Checklist
1. Create/update [roadmaps/<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md](../../../<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md).
2. For each mandatory criterion, include concrete evidence references.
3. Mark deferred optional hardening with explicit non-blocking rationale.
4. Record final EP-<SEQUENCE> signoff decision in the matrix file.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/<ROADMAP_NAME>/discovery/discovery-EXECUTION-EP<SEQUENCE>.md](../discovery/discovery-EXECUTION-EP<SEQUENCE>.md)
2. Roadmap: [roadmaps/SILK_ROAD/<ROADMAP_NAME>/ROADMAP.md](../ROADMAP.md)
3. Matrix target: [roadmaps/<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md](../../../<ROADMAP_NAME>_EP<SEQUENCE>_MATRIX.md)
