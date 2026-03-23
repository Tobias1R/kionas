<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->
- [x] Verify that the copilot-instructions.md file in the .github directory is created.

- [x] Clarify Project Requirements
- [x] Scaffold the Project
- [x] Customize the Project
- [x] Install Required Extensions
- [ ] Compile the Project
- [ ] Create and Run Task
- [ ] Launch the Project
- [x] Ensure Documentation is Complete

## Execution Guidelines
- Work through each checklist item systematically.
- Keep communication concise and focused.
- Follow development best practices.

# Rust Development Rules for AI Agents

## When Creating New Code (Files, Functions, Methods, Enums)
- Always check cyclomatic complexity < 25
- Always check data flow complexity < 25
- Always add rust docs to classes/methods/functions (What, Inputs, Output, Details)
- Always ask for input on dualities. Never assume!

## When Fixing Bugs/Issues
- Check deeply what the issue is
- Create tests that fail for the specific issue
- Run the created tests: it should fail, if not adjust the test
- Solve the issue
- Check if test passes, if not continue trying other solutions

## Always Run After Changes
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
- cargo check


## Cargo Check runs
- project is taking up to 5 minutes to compile, so we want to make sure we are only running it when necessary

## Cargo Clippy Configuration
Check with cargo clippy after adding a new feature and fix clippy errors with the following settings:
```toml
[lints.clippy]
# Enable cognitive complexity lint to catch overly complex functions
cognitive_complexity = "warn"
pedantic = { level = "deny", priority = -1 }
nursery = { level = "deny", priority = -1 }
unwrap_used = "deny"
```

Additional settings in `clippy.toml`:
- `cognitive-complexity-threshold = 25`
- `too-many-lines-threshold = 150`

## Code Quality Requirements

### Pre-commit Checklist
Before completing any task, ensure all of the following pass:
1. **Format code**: `cargo fmt --all` (must produce no changes)
2. **Lint with Clippy**: `cargo clippy --all-targets --all-features -- -D warnings` (must be clean)
3. **Check compilation**: `cargo check` (must compile successfully)
4. **Run tests**: Tests are taking too long. Ignore for now.
5. **Check complexity**: For new code, verify cyclomatic complexity < 25 and data flow complexity < 25

### Code Documentation Requirements
For all new code (functions, methods, structs, enums):
- **Always add Rust documentation comments** with the following format:
  ```rust
  /// What: Brief description of what the function does.
  ///
  /// Inputs:
  /// - `param1`: Description of parameter 1
  /// - `param2`: Description of parameter 2
  ///
  /// Output:
  /// - Description of return value or side effects
  ///
  /// Details:
  /// - Additional context, edge cases, or important notes
  pub fn example_function(param1: Type1, param2: Type2) -> Result<Type3> {
      // implementation
  }
  ```
- Documentation must include: **What**, **Inputs**, **Output**, and **Details** sections


## Code Style Conventions
- **Language**: Rust (edition 2024)
- **Naming**: Clear, descriptive names. Favor clarity over brevity.
- **Error handling**: Use `Result` types. Never use `unwrap()` or `expect()` in non-test code.
- **Early returns**: Prefer early returns over deep nesting.
- **Logging**: Use `tracing` for diagnostics. Avoid noisy logs at info level.

## Platform Behavior Requirements

### Error Messages
- Always provide clear, actionable error messages
- Error messages should help users understand what went wrong and how to fix it

## Configuration Updates
If you add or change config keys:
- Update `config/*.json*`
- Ensure backward compatibility when possible
- Do NOT update wiki pages or README unless explicitly asked (see General Rules)
- All binaries must use kionas::config

## Documentation Updates
- **Do NOT create or update *.md files, unless explicitly asked**
- **Do NOT update wiki pages, unless explicitly asked**
- **Do NOT update README, unless explicitly asked**
- **ROADMAPS** Are the only exception: update `ROADMAP.md` and create corresponding `ROADMAP_PHASEX_MATRIX.md` files as needed for phase signoff documentation.
- Focus on code implementation and inline documentation (rustdoc comments)


## Complexity Thresholds
- **Cyclomatic complexity**: Must be < 25 for all new functions
- **Data flow complexity**: Must be < 25 for all new functions
- **Function length**: Should not exceed 150 lines (too-many-lines-threshold)

## General Rules
- Do not create *.md files, unless explicitly asked
- Do not update wiki pages, unless explicitly asked
- Do not update README, unless explicitly asked
- Focus on code quality, tests, and inline documentation
- All changes must respect `--dry-run` flag
- All changes must degrade gracefully if system tools are unavailable

## Roadmping
- We build our plans usig the roadmap format in `ROADMAP.md` and track phase completion with corresponding `ROADMAP_PHASEX_MATRIX.md` files.
- Each roadmap phase should have a clear scope, mandatory criteria for completion, optional hardening items, and a signoff decision process.
- The roadmap should be updated iteratively as we progress through phases, ensuring that all relevant files and documentation are maintained in sync with the current state of the project.
- Each phase is composed by:
  - **DISCOVERY**:
    - From now on we are going to persist our discovery findings in roadmaps/<ROADMAP_NAME>/discovery/discovery-phaseX.md files, and link them in the discovery section of the roadmap.
  - **IMPLEMENTATION**:
    - save planning in roadmaps/<ROADMAP_NAME>/plans/plan-phaseX.md files, and link them in the implementation section of the roadmap.
  - **COMPLETION MATRIX**:
    - For each phase, create a ROADMAP_PHASEX_MATRIX.md file with the structure defined in the template below. This file will be used to track the completion of mandatory criteria and optional hardening items for the phase, along with evidence references and notes.

## Roadmap Phase Signoff Matrix Requirements
For each roadmap phase, create a corresponding `ROADMAP_PHASEX_MATRIX.md` file with the following structure:
```markdown
# Phase X Completion Matrix

## Scope
Describe the exact phase scope from [ROADMAP.md](ROADMAP.md).

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Mandatory criterion 1 | Not Started | N/A | |
| Mandatory criterion 2 | Not Started | N/A | |
| Mandatory criterion 3 | Not Started | N/A | |
| Optional hardening 1 | Deferred | N/A | Non-blocking for phase signoff. |
| Optional hardening 2 | Deferred | N/A | Non-blocking for phase signoff. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Pending`
- Blocking items:
  - List mandatory criteria not marked `Done`.
  - List optional hardening items not marked `Done`.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Inform all environment variables required for the phase, if any. And where in the codebase they are expected to be used.
- Inform any parameters that are expected to be used for the phase, if any. And where in the codebase they are expected to be used.
```

## Silk Road Methodology
- Silk road discovery and implementation files should be created in `roadmaps/SILK_ROAD/` with the following structure:
  - `roadmaps/SILK_ROAD/silkroad.md`: This file will contain the overall Silk Road methodology, including the roadmap to indexing and constraints.
  - `roadmaps/SILK_ROAD/<ROAD_NAME>/discovery/discovery-<ROAD_NAME>-<PHASE>.md`: This file will capture the discovery findings for the specific road (e.g., indexing, constraints). Must contains high-level summaries of blockers, technical deep dives, and any relevant diagrams or references. No code is produced in this phase, but the output should be only the discovery findings and the next steps for implementation.
  - Once discovery is completed, we build a roadmap for the ROAD_NAME.  
  - A plan must be created in `roadmaps/SILK_ROAD/<ROAD_NAME>/plans/plan-<ROAD_NAME>-<PHASE>.md` and linked in the discovery file on the next iteration after the discovery is completed.
