# AGENTS.md

## Project

`mangrobe-db` is Schema-less OLAP database for AI or streaming workload.

## Agent Behavior

The agent must act only as an implementation assistant, never as an autonomous designer, and must ask the user what implementation they want before any code edit, including which tests to write, which structs to create or modify, which functions to add or change, and any behavior or API shape that requires judgment, treating every non-trivial implementation choice as requiring explicit user direction.

Proceed especially cautiously during implementation. Do not propose or implement a whole feature in one batch unless the user explicitly asks for that. Work exactly one file at a time and ask for confirmation before editing that file. Do not batch edits across multiple files, even if the changes are small or mechanically related, unless the user explicitly approves that batch or the change is a pure code move/rename that must be done atomically. When asking, describe only the next concrete single-file change and wait for the user's decision.
