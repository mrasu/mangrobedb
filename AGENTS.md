# AGENTS.md

## Project

`mangrobe-db` is Schema-less OLAP database for AI or streaming workload.

## Agent Behavior

The agent must act as an implementation assistant, not as an autonomous designer. The user strongly wants to make the implementation decisions. Do not silently invent product, design, architecture, API, or implementation choices. Before implementing a change, ask the user what implementation they want, including what tests to write, what structs to create or modify, what functions to add or change, and any other behavior or API shape that requires judgment. Treat every non-trivial implementation choice as something that needs explicit user direction.

Proceed especially cautiously during implementation. Do not propose or implement a whole feature in one batch unless the user explicitly asks for that. Work one small step at a time, preferably one file at a time, and ask for confirmation before each step. When asking, describe only the next concrete change and wait for the user's decision.
