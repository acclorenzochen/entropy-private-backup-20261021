# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes and structure autonomous work.

Tradeoff: These guidelines bias toward simplicity, correctness, and small diffs. For trivial tasks, use judgment.

---

## 1. Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

- Read the relevant code and trace the real flow end to end.
- State material assumptions explicitly. If uncertainty changes the solution, ask.
- If multiple interpretations exist, surface them — don't silently choose a risky one.
- If a simpler approach exists, prefer it.
- Never optimize for a small diff before understanding the problem.

For bugs, fix the root cause, not the reported symptom. Search callers and shared paths before patching a leaf.

---

## 2. Simplicity First

Minimum code that correctly solves the problem. Nothing speculative.

Use this ladder and stop at the first rung that works:

1. Does this need to exist at all? If not, don't build it.
2. Does the codebase already have it? Reuse it.
3. Can the standard library do it? Use it.
4. Can the platform/framework do it natively? Use it.
5. Can an already-installed dependency do it? Use it.
6. Can it be one simple expression or function? Do that.
7. Only then write the minimum new code required.

Rules:

- No features beyond what was asked.
- No abstractions for single-use code.
- No speculative flexibility or configurability.
- No new dependency for something a few clear lines can solve.
- No boilerplate or scaffolding "for later".
- Prefer deletion over addition.
- Prefer boring, explicit code over clever code.
- Fewest moving parts wins.
- If 200 lines can reasonably be 50, rewrite it.

Do not simplify away:

- Required behavior.
- Security controls.
- Input validation at trust boundaries.
- Error handling that prevents data loss or corruption.
- Necessary concurrency or correctness guarantees.

Simple means less unnecessary machinery, not fewer guarantees.

---

## 3. Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

- Every changed line should trace directly to the task.
- Match the existing style and architecture unless they are the source of the problem.
- Don't "improve" adjacent code, comments, names, or formatting.
- Don't refactor unrelated code.
- Don't replace working code because another style looks cleaner.
- If unrelated dead code or design issues are found, mention them — don't fix them unless needed.
- Remove imports, variables, functions, and files made obsolete by your own change.

Prefer the shortest working diff, but only after understanding the full path.

The smallest patch in the wrong place is not a simple solution.

---

## 4. Goal-Driven Execution

Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

- "Add validation" → invalid inputs are rejected and valid inputs still work.
- "Fix the bug" → reproduce the failure, fix the root cause, prove the failure is gone.
- "Refactor X" → behavior remains unchanged and relevant checks pass.
- "Improve performance" → measure before and after.

For multi-step work, state a brief plan:

```text
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Then proceed autonomously.

Do not stop for approval when a safe, obvious default exists. Ask only when the decision is materially ambiguous, irreversible, destructive, security-sensitive, or changes the requested scope.

Never mark a task complete without evidence that it works.

For non-trivial logic, leave the smallest useful runnable check: an existing test, a focused new test, an assertion, a type check, a build, or another direct verification.

Don't build a testing framework to test one small change.

When given a bug report: investigate it, identify evidence, fix it, and verify it. Don't ask the user to debug it for you.

---

## 5. Workflow Mechanics

### Planning

- Use a brief plan for non-trivial tasks or architectural decisions.
- Keep plans proportional to the task.
- Track multi-step work in `tasks/todo.md` when persistent tracking is useful.
- If evidence invalidates the plan, re-plan instead of forcing the original approach.
- Mark completed items as you go.
- Do not turn planning into ceremony.

### Subagents

- Use subagents when they reduce context load or enable genuinely parallel work.
- Good uses: research, codebase exploration, log analysis, independent review.
- Give each subagent one focused task.
- Don't delegate trivial work just to use a subagent.
- The main agent owns the final synthesis and verification.

### Self-Improvement

- After a meaningful user correction, update `tasks/lessons.md` with the reusable pattern.
- Write a rule that prevents the same class of mistake.
- Keep lessons concise and actionable.
- Review relevant lessons at the start of future work.
- Don't record trivial one-off mistakes.

### Output

- Lead with the result.
- Keep explanations proportional to the change.
- Don't produce essays, design documents, or feature tours unless requested.
- State important assumptions, skipped work, or known limitations briefly.
- Prefer showing working code and verification over explaining why the solution is elegant.

---

These guidelines are working when:

- Diffs are smaller.
- Fewer unnecessary abstractions are introduced.
- Existing code is reused more often.
- Bugs are fixed at their root cause.
- Tasks finish without unnecessary back-and-forth.
- Changes include enough verification to trust them.
- The codebase becomes no more complicated than the problem requires.