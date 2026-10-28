# Security Policy

## Supported Versions

Only the latest released version receives security fixes.

## Reporting a Vulnerability

Please do not open a public issue for security problems. Report them privately through
GitHub's Private Vulnerability Reporting:

1. Open the repository on GitHub.
2. Go to the **Security** tab.
3. Click **Report a vulnerability** and describe the issue.

Include the affected version, steps to reproduce, and the expected and actual behavior. You will
get an acknowledgement, and the report stays private until a fix is released.

## Scope

In scope:

- Violations of the ordering guarantee, such as two tasks of the same key running at the same
  time, or a task starting before the previous task of its key has returned.
- Bypasses of the concurrency limit or the pending limit.
- Goroutine, timer or memory leaks that an untrusted caller can trigger, for example through
  keys or cancelled contexts.
- Panics or data races caused by the library itself.

Out of scope:

- Using one queue across several processes or instances. This is documented as unsupported. Use
  a distributed lock for that.
- Tasks that ignore their context. The queue cannot stop them.
