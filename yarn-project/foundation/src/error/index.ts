/**
 * Represents an error thrown when an operation is interrupted unexpectedly.
 * This custom error class extends the built-in Error class in JavaScript and
 * can be used to handle cases where a process or task is terminated before completion.
 */
export class InterruptError extends Error {
  public override readonly name = 'InterruptError';
}

/**
 * An error thrown when an action times out.
 */
export class TimeoutError extends Error {
  public override readonly name = 'TimeoutError';
}

/**
 * Represents an error thrown when an operation is aborted.
 */
export class AbortError extends Error {
  public override readonly name = 'AbortError';
}

/**
 * Throws a fresh {@link AbortError} with no `cause` if `signal` has aborted. Unlike `AbortSignal#throwIfAborted`, it
 * never throws `signal.reason`: code the error crosses may assign its `message`, which a `DOMException` reason forbids,
 * or walk its `cause` chain. The caller's boundary maps the error back to the reason.
 */
export function throwAbortErrorIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw new AbortError('Aborted by caller');
  }
}

/**
 * `err` followed by each `cause` below it, outermost first. Follows any object that has a `cause`, and stops at a value
 * without one or at a value already listed, so a cyclic chain ends.
 */
export function causeChain(err: unknown): unknown[] {
  const chain: unknown[] = [];
  let current = err;
  while (!chain.includes(current)) {
    chain.push(current);
    if (typeof current !== 'object' || current === null || !('cause' in current)) {
      break;
    }
    current = current.cause;
  }
  return chain;
}

/**
 * An error caused by the environment rather than by the request: a process that died or could not be started, or a
 * failed system call. Retrying may succeed. Satisfies {@link isRetryableError}.
 */
export class RetryableError extends Error {
  public override readonly name = 'RetryableError';
  public readonly retry = true;
}

/**
 * Whether an error is a Node system error, one carrying `syscall` (such as ENOSPC or EMFILE): the machine failed rather
 * than the request, so retrying may succeed. Checked structurally because errors from Node's own APIs can come from
 * another realm and fail `instanceof Error`.
 */
export function isSystemError(err: unknown): err is { syscall: string } {
  return typeof err === 'object' && err !== null && 'syscall' in err && typeof err.syscall === 'string';
}

/**
 * Whether a failure was environmental, and so may be retried: the process behind a call died, its
 * connection broke, or it could not be started.
 *
 * The bare `retry` property is the contract, feature-detected rather than imported, so the same check
 * holds across bb.js, ipc-runtime and ProvingError. An error without it failed for a reason retrying
 * cannot fix.
 */
export function isRetryableError(err: unknown): boolean {
  return err instanceof Error && 'retry' in err && err.retry === true;
}
