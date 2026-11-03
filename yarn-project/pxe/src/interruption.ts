import { AbortError } from '@aztec-labs/foundation/error';

/**
 * Throws if `signal` has aborted. The error is always a fresh `AbortError` with no `cause`, never `signal.reason`: the
 * private execution path assigns `message` on errors that cross it (a caller's `DOMException` forbids that), and the
 * simulator walks `cause` chains without a cycle guard, so a caller-supplied reason must not travel inside the work.
 * The operation queue maps this error back to `signal.reason` for the caller.
 */
export function throwIfInterrupted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw new AbortError('Interrupted by caller');
  }
}
