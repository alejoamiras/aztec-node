import { AbortError } from '@aztec-labs/foundation/error';
import type { Logger } from '@aztec-labs/foundation/log';
import { promiseWithResolvers } from '@aztec-labs/foundation/promise';
import { SerialQueue } from '@aztec-labs/foundation/queue';
import { Timer } from '@aztec-labs/foundation/timer';
import { SimulationError } from '@aztec-labs/stdlib/errors';
import type { BlockHeader } from '@aztec-labs/stdlib/tx';

import type { BlockSynchronizer } from './block_synchronizer/index.js';
import type { Recording } from './node/benchmarked_node.js';
import type { CachingAztecNode } from './node/caching_aztec_node.js';
import { type OperationContributor, runOperation } from './operation_lifecycle.js';
import type { AnchorBlockStore } from './storage/anchor_block_store/anchor_block_store.js';
import type { ChangeSetId, StagedWriteCoordinator } from './storage/staged_write_coordinator.js';

/** What a synced operation receives: its change set id, the anchor its sync established, and its instrumentation. */
export type SyncedOperationContext = {
  /**
   * The change set the operation's writes are staged under. A synced operation runs inside exactly one change set,
   * which is committed if it succeeds and discarded if it fails, so this ID doubles as the operation's identity:
   * bookkeeping that must be kept or thrown away along with the operation is keyed on it, whether or not it lives in
   * a store.
   */
  changeSetId: ChangeSetId;
  /** Duration of the sync, for timing stats. */
  syncTime: number;
  anchorBlockHeader: BlockHeader;
  /** Open recording of the node RPC calls made so far in this operation; `stats()` snapshots them for reporting. */
  recording: Recording;
  /** The operation's duration so far, including the sync. */
  totalMs: () => number;
  /** The caller's signal; work checks it at its own checkpoints (see {@link OperationQueue.runSynced}). */
  signal?: AbortSignal;
};

/**
 * Serializes the PXE's operations, since concurrent execution is not supported: operations execute oracles that read
 * and write the PXE stores, and concurrent runs would interfere with one another.
 *
 * Synced operations additionally run after a sync with the node and inside a staged-write session (see
 * {@link StagedWriteCoordinator}): staged writes are committed if the operation succeeds and discarded if it throws.
 */
export class OperationQueue {
  private static readonly WARN_QUEUE_DEPTH = 10;
  private static readonly WARN_WAIT_MS = 30_000;
  private static readonly WARN_INTERVAL_MS = 60_000;
  private lastPressureWarningAt = -Infinity;
  private readonly queue = new SerialQueue();
  private readonly node: CachingAztecNode;
  private readonly synchronizer: BlockSynchronizer;
  private readonly anchorBlockStore: AnchorBlockStore;
  private readonly stagedWriteCoordinator: StagedWriteCoordinator;
  private readonly contributors: OperationContributor[];
  private readonly autoSync: boolean;
  private readonly log: Logger;

  constructor(args: OperationQueueArgs) {
    this.node = args.node;
    this.synchronizer = args.synchronizer;
    this.anchorBlockStore = args.anchorBlockStore;
    this.stagedWriteCoordinator = args.stagedWriteCoordinator;
    this.contributors = args.contributors;
    this.autoSync = args.autoSync;
    this.log = args.log;
  }

  public start(): void {
    this.queue.start();
  }

  public async stop(): Promise<void> {
    await this.queue.end();
  }

  /**
   * Runs an operation once no other operations are running. Returns a promise that will resolve once the operation
   * is complete.
   *
   * Useful for tasks that cannot run concurrently, such as contract function simulation.
   */
  public run<T>(fn: () => Promise<T>): Promise<T> {
    // TODO(#12636): relax the conditions under which we forbid concurrency.
    const queued = new Timer();
    const queueDepth = this.queue.length();
    if (queueDepth >= OperationQueue.WARN_QUEUE_DEPTH) {
      this.warnQueuePressure('depth', queueDepth, 0);
    }
    const waitWarning = setTimeout(() => {
      this.warnQueuePressure('wait', this.queue.length(), queued.ms());
    }, OperationQueue.WARN_WAIT_MS);

    return this.queue
      .put(() => {
        clearTimeout(waitWarning);
        this.log.debug('Starting queued PXE operation', {
          eventName: 'pxe_operation_started',
          queueDepth,
          waitMs: queued.ms(),
        });
        return fn();
      })
      .finally(() => clearTimeout(waitWarning));
  }

  private warnQueuePressure(reason: 'depth' | 'wait', queueDepth: number, waitMs: number): void {
    const now = Date.now();
    if (now - this.lastPressureWarningAt < OperationQueue.WARN_INTERVAL_MS) {
      return;
    }
    this.lastPressureWarningAt = now;
    this.log.warn('PXE operation queue is congested', {
      eventName: 'pxe_operation_queue_pressure',
      reason,
      queueDepth,
      waitMs,
    });
  }

  /**
   * Runs an operation (`fn`) after a sync with the node (skipped when the `autoSync` config flag is disabled, unless
   * `forceSync` is set). If the operation succeeds, then all staged writes are committed. If it rejects, then all
   * staged writes are discarded.
   *
   * When `signal` aborts, the call rejects with `signal.reason`. A queued operation is rejected at once and never runs.
   * A running one stops at its next checkpoint and discards its staged writes; the queue moves on only once its work
   * has stopped, which includes node requests and contributor work already in flight. An abort after the commit has
   * started has no effect.
   */
  public runSynced<T>(
    fn: (ctx: SyncedOperationContext) => Promise<T>,
    {
      errorContext,
      forceSync = false,
      signal,
    }: { errorContext?: () => string[]; forceSync?: boolean; signal?: AbortSignal } = {},
  ): Promise<T> {
    const job = async (): Promise<T> => {
      const totalTimer = new Timer();
      const recording = this.node.startRecording();
      let changeSetId: ChangeSetId | undefined;
      try {
        const syncTimer = new Timer();
        if (forceSync || this.autoSync) {
          await this.synchronizer.sync();
        }
        const syncTime = syncTimer.ms();
        const anchorBlockHeader = await this.anchorBlockStore.getBlockHeader();
        signal?.throwIfAborted();

        changeSetId = this.stagedWriteCoordinator.begin();
        this.log.verbose(`Beginning operation ${changeSetId}`, { changeSetId, syncMs: syncTime });

        const operationArgs = {
          stagedWriteCoordinator: this.stagedWriteCoordinator,
          contributors: this.contributors,
          changeSetId,
          signal,
          log: this.log,
        };
        const ctx = { changeSetId, syncTime, anchorBlockHeader, recording, totalMs: () => totalTimer.ms(), signal };
        return await runOperation(operationArgs, async () => {
          try {
            return await fn(ctx);
          } catch (err) {
            // The work stops with a cause-less `AbortError` rather than `signal.reason`: private execution assigns
            // `message` on the errors that cross it, which a `DOMException` reason forbids, and the simulator walks
            // `cause` chains without a cycle guard. The caller gets its own reason back here.
            throw signal && isInterruption(err, signal) ? signal.reason : err;
          }
        });
      } catch (err: any) {
        if (signal?.aborted && Object.is(err, signal.reason)) {
          this.log.verbose('PXE operation interrupted by caller', {
            eventName: 'pxe_operation_interrupted',
            changeSetId,
          });
          throw err;
        }
        throw errorContext ? this.#contextualizeError(err, ...errorContext()) : err;
      } finally {
        recording.stop();
      }
    };

    if (!signal) {
      return this.run(job);
    }
    if (signal.aborted) {
      return Promise.reject(signal.reason);
    }
    const caller = promiseWithResolvers<T>();
    const rejectQueued = () => caller.reject(signal.reason);
    signal.addEventListener('abort', rejectQueued, { once: true });
    this.run(() => {
      signal.removeEventListener('abort', rejectQueued);
      // Aborted while queued: the caller is already rejected, so the operation must not start.
      return signal.aborted ? Promise.reject(signal.reason) : job();
    }).then(caller.resolve, caller.reject);
    return caller.promise;
  }

  #contextualizeError(err: Error, ...context: string[]): Error {
    let contextStr = '';
    if (context.length > 0) {
      contextStr = `\nContext:\n${context.join('\n')}`;
    }
    if (err instanceof SimulationError) {
      err.setAztecContext(contextStr);
    } else {
      this.log.error('PXE operation failed', err);
      this.log.debug('Context of failed PXE operation', { context });
    }
    return err;
  }
}

/**
 * Whether `err` means the work stopped because `signal` aborted: the signal has aborted, and `err` or an error in its
 * `cause` chain is an `AbortError` or the signal's reason itself. The walk tolerates cycles and never reads the
 * reason's own `cause`.
 */
function isInterruption(err: unknown, signal: AbortSignal): boolean {
  if (!signal.aborted) {
    return false;
  }
  const seen = new Set<unknown>();
  let current = err;
  while (!seen.has(current)) {
    if (current instanceof AbortError || Object.is(current, signal.reason)) {
      return true;
    }
    if (typeof current !== 'object' || current === null || !('cause' in current)) {
      return false;
    }
    seen.add(current);
    current = current.cause;
  }
  return false;
}

/** Dependencies of the {@link OperationQueue}. */
type OperationQueueArgs = {
  node: CachingAztecNode;
  synchronizer: BlockSynchronizer;
  anchorBlockStore: AnchorBlockStore;
  stagedWriteCoordinator: StagedWriteCoordinator;
  contributors: OperationContributor[];
  /** Whether synced operations sync with the node before running (see {@link OperationQueue.runSynced}). */
  autoSync: boolean;
  log: Logger;
};
