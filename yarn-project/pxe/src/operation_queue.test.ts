import { AbortError, throwAbortErrorIfAborted } from '@aztec-labs/foundation/error';
import type { Logger } from '@aztec-labs/foundation/log';
import { promiseWithResolvers } from '@aztec-labs/foundation/promise';
import type { AztecAsyncKVStore } from '@aztec-labs/kv-store';
import { openTmpStore } from '@aztec-labs/kv-store/lmdb-v2';
import { SimulationError } from '@aztec-labs/stdlib/errors';
import { BlockHeader } from '@aztec-labs/stdlib/tx';
import { type MockProxy, mock } from 'jest-mock-extended';
import { getEventListeners } from 'node:events';

import type { BlockSynchronizer } from './block_synchronizer/index.js';
import type { Recording } from './node/benchmarked_node.js';
import type { CachingAztecNode } from './node/caching_aztec_node.js';
import type { OperationContributor } from './operation_lifecycle.js';
import { OperationQueue } from './operation_queue.js';
import type { AnchorBlockStore } from './storage/anchor_block_store/anchor_block_store.js';
import { type ChangeSetId, type StagedStore, StagedWriteCoordinator } from './storage/staged_write_coordinator.js';

describe('OperationQueue', () => {
  let store: AztecAsyncKVStore;
  let coordinator: StagedWriteCoordinator;
  let node: MockProxy<CachingAztecNode>;
  let synchronizer: MockProxy<BlockSynchronizer>;
  let anchorBlockStore: MockProxy<AnchorBlockStore>;

  /** A staged store whose committed/discarded change sets are observable. */
  let committed: ChangeSetId[];
  let discarded: ChangeSetId[];
  /** Runs inside the commit, before the change set counts as committed. */
  let duringCommit: () => void;

  beforeEach(async () => {
    store = await openTmpStore('operation_queue_test');

    committed = [];
    discarded = [];
    duringCommit = () => {};
    const recordingStore: StagedStore = {
      storeName: 'recording_store',
      beginChangeSet: () => {},
      commitChangeSet: id => {
        duringCommit();
        committed.push(id);
        return Promise.resolve();
      },
      discardChangeSet: id => {
        discarded.push(id);
      },
    };
    coordinator = new StagedWriteCoordinator({ kvStore: store, stagedStores: [recordingStore] });

    node = mock<CachingAztecNode>();
    node.startRecording.mockReturnValue(mock<Recording>());
    synchronizer = mock<BlockSynchronizer>();
    anchorBlockStore = mock<AnchorBlockStore>();
    anchorBlockStore.getBlockHeader.mockResolvedValue(BlockHeader.empty());
  });

  it('commits the change set when the operation succeeds', async () => {
    const queue = makeQueue();

    const result = await queue.runSynced(({ changeSetId }) => Promise.resolve(changeSetId));

    expect(committed).toEqual([result]);
    expect(discarded).toEqual([]);
  });

  it('discards the change set when the operation rejects', async () => {
    const queue = makeQueue();
    let id: ChangeSetId;

    await expect(
      queue.runSynced(({ changeSetId }) => {
        id = changeSetId;
        return Promise.reject(new Error('operation failed'));
      }),
    ).rejects.toThrow('operation failed');

    expect(committed).toEqual([]);
    expect(discarded).toEqual([id!]);
  });

  it('serializes concurrent work in submission order', async () => {
    const queue = makeQueue();
    const gate = promiseWithResolvers<void>();
    const started = promiseWithResolvers<void>();
    const executionOrder: number[] = [];
    const first = queue.run(async () => {
      executionOrder.push(1);
      started.resolve();
      await gate.promise;
    });
    await started.promise;
    const second = queue.run(() => {
      executionOrder.push(2);
      return Promise.resolve(2);
    });
    const third = queue.run(() => {
      executionOrder.push(3);
      return Promise.resolve(3);
    });
    try {
      await Promise.resolve();
      expect(executionOrder).toEqual([1]);
    } finally {
      gate.resolve();
      await queue.stop();
    }
    expect(await Promise.all([first, second, third])).toEqual([undefined, 2, 3]);
    expect(executionOrder).toEqual([1, 2, 3]);
  });

  describe('with an abort signal', () => {
    let controller: AbortController;
    const reason = new Error('cancelled by the wallet');

    beforeEach(() => {
      controller = new AbortController();
    });

    it('rejects a queued operation at once and never starts it', async () => {
      const queue = makeQueue();
      const gate = promiseWithResolvers<void>();
      const events: string[] = [];
      synchronizer.sync.mockImplementation(() => {
        events.push('synced');
        return Promise.resolve();
      });
      const ahead = queue.runSynced(async ({ changeSetId }) => {
        await gate.promise;
        return changeSetId;
      });
      const queued = queue.runSynced(
        () => {
          events.push('ran');
          return Promise.resolve();
        },
        { forceSync: true, signal: controller.signal },
      );

      controller.abort(reason);
      // Rejects while the operation ahead is still running.
      await expect(queued).rejects.toBe(reason);
      gate.resolve();
      const aheadId = await ahead;
      await queue.stop();

      expect(committed).toEqual([aheadId]);
      expect(discarded).toEqual([]);
      expect(events).toEqual([]);
    });

    it('rejects with the reason when the work stops, discards, and frees the queue', async () => {
      const queue = makeQueue();
      let id: ChangeSetId;

      const aborted = queue.runSynced(
        ({ changeSetId, signal }) => {
          id = changeSetId;
          controller.abort(reason);
          try {
            throwAbortErrorIfAborted(signal);
          } catch (err) {
            return Promise.reject(new SimulationError('Aborted by caller', [], [], undefined, { cause: err }));
          }
          return Promise.resolve();
        },
        { signal: controller.signal, errorContext: () => ['context'] },
      );

      await expect(aborted).rejects.toBe(reason);
      expect(discarded).toEqual([id!]);
      const next = await queue.runSynced(({ changeSetId }) => Promise.resolve(changeSetId));
      expect(committed).toEqual([next]);
    });

    it('resolves and commits when aborted once the commit has started', async () => {
      const queue = makeQueue();
      duringCommit = () => controller.abort(reason);

      await expect(queue.runSynced(() => Promise.resolve('result'), { signal: controller.signal })).resolves.toEqual(
        'result',
      );
      expect(committed).toHaveLength(1);
    });

    it.each([
      ['an independent failure', new Error('independent failure')],
      ['a failure whose cause chain loops', cyclicError()],
    ])('passes %s through unchanged while the signal is aborted', async (_, failure) => {
      const aborted = makeQueue().runSynced(
        () => {
          controller.abort(reason);
          return Promise.reject(failure);
        },
        { signal: controller.signal },
      );

      await expect(aborted).rejects.toBe(failure);
    });

    it('passes a settle failure through unchanged while the signal is aborted', async () => {
      const failure = new Error('settle failed', { cause: new AbortError('another operation') });
      const failingSettle: OperationContributor = {
        settle: () => {
          controller.abort(reason);
          return Promise.reject(failure);
        },
        onOperationEnd: () => {},
      };

      await expect(
        makeQueue([failingSettle]).runSynced(() => Promise.resolve(), { signal: controller.signal }),
      ).rejects.toBe(failure);
    });

    it('passes a commit failure through unchanged while the signal is aborted', async () => {
      const failure = new Error('commit failed', { cause: new AbortError('another operation') });
      duringCommit = () => {
        controller.abort(reason);
        throw failure;
      };

      await expect(makeQueue().runSynced(() => Promise.resolve(), { signal: controller.signal })).rejects.toBe(failure);
      expect(committed).toEqual([]);
    });

    it('rejects without running when the signal has already aborted', async () => {
      const queue = makeQueue();
      let ran = false;
      controller.abort(reason);

      await expect(
        queue.runSynced(
          () => {
            ran = true;
            return Promise.resolve();
          },
          { signal: controller.signal },
        ),
      ).rejects.toBe(reason);
      await queue.stop();

      expect(ran).toBe(false);
      expect(committed).toEqual([]);
      expect(discarded).toEqual([]);
    });

    it.each([
      ['stops at a checkpoint', false],
      ['rejects with the reason itself', true],
    ])('returns a non-Error reason by identity when the work %s', async (_, rejectWithReason) => {
      const aborted = makeQueue().runSynced(
        ({ signal }) => {
          controller.abort('closed');
          if (rejectWithReason) {
            return Promise.reject(signal?.reason);
          }
          throwAbortErrorIfAborted(signal);
          return Promise.resolve();
        },
        { signal: controller.signal },
      );

      await expect(aborted).rejects.toBe('closed');
    });

    it('leaves no abort listener behind when the operation cannot be enqueued', async () => {
      const queue = makeQueue();
      await queue.stop();

      await expect(queue.runSynced(() => Promise.resolve(), { signal: controller.signal })).rejects.toThrow(
        'Could not enqueue',
      );
      expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
    });
  });

  function makeQueue(contributors: OperationContributor[] = []) {
    const queue = new OperationQueue({
      node,
      synchronizer,
      anchorBlockStore,
      stagedWriteCoordinator: coordinator,
      contributors,
      autoSync: false,
      log: mockLog,
    });
    queue.start();
    return queue;
  }

  const mockLog = mock<Logger>();
});

function cyclicError(): Error {
  const err = new Error('cyclic failure');
  err.cause = err;
  return err;
}
