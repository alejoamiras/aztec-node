import { AztecClientBackend, Barretenberg } from '@aztec-foundation/bb.js';

import type { AztecAddress } from '@aztec-labs/aztec.js/addresses';
import { Fr } from '@aztec-labs/aztec.js/fields';
import type { Logger } from '@aztec-labs/aztec.js/log';
import { parseBooleanEnv } from '@aztec-labs/foundation/config';
import { retryUntil } from '@aztec-labs/foundation/retry';
import { TestContract } from '@aztec-labs/noir-test-contracts.js/Test';
import { jest } from '@jest/globals';

import { AutomineTestContext } from '../../../automine/automine_test_context.js';
import type { TestWallet } from '../../../test-wallet/test_wallet.js';
import type { ProvenTx } from '../../../test-wallet/utils.js';

jest.setTimeout(20 * 60 * 1000);

/** One call to the real `AztecClientBackend.prove`, as the pass-through spy saw it. */
type ProveCall = {
  startedAt: number;
  promise: Promise<unknown>;
  settledAt?: number;
  outcome?: 'resolved' | 'rejected';
};

const REAL_PROOFS = !parseBooleanEnv(process.env.FAKE_PROOFS);

// Aborts a real Chonk proof mid-way through PXE.proveTx and checks that the abort reaches bb.js, the call rejects with
// the caller's reason, and the next proof runs on the same Barretenberg instance. Real proofs only: with fake proofs
// there is no bb.js proof to interrupt.
(REAL_PROOFS ? describe : describe.skip)('single-node/prover/client/prove_abort', () => {
  const reason = new Error('cancelled by the wallet');
  const proveCalls: ProveCall[] = [];
  let suiteStartedAt: number;
  let logger: Logger;
  let proveSpy: jest.SpiedFunction<AztecClientBackend['prove']>;
  let teardown: () => Promise<void>;
  let wallet: TestWallet;
  let owner: AztecAddress;
  let contract: TestContract;
  let abortTimer: NodeJS.Timeout | undefined;

  beforeAll(async () => {
    suiteStartedAt = performance.now();
    const test = await AutomineTestContext.setup({ numberOfAccounts: 1, pxeOpts: { proverEnabled: true } });
    ({
      teardown,
      wallet,
      logger,
      accounts: [owner],
    } = test.context);
    // Only the noinitcheck private `emit_nullifier` is called, so registering is enough.
    contract = await test.registerContract(wallet, TestContract);

    const prove = AztecClientBackend.prototype.prove;
    proveSpy = jest.spyOn(AztecClientBackend.prototype, 'prove').mockImplementation(function (
      this: AztecClientBackend,
      ...args
    ) {
      const startedAt = performance.now();
      const promise = prove.apply(this, args);
      const call: ProveCall = { startedAt, promise };
      promise.then(
        () => Object.assign(call, { settledAt: performance.now(), outcome: 'resolved' }),
        () => Object.assign(call, { settledAt: performance.now(), outcome: 'rejected' }),
      );
      proveCalls.push(call);
      return promise;
    });
  });

  afterAll(async () => {
    clearTimeout(abortTimer);
    proveSpy?.mockRestore();
    await teardown?.();
    logger?.info('prove_abort suite finished', { suiteMs: performance.now() - suiteStartedAt });
  });

  const proveNullifier = async (signal?: AbortSignal): Promise<ProvenTx> => {
    const exec = await contract.methods.emit_nullifier(Fr.random()).request();
    return wallet.proveTx(exec, { from: owner, signal });
  };

  it('aborts a running proof and proves the next tx on the same Barretenberg instance', async () => {
    await proveNullifier();
    const [warmUp] = proveCalls;
    const baselineMs = warmUp.settledAt! - warmUp.startedAt;
    const barretenberg = Barretenberg.getSingleton();

    const controller = new AbortController();
    let tx2RejectedAt: number | undefined;
    const tx2Outcome = proveNullifier(controller.signal).then(
      () => undefined,
      (err: unknown) => {
        tx2RejectedAt = performance.now();
        return err;
      },
    );
    const tx2Prove = await retryUntil(() => proveCalls[1], 'tx 2 prove() start', 600, 0.05);
    const tx3Outcome = proveNullifier().then(
      tx => ({ tx }),
      (err: unknown) => ({ err }),
    );

    let abortedAt: number | undefined;
    let abortedWhileProving = false;
    abortTimer = setTimeout(
      () => {
        abortedWhileProving = tx2Prove.settledAt === undefined;
        abortedAt = performance.now();
        controller.abort(reason);
      },
      Math.max(0, tx2Prove.startedAt + baselineMs / 4 - performance.now()),
    );

    const tx2Error = await tx2Outcome;
    const tx3Result = await tx3Outcome;
    const tx3Prove = proveCalls[2];
    logger.info('prove_abort measurements', {
      baselineProveMs: baselineMs,
      abortToRejectionMs:
        abortedAt === undefined || tx2RejectedAt === undefined ? undefined : tx2RejectedAt - abortedAt,
      tx2ProveMs: tx2Prove.settledAt === undefined ? undefined : tx2Prove.settledAt - tx2Prove.startedAt,
      tx2ProveOutcome: tx2Prove.outcome,
      tx3ProveMs: tx3Prove?.settledAt === undefined ? undefined : tx3Prove.settledAt - tx3Prove.startedAt,
    });

    expect(abortedWhileProving).toBe(true);
    expect(tx2Error).toBe(reason);
    expect(tx3Result).toEqual({ tx: expect.anything() });
    expect(Barretenberg.getSingleton()).toBe(barretenberg);
    // Last, so that against a bb.js that ignores the signal every other expectation has already been checked.
    await expect(tx2Prove.promise).rejects.toBe(reason);
  });

  it('starts no proof when the signal aborts while prepareProve runs', async () => {
    const controller = new AbortController();
    const provesBefore = proveCalls.length;
    const exec = await contract.methods.emit_nullifier(Fr.random()).request();

    await expect(
      wallet.simulateAndProveTx(exec, {
        from: owner,
        signal: controller.signal,
        onSimulated: () => controller.abort(reason),
      }),
    ).rejects.toBe(reason);
    expect(proveCalls).toHaveLength(provesBefore);
  });
});
