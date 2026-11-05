import { AztecClientBackend, Barretenberg, INTERRUPTED_MESSAGE } from '@aztec-foundation/bb.js';

import type { AztecAddress } from '@aztec-labs/aztec.js/addresses';
import { Fr } from '@aztec-labs/aztec.js/fields';
import type { Logger } from '@aztec-labs/aztec.js/log';
import { parseBooleanEnv } from '@aztec-labs/foundation/config';
import { retryUntil } from '@aztec-labs/foundation/retry';
import { TestContract } from '@aztec-labs/noir-test-contracts.js/Test';
import { jest } from '@jest/globals';

import { AutomineTestContext } from '../../../automine/automine_test_context.js';
import type { TestWallet } from '../../../test-wallet/test_wallet.js';

// Below CI's 10-minute limit for the whole file, so a stuck hook or test fails under its own name.
jest.setTimeout(8 * 60 * 1000);

/** One call to the real `AztecClientBackend.prove`, as the pass-through spy saw it. */
type ProveCall = {
  startedAt: number;
  promise: Promise<unknown>;
  settledAt?: number;
  outcome?: 'resolved' | 'rejected';
};

const REAL_PROOFS = !parseBooleanEnv(process.env.FAKE_PROOFS);

describe('single-node/prover/client/prove_abort', () => {
  const reason = new Error('cancelled by the wallet');
  const proveCalls: ProveCall[] = [];
  let suiteStartedAt: number;
  let logger: Logger;
  let proveSpy: jest.SpiedFunction<AztecClientBackend['prove']>;
  let chonkAccumulateSpy: jest.SpiedFunction<Barretenberg['chonkAccumulate']>;
  let chonkProveSpy: jest.SpiedFunction<Barretenberg['chonkProve']>;
  let teardown: () => Promise<void>;
  let wallet: TestWallet;
  let owner: AztecAddress;
  let contract: TestContract;

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
    chonkAccumulateSpy = jest.spyOn(Barretenberg.prototype, 'chonkAccumulate');
    chonkProveSpy = jest.spyOn(Barretenberg.prototype, 'chonkProve');
  });

  afterAll(async () => {
    proveSpy?.mockRestore();
    chonkAccumulateSpy?.mockRestore();
    chonkProveSpy?.mockRestore();
    await teardown?.();
    logger?.info('prove_abort suite finished', { suiteMs: performance.now() - suiteStartedAt });
  });

  const nullifierRequest = () => contract.methods.emit_nullifier(Fr.random()).request();

  // `proverEnabled` makes the PXE prove with bb.js even under FAKE_PROOFS, so skipping this case there only saves the
  // cost of its two real Chonk proofs.
  (REAL_PROOFS ? it : it.skip)(
    'aborts a running proof and proves the next tx on the same Barretenberg instance',
    async () => {
      // Built before tx 2 starts: building it runs PXE operations that would otherwise wait for tx 2 to finish.
      const proveTx3 = await wallet.prepareProveTx(await nullifierRequest(), { from: owner });

      const controller = new AbortController();
      const provesBefore = proveCalls.length;
      const accumulatesBefore = chonkAccumulateSpy.mock.calls.length;
      const chonkProvesBefore = chonkProveSpy.mock.calls.length;
      let tx2RejectedAt: number | undefined;
      const tx2Outcome = wallet.proveTx(await nullifierRequest(), { from: owner, signal: controller.signal }).then(
        () => undefined,
        (err: unknown) => {
          tx2RejectedAt = performance.now();
          return err;
        },
      );
      const tx2Prove = await retryUntil(() => proveCalls[provesBefore], 'tx 2 prove() start', 600, 0.05);
      const tx3QueuedWhileProving = tx2Prove.settledAt === undefined;
      const tx3Outcome = proveTx3().then(
        tx => ({ tx }),
        (err: unknown) => ({ err }),
      );

      const barretenberg = Barretenberg.getSingleton();
      // Fails here on a backend that cannot interrupt, such as a Wasm fallback, rather than on a timing below.
      expect(barretenberg.interruptible).toBe(true);
      const firstAccumulate = await retryUntil(
        () => chonkAccumulateSpy.mock.results[accumulatesBefore],
        'tx 2 first chonkAccumulate',
        600,
        0.01,
      );
      await firstAccumulate.value;
      const abortedWhileProving = tx2Prove.settledAt === undefined;
      const abortedAt = performance.now();
      controller.abort(reason);

      const tx2Error = await tx2Outcome;
      const tx3Result = await tx3Outcome;
      const tx3Prove = proveCalls[provesBefore + 1];
      logger.info('prove_abort measurements', {
        abortToRejectionMs: tx2RejectedAt === undefined ? undefined : tx2RejectedAt - abortedAt,
        tx2ProveMs: tx2Prove.settledAt === undefined ? undefined : tx2Prove.settledAt - tx2Prove.startedAt,
        tx2ProveOutcome: tx2Prove.outcome,
        tx3ProveMs: tx3Prove?.settledAt === undefined ? undefined : tx3Prove.settledAt - tx3Prove.startedAt,
      });

      expect(tx3QueuedWhileProving).toBe(true);
      expect(abortedWhileProving).toBe(true);
      // bb's answer to a Chonk command stopped by an interrupt; later Chonk commands get it too until it is cleared.
      await expect(chonkProveSpy.mock.results[chonkProvesBefore]?.value).rejects.toThrow(INTERRUPTED_MESSAGE);
      await expect(tx2Prove.promise).rejects.toBe(reason);
      expect(tx2Error).toBe(reason);
      // bb.js turns it false only when bb does not know `chonkStop` (an older binary), not on a slow acknowledgement.
      // Such a binary never clears the interrupt, so the next proof fails with it: checked first for a clear message.
      expect(barretenberg.interruptible).toBe(true);
      expect(tx3Result).toEqual({ tx: expect.anything() });
      expect(tx3Prove.startedAt).toBeGreaterThanOrEqual(tx2Prove.settledAt!);
      expect(Barretenberg.getSingleton()).toBe(barretenberg);
    },
  );

  it('starts no proof when the signal aborts while prepareProve runs', async () => {
    const controller = new AbortController();
    const provesBefore = proveCalls.length;

    await expect(
      wallet.simulateAndProveTx(await nullifierRequest(), {
        from: owner,
        signal: controller.signal,
        onSimulated: () => controller.abort(reason),
      }),
    ).rejects.toBe(reason);
    expect(proveCalls).toHaveLength(provesBefore);
  });
});
