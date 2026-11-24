import { ProviderDriverKind, type ServerProvider, TextGenerationError } from "@t3tools/contracts";
import { DeepSeekHarnessSettings } from "../settings.ts";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import type * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import {
  applyAcpRegistryLiveConfiguration,
  type AcpRegistryLiveConfiguration,
} from "@t3tools/provider-acp-registry/server/liveConfiguration";
import { type DshAdapterV2DriverEnv, makeDshAdapterV2Driver } from "./adapter.ts";
import { buildInitialDshProviderSnapshot, checkDshProviderStatus } from "./status.ts";

const DRIVER_KIND = ProviderDriverKind.make("deepseekHarness");
const decodeDshSettings = Schema.decodeSync(DeepSeekHarnessSettings);
// The npm CLI updates itself between releases; T3 offers no one-click update.
const MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

const unsupportedTextGeneration: ProviderTextGeneration = (() => {
  const unsupported = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "DeepSeek Harness does not provide application text generation.",
      }),
    );
  return {
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
    generateReviewGuide: () => unsupported("generateReviewGuide"),
    generateTranscriptionPostProcessing: () => unsupported("generateTranscriptionPostProcessing"),
  };
})();

export type DshDriverEnv =
  | DshAdapterV2DriverEnv
  | ChildProcessSpawner.ChildProcessSpawner
  | SecureRandom.SecureRandom
  | ProviderHost.ProviderHost;

export const DshDriver: ProviderDriver<DeepSeekHarnessSettings, DshDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "DeepSeek Harness",
    supportsMultipleInstances: true,
  },
  configSchema: DeepSeekHarnessSettings,
  defaultConfig: (): DeepSeekHarnessSettings => decodeDshSettings({}),
  create: Effect.fn("DshDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    environment,
    enabled,
    config,
  }) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const processEnv = yield* mergeProviderInstanceEnvironment(
      environment,
      yield* HostProcess.Environment,
    );
    // Shared with the registry driver: the adapter publishes what each session advertises
    // (models, reasoning effort) and the snapshot below merges it into the picker.
    const runtimeCoordinator = Option.getOrUndefined(
      yield* Effect.serviceOption(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator),
    );
    if (runtimeCoordinator !== undefined) {
      yield* runtimeCoordinator.clearLiveConfiguration(instanceId);
      yield* Effect.addFinalizer(() => runtimeCoordinator.clearLiveConfiguration(instanceId));
    }
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER_KIND,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const effectiveConfig = {
      ...config,
      enabled,
      binaryPath: expandHomePath(config.binaryPath, yield* HostProcess.HomeDirectory),
    } satisfies DeepSeekHarnessSettings;
    const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
    const snapshot = yield* makeManagedServerProvider<
      ProviderSnapshotSettings<DeepSeekHarnessSettings>
    >({
      resolveMaintenance: () => Effect.succeed(MAINTENANCE),
      getSettings: snapshotSettings.getSettings,
      streamSettings: snapshotSettings.streamSettings,
      haveSettingsChanged: haveProviderSnapshotSettingsChanged,
      initialSnapshot: (settings) =>
        buildInitialDshProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
      checkProvider: checkDshProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      ),
      ...(runtimeCoordinator === undefined
        ? {}
        : {
            enrichSnapshot: Effect.fn("DshDriver.enrichSnapshot")(function* ({
              getSnapshot,
              publishSnapshot,
            }: {
              readonly getSnapshot: Effect.Effect<ServerProvider>;
              readonly publishSnapshot: (next: ServerProvider) => Effect.Effect<void>;
            }) {
              const coordinator = runtimeCoordinator;
              const semaphore = yield* Semaphore.make(1);
              const applyLive = (configuration: AcpRegistryLiveConfiguration) =>
                semaphore.withPermits(1)(
                  getSnapshot.pipe(
                    Effect.flatMap((latest) =>
                      publishSnapshot(
                        stampIdentity(
                          applyAcpRegistryLiveConfiguration(
                            latest,
                            configuration,
                            effectiveConfig.customModels,
                            latest.models,
                          ),
                        ),
                      ),
                    ),
                  ),
                );
              yield* coordinator.watchLiveConfiguration(instanceId, applyLive);
            }),
          }),
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build DeepSeek Harness snapshot.",
            cause,
          }),
      ),
    );

    const orchestrationAdapter = yield* makeDshAdapterV2Driver()
      .create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config: effectiveConfig,
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build DeepSeek Harness orchestration adapter.",
              cause,
            }),
        ),
      );

    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      orchestrationAdapter,
      textGeneration: unsupportedTextGeneration,
    } satisfies ProviderInstance;
  }),
};
