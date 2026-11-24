import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSelfInvocation, type SelfInvocation } from "@t3tools/shared/nodeRuntime";
import { ProviderDriverKind } from "@t3tools/contracts";
import { DeepSeekHarnessSettings } from "../settings.ts";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as SecureRandom from "effect/crypto/SecureRandom";
import type * as Scope from "effect/Scope";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as FileSystem from "effect/FileSystem";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import type * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import type * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type {
  ProviderAdapterDriver,
  ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import type * as Path from "effect/Path";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { readCustomModelEntries } from "@t3tools/shared/model";

import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "@t3tools/provider-acp/server/adapter";
import { makeAcpNativeLoggerFactory } from "@t3tools/provider-acp/server/nativeLogging";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import { normalizeAcpRegistryLiveConfiguration } from "@t3tools/provider-acp-registry/server/liveConfiguration";

import { makeDshAcpRuntime } from "./dshSupport.ts";

export const DSH_PROVIDER = ProviderDriverKind.make("deepseekHarness");
const DEFAULT_DSH_SETTINGS = Schema.decodeSync(DeepSeekHarnessSettings)({});
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);

/**
 * DeepSeek Harness drives its own models and tools behind its profile. Everything
 * stays on the shared ACP defaults: the models, reasoning options, and commands
 * the harness advertises per session reach the composer through session config.
 */
const DshProviderCapabilitiesV2 = AcpProviderCapabilitiesV2;

/**
 * DeepSeek Harness advertises `mcpCapabilities.http`, but T3's stdio MCP bridge is
 * the transport verified end to end against it (same as the registry driver), so
 * HTTP MCP is left off: a failed HTTP MCP connection fails `session/new` outright.
 */
function dshPromptFailure(cause: unknown) {
  return makeProviderFailure({
    cause,
    // The harness reports its own failures (e.g. MCP sync, auth) as request errors
    // with a user-facing message; keep it instead of a generic failure.
    ...(isAcpRequestError(cause)
      ? { message: cause.errorMessage, code: String(cause.code), class: "provider_error" as const }
      : { class: "provider_error" as const }),
  });
}

export interface DshAdapterV2Options {
  readonly instanceId: Parameters<typeof makeAcpAdapterV2>[0]["instanceId"];
  readonly settings: DeepSeekHarnessSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly selfInvocation: SelfInvocation;
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  /** Publishes the models and options each session advertises to the provider snapshot. */
  readonly runtimeCoordinator?: AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator["Service"];
  /** Replaces the `dsh` launch (replay tests). The session setup still applies. */
  readonly makeRuntime?: (
    input: AcpAdapterV2RuntimeInput,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError,
    ChildProcessSpawner.ChildProcessSpawner | SecureRandom.SecureRandom | Scope.Scope
  >;
  readonly testHooks?: Parameters<typeof makeAcpAdapterV2>[0]["testHooks"];
}

function makeDshAcpAdapterFlavor(options: DshAdapterV2Options): AcpAdapterV2Flavor {
  const customModels = readCustomModelEntries(options.settings.customModels).map(
    (entry) => entry.slug,
  );
  const makeRuntime =
    options.makeRuntime ??
    ((input: AcpAdapterV2RuntimeInput) => {
      const { runtimePolicy: _runtimePolicy, processEnvironment, ...runtimeInput } = input;
      return makeDshAcpRuntime({
        ...runtimeInput,
        settings: options.settings,
        environment: { ...options.environment, ...processEnvironment },
        customModels,
      });
    });
  const { runtimeCoordinator } = options;
  return {
    driver: DSH_PROVIDER,
    runtimeHarness: "DeepSeek Harness",
    capabilities: DshProviderCapabilitiesV2,
    // Custom model ids bypass session validation; the harness rejects a model it cannot run.
    customModels,
    compactViaAdvertisedCommand: true,
    promptFailure: dshPromptFailure,
    makeRuntime,
    ...(runtimeCoordinator === undefined
      ? {}
      : {
          onSessionConfigurationUpdate: (configOptions, modeState, modelState) =>
            runtimeCoordinator.publishLiveConfiguration(
              options.instanceId,
              normalizeAcpRegistryLiveConfiguration(configOptions, modeState, modelState),
            ),
        }),
  };
}

export const makeDshAdapterV2 = Effect.fn("makeDshAdapterV2")(function* (
  options: DshAdapterV2Options,
) {
  return yield* makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor: makeDshAcpAdapterFlavor(options),
    selfInvocation: options.selfInvocation,
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
    ...(options.testHooks === undefined ? {} : { testHooks: options.testHooks }),
  });
});

export type DshAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | SecureRandom.SecureRandom
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | Path.Path
  | McpProviderSessions.McpProviderSessions
  | ProviderEventLoggers.ProviderEventLoggers
  | ProviderHost.ProviderHost;

export const makeDshAdapterV2Driver = (): ProviderAdapterDriver<
  DeepSeekHarnessSettings,
  DshAdapterV2DriverEnv
> => ({
  driverKind: DSH_PROVIDER,
  configSchema: DeepSeekHarnessSettings,
  defaultConfig: () => DEFAULT_DSH_SETTINGS,
  create: Effect.fn("DshAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<DeepSeekHarnessSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const selfInvocation = yield* resolveSelfInvocation();
      const providerEventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
      const makeNativeLogger = yield* makeAcpNativeLoggerFactory();
      const runtimeCoordinator = Option.getOrUndefined(
        yield* Effect.serviceOption(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator),
      );
      return yield* makeDshAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        selfInvocation,
        nativeLogging: (threadId) =>
          makeNativeLogger({
            nativeEventLogger: providerEventLoggers.native,
            provider: DSH_PROVIDER,
            threadId,
          }),
        ...(runtimeCoordinator === undefined ? {} : { runtimeCoordinator }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: DSH_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create DeepSeek Harness ACP adapter.",
              cause,
            }),
        ),
      ),
  ),
});
