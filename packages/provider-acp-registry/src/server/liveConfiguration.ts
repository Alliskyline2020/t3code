// The live-configuration helpers other ACP drivers reuse to surface the models and
// options an agent advertises after `session/new`, as the registry driver does.
export { applyAcpRegistryLiveConfiguration } from "./driver.ts";
export {
  normalizeAcpRegistryLiveConfiguration,
  type AcpRegistryLiveConfiguration,
} from "./probe.ts";
