// Generates TypeScript frame types from the Falcon AsyncAPI spec.
//
// The spec (`spec/falcon.asyncapi.yaml`) is the copied source of truth from the
// nanobpmn server repo (`docs/falcon.asyncapi.yaml`). Its `components.schemas`
// are plain JSON Schema, so we bundle them under `definitions`, rewrite the
// intra-spec `$ref`s, and run `json-schema-to-typescript`. Client vs server
// grouping is derived from the `operations` section (sendCommand = client,
// receiveEvent = server) so the two discriminated unions track the documented
// protocol exactly — not a hand-maintained list.
//
// Output: `src/generated/falconFrames.ts`. Do not edit by hand; run
// `npm run generate`.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { compile } from "json-schema-to-typescript";
import { parse } from "yaml";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const SPEC = join(root, "spec", "falcon.asyncapi.yaml");
const OUT = join(root, "src", "generated", "falconFrames.ts");

const raw = readFileSync(SPEC, "utf8");
const doc = parse(raw);
const specHash = createHash("sha256").update(raw).digest("hex").slice(0, 12);

const schemas = doc.components?.schemas ?? {};
const messages = doc.components?.messages ?? {};
const channelMessages = doc.channels?.falcon?.messages ?? {};

// Resolve an operation message ref chain: operation -> channel message ->
// component message -> schema name.
const refName = (ref) => ref.split("/").pop();
const schemaForChannelMessage = (channelMsgName) => {
  const compMsgRef = channelMessages[channelMsgName]?.$ref;
  const compMsg = messages[refName(compMsgRef)];
  return refName(compMsg?.payload?.$ref);
};
const schemasForOperation = (opName) =>
  (doc.operations?.[opName]?.messages ?? [])
    .map((m) => schemaForChannelMessage(refName(m.$ref)))
    .filter(Boolean);

const clientSchemas = schemasForOperation("sendCommand");
const serverSchemas = schemasForOperation("receiveEvent");

// Rewrite `#/components/schemas/X` -> `#/definitions/X` and give each schema a
// title so json-schema-to-typescript names the interface after its key.
const rewriteRefs = (node) => {
  if (Array.isArray(node)) return node.map(rewriteRefs);
  if (node && typeof node === "object") {
    const out = {};
    for (const [k, v] of Object.entries(node)) {
      if (k === "$ref" && typeof v === "string") {
        out.$ref = v.replace("#/components/schemas/", "#/definitions/");
      } else {
        out[k] = rewriteRefs(v);
      }
    }
    return out;
  }
  return node;
};

const definitions = {};
for (const [name, schema] of Object.entries(schemas)) {
  definitions[name] = { title: name, ...rewriteRefs(schema) };
}

// One combined root so every referenced definition is emitted exactly once
// (shared `Heartbeat`, `Corr`, `Key` are not duplicated across the two unions).
const frameNames = [...new Set([...clientSchemas, ...serverSchemas])];
const bundle = {
  title: "AnyFalconFrame",
  definitions,
  oneOf: frameNames.map((n) => ({ $ref: `#/definitions/${n}` })),
};

const banner = `/**
 * GENERATED — DO NOT EDIT.
 *
 * TypeScript frame types for the Falcon WebSocket protocol, generated from
 * \`spec/falcon.asyncapi.yaml\` (source: nanobpmn \`docs/falcon.asyncapi.yaml\`)
 * by \`scripts/generate-falcon.mjs\`. Regenerate with \`npm run generate\`.
 *
 * Spec content hash: ${specHash}
 */
/* eslint-disable */`;

const body = await compile(bundle, "AnyFalconFrame", {
  bannerComment: "",
  additionalProperties: false,
  declareExternallyReferenced: true,
  format: true,
  style: { singleQuote: false },
});

const union = (name, members, doc) =>
  `\n/** ${doc} */\nexport type ${name} =\n  | ${members.join("\n  | ")};\n`;

const clientUnion = union(
  "ClientFrame",
  clientSchemas,
  "Every frame a public client may send on `/falcon` (sendCommand operation).",
);
const serverUnion = union(
  "ServerFrame",
  serverSchemas,
  "Every frame the server may push to a client (receiveEvent operation).",
);

// Discriminator literal-string unions, handy for exhaustiveness checks.
const tags = (names) =>
  names.map((n) => JSON.stringify(schemas[n].properties.type.const)).join(" | ");
const tagTypes =
  `\n/** Discriminator (\`type\`) tags for {@link ClientFrame}. */\n` +
  `export type ClientFrameType = ${tags(clientSchemas)};\n` +
  `\n/** Discriminator (\`type\`) tags for {@link ServerFrame}. */\n` +
  `export type ServerFrameType = ${tags(serverSchemas)};\n`;

writeFileSync(OUT, `${banner}\n\n${body}${clientUnion}${serverUnion}${tagTypes}`);
console.log(
  `Generated ${OUT}\n  client frames: ${clientSchemas.join(", ")}\n  server frames: ${serverSchemas.join(", ")}\n  spec hash: ${specHash}`,
);
