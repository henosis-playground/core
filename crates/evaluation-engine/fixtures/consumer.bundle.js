// ../../home/null/Work/henosis/repos/platform/packages/core/dist/src/sdk.js
var schemaSymbol = Symbol("henosis.schema");
function makeSchema(wire) {
  return Object.freeze({ kind: wire.kind, [schemaSymbol]: wire });
}
var value = Object.freeze({
  string: () => makeSchema({ kind: "string" }),
  url: () => makeSchema({ kind: "url" }),
  number: () => makeSchema({ kind: "number" }),
  boolean: () => makeSchema({ kind: "boolean" }),
  json: () => makeSchema({ kind: "json" }),
  array: (element) => makeSchema({ kind: "array", element: schemaWire(element) }),
  object: (fields) => makeSchema({
    kind: "object",
    fields: Object.freeze(Object.fromEntries(Object.entries(fields).sort(([left], [right]) => compareCodeUnits(left, right)).map(([name, field]) => [name, schemaWire(field)])))
  })
});
function schemaWire(schema) {
  return schema[schemaSymbol];
}
var output = Object.freeze({
  static(schema) {
    return Object.freeze({ availability: "static", schema, optional: false });
  },
  optionalStatic(schema) {
    return Object.freeze({ availability: "static", schema, optional: true });
  },
  observed(schema) {
    return Object.freeze({ availability: "observed", schema, optional: false });
  },
  optionalObserved(schema) {
    return Object.freeze({ availability: "observed", schema, optional: true });
  }
});
var componentSymbol = Symbol.for("henosis.component.v1");
var outputHandleSymbol = Symbol.for("henosis.output-handle.v1");
var inputValueSymbol = Symbol("henosis.input-value");
var bindingSymbol = Symbol("henosis.output-binding");
var input = Object.freeze({
  required(source) {
    return Object.freeze({ source, optional: false });
  },
  optional(source) {
    return Object.freeze({ source, optional: true });
  }
});
function defineResource(spec) {
  assertKind(spec.kind);
  const outputs = freezeOutputs(spec.outputs);
  return Object.freeze({
    kind: spec.kind,
    outputs,
    create(name, body) {
      assertName(name, "resource name");
      return Object.freeze({ kind: spec.kind, name, body, outputs });
    }
  });
}
function defineComponent(spec) {
  assertName(spec.name, "component name");
  const inputs = Object.freeze({ ...spec.inputs ?? {} });
  const outputs = freezeOutputs(spec.outputs);
  for (const [name, declaration] of Object.entries(inputs)) {
    assertName(name, "input name");
    if (!isOutputHandle(declaration.source)) {
      throw diagnostic("HENOSIS_INPUT_SOURCE", `Input ${quoted(name)} does not reference a component output.`, "Import the producer and use input.required(producer.outputs.<name>) or input.optional(...).");
    }
    if (declaration.optional && !declaration.source.optional) {
      throw diagnostic("HENOSIS_OPTIONAL_INPUT", `Input ${quoted(name)} is optional, but ${sourceLabel(declaration)} is required.`, "Make the producer output optional or consume it with input.required(...).");
    }
  }
  const definition = Object.freeze({ protocolVersion: 1, name: spec.name, inputs, outputs, build: spec.build });
  const handles = Object.freeze(Object.fromEntries(Object.entries(outputs).map(([name, declaration]) => [
    name,
    Object.freeze({ component: spec.name, output: name, optional: declaration.optional, [outputHandleSymbol]: true })
  ])));
  return Object.freeze({ name: spec.name, outputs: handles, [componentSymbol]: definition });
}
function getComponentDefinition(component2) {
  return component2[componentSymbol];
}
function createBundle(component2) {
  const definition = getComponentDefinition(component2);
  return Object.freeze({
    protocolVersion: 1,
    component: metadata(definition),
    evaluate: (snapshot) => executeComponent(component2, snapshot)
  });
}
function executeComponent(component2, snapshot) {
  if (snapshot.protocolVersion !== 1) {
    throw diagnostic("HENOSIS_PROTOCOL_VERSION", `Unsupported snapshot protocol ${String(snapshot.protocolVersion)}.`, "Use the same HOST-PROTOCOL.md version on both sides of the isolate boundary.");
  }
  const definition = getComponentDefinition(component2);
  const reads = /* @__PURE__ */ new Set();
  const sink = new ResourceSink();
  const inputs = materializeInputs(definition.inputs, snapshot.inputs, reads);
  try {
    const result = guardDeterminism(() => definition.build(sink, inputs));
    const encoded = encodeOutputs(definition.outputs, result, sink.addresses());
    return Object.freeze({
      protocolVersion: 1,
      status: "complete",
      resources: sink.seal(),
      outputs: encoded.staticOutputs,
      observedOutputs: encoded.observedOutputs,
      reads: sorted(reads)
    });
  } catch (error) {
    if (error instanceof Blocked) {
      return Object.freeze({ protocolVersion: 1, status: "blocked", resources: sink.seal(), blocked: error.toWire(), reads: sorted(reads) });
    }
    sink.abort();
    throw error;
  }
}
var AuthoringError = class extends Error {
  code;
  summary;
  help;
  constructor(code, summary, help) {
    super(`error[${code}]: ${summary}
  |
  = help: ${help}`);
    this.code = code;
    this.summary = summary;
    this.help = help;
    this.name = "AuthoringError";
  }
};
var Blocked = class extends Error {
  input;
  source;
  operation;
  code = "HENOSIS_BLOCKED";
  constructor(input2, source, operation) {
    super(`blocked[HENOSIS_BLOCKED]: input ${quoted(input2)} from ${source} is not available
  |
  = note: ${operation} requires its concrete value
  = help: Henosis recorded this read and will re-run the component when the producer publishes it`);
    this.input = input2;
    this.source = source;
    this.operation = operation;
    this.name = "Blocked";
  }
  toWire() {
    return Object.freeze({ code: this.code, input: this.input, source: this.source, operation: this.operation, message: this.message });
  }
};
function compareCodeUnits(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}
function canonicalStringify(input2) {
  return JSON.stringify(canonicalize(input2));
}
function canonicalize(input2) {
  if (Array.isArray(input2))
    return Object.freeze(input2.map(canonicalize));
  if (input2 !== null && typeof input2 === "object") {
    return Object.freeze(Object.fromEntries(Object.entries(input2).sort(([a], [b]) => compareCodeUnits(a, b)).map(([key, child]) => [key, canonicalize(child)])));
  }
  return input2;
}
var ResourceSink = class {
  state = "open";
  resources = [];
  seen = /* @__PURE__ */ new Set();
  emit(intent) {
    if (this.state !== "open")
      throw diagnostic("HENOSIS_CLOSED_EMITTER", `The resource emitter is already ${this.state}.`, "Emit synchronously while build is running.");
    const address = `${intent.kind}/${intent.name}`;
    if (this.seen.has(address))
      throw diagnostic("HENOSIS_DUPLICATE_RESOURCE", `Resource ${quoted(address)} was emitted more than once.`, "Give each resource of a kind a stable unique logical name.");
    const body = snapshotJson(intent.body, `resource ${address}`);
    this.resources.push(Object.freeze({ address, kind: intent.kind, name: intent.name, body, canonical: canonicalStringify(body) }));
    this.seen.add(address);
    const outputs = Object.freeze(Object.fromEntries(Object.keys(intent.outputs).map((name) => [
      name,
      Object.freeze({ resource: address, output: name, [bindingSymbol]: void 0 })
    ])));
    return Object.freeze({ address, outputs });
  }
  addresses() {
    return this.seen;
  }
  seal() {
    if (this.state !== "open")
      throw diagnostic("HENOSIS_CLOSED_EMITTER", `The resource emitter is already ${this.state}.`, "The host seals an evaluation exactly once.");
    this.state = "sealed";
    return Object.freeze([...this.resources]);
  }
  abort() {
    this.state = "aborted";
    this.resources.length = 0;
    this.seen.clear();
  }
};
function materializeInputs(declarations, snapshot, reads) {
  const result = {};
  for (const [name, declaration] of Object.entries(declarations)) {
    const cell = snapshot[name];
    if (cell === void 0)
      throw diagnostic("HENOSIS_SNAPSHOT_MISSING_INPUT", `The host omitted declared input ${quoted(name)}.`, "Provide exactly one available, blocked, or absent cell for every declared input.");
    if (cell.state === "absent" && !declaration.optional)
      throw diagnostic("HENOSIS_REQUIRED_INPUT_ABSENT", `Required input ${quoted(name)} (${sourceLabel(declaration)}) is absent.`, "Only optional producer outputs may be absent.");
    const handle = {
      ...declaration.optional ? { present: cell.state !== "absent" } : {},
      get value() {
        reads.add(name);
        if (cell.state === "blocked")
          throw new Blocked(name, sourceLabel(declaration), "reading `.value`");
        if (cell.state === "absent")
          throw diagnostic("HENOSIS_ABSENT_INPUT_READ", `Optional input ${quoted(name)} is absent, but its .value was read.`, `Branch on inputs.${name}.present before reading inputs.${name}.value.`);
        return cell.value;
      },
      [inputValueSymbol]: Object.freeze({
        name,
        source: sourceLabel(declaration),
        state: cell.state,
        markRead: () => {
          reads.add(name);
        }
      })
    };
    result[name] = Object.freeze(handle);
  }
  for (const extra of Object.keys(snapshot)) {
    if (!(extra in declarations))
      throw diagnostic("HENOSIS_SNAPSHOT_EXTRA_INPUT", `The host supplied undeclared input ${quoted(extra)}.`, "Build snapshots from this bundle revision's metadata.");
  }
  return Object.freeze(result);
}
function encodeOutputs(declarations, result, emitted) {
  if (!isRecord(result))
    throw diagnostic("HENOSIS_OUTPUT_OBJECT", "A component build must return an output object.", "Return static values and observed bindings keyed by declared output name.");
  const staticOutputs = {};
  const observedOutputs = {};
  for (const [name, declaration] of Object.entries(declarations)) {
    const candidate = result[name];
    if (candidate === void 0 && declaration.optional)
      continue;
    if (candidate === void 0)
      throw diagnostic("HENOSIS_OUTPUT_MISSING", `Build did not return required ${declaration.availability} output ${quoted(name)}.`, "Return every required output or use an optional declaration.");
    if (declaration.availability === "observed") {
      if (!isBinding(candidate))
        throw diagnostic("HENOSIS_OBSERVED_OUTPUT_BINDING", `Observed output ${quoted(name)} is not bound to an emitted resource output.`, "Use context.emit(resource).outputs.<name>; authors cannot invent observations.");
      if (!emitted.has(candidate.resource))
        throw diagnostic("HENOSIS_UNEMITTED_OUTPUT_BINDING", `Observed output ${quoted(name)} refers to un-emitted resource ${quoted(candidate.resource)}.`, "Bind outputs only from this build's emitted resources.");
      observedOutputs[name] = Object.freeze({ resource: candidate.resource, output: candidate.output });
    } else {
      const json = snapshotJson(candidate, `static output ${name}`);
      assertSchemaValue(declaration.schema, json, `static output ${name}`);
      staticOutputs[name] = json;
    }
  }
  for (const extra of Object.keys(result)) {
    if (!(extra in declarations))
      throw diagnostic("HENOSIS_OUTPUT_EXTRA", `Build returned undeclared output ${quoted(extra)}.`, "Declare the output or remove it.");
  }
  return Object.freeze({ staticOutputs: Object.freeze(staticOutputs), observedOutputs: Object.freeze(observedOutputs) });
}
function snapshotJson(candidate, location) {
  const ancestors = /* @__PURE__ */ new Set();
  const visit = (current, path) => {
    if (isRecord(current) && inputValueSymbol in current) {
      const details = current[inputValueSymbol];
      details.markRead();
      if (details.state === "blocked")
        throw new Blocked(details.name, details.source, `serializing ${path}`);
      throw diagnostic("HENOSIS_INPUT_HANDLE_SERIALIZED", `Input handle ${quoted(details.name)} was placed into ${path}.`, `Use inputs.${details.name}.value. Resources are total and cannot contain handles.`);
    }
    if (current === null || typeof current === "string" || typeof current === "boolean")
      return current;
    if (typeof current === "number") {
      if (!Number.isFinite(current))
        throw diagnostic("HENOSIS_NONFINITE_NUMBER", `${path} contains ${String(current)}.`, "Use a finite JSON number.");
      return current;
    }
    if (typeof current !== "object")
      throw diagnostic("HENOSIS_NON_JSON_VALUE", `${path} contains ${typeof current}.`, "Use only JSON values in resources and static outputs.");
    if (ancestors.has(current))
      throw diagnostic("HENOSIS_CYCLIC_VALUE", `${path} contains a cycle.`, "Return an acyclic JSON value.");
    ancestors.add(current);
    try {
      if (Array.isArray(current))
        return Object.freeze(current.map((child, index) => visit(child, `${path}[${index}]`)));
      const prototype = Object.getPrototypeOf(current);
      if (prototype !== Object.prototype && prototype !== null)
        throw diagnostic("HENOSIS_NON_PLAIN_OBJECT", `${path} contains a class instance.`, "Convert Dates, Maps, Sets, and classes to explicit plain JSON.");
      return Object.freeze(Object.fromEntries(Object.entries(current).sort(([a], [b]) => compareCodeUnits(a, b)).map(([key, child]) => [key, visit(child, `${path}.${key}`)])));
    } finally {
      ancestors.delete(current);
    }
  };
  return visit(candidate, location);
}
function guardDeterminism(run) {
  const now = Date.now;
  const random = Math.random;
  const forbidden = (name) => {
    throw diagnostic("HENOSIS_NONDETERMINISTIC_API", `${name} is unavailable while evaluating a component.`, "Derive desire only from declared inputs and source constants.");
  };
  Date.now = () => forbidden("Date.now()");
  Math.random = () => forbidden("Math.random()");
  try {
    return run();
  } finally {
    Date.now = now;
    Math.random = random;
  }
}
function metadata(definition) {
  return Object.freeze({
    name: definition.name,
    inputs: Object.freeze(Object.fromEntries(Object.entries(definition.inputs).map(([name, declaration]) => [name, Object.freeze({ component: declaration.source.component, output: declaration.source.output, optional: declaration.optional })]))),
    outputs: Object.freeze(Object.fromEntries(Object.entries(definition.outputs).map(([name, declaration]) => [name, Object.freeze({ availability: declaration.availability, optional: declaration.optional, schema: schemaWire(declaration.schema) })])))
  });
}
function freezeOutputs(outputs) {
  for (const [name, declaration] of Object.entries(outputs)) {
    assertName(name, "output name");
    if (declaration.availability !== "static" && declaration.availability !== "observed")
      throw diagnostic("HENOSIS_OUTPUT_AVAILABILITY", `Output ${quoted(name)} has invalid availability.`, "Use output.static(), output.observed(), or an optional form.");
  }
  return Object.freeze({ ...outputs });
}
function assertSchemaValue(schema, candidate, label) {
  const wire = schemaWire(schema);
  const fail = (expected) => {
    throw diagnostic("HENOSIS_OUTPUT_TYPE", `${label} expected ${expected}, received ${jsonKind(candidate)}.`, "Return a value matching the declared schema.");
  };
  switch (wire.kind) {
    case "string":
      if (typeof candidate !== "string")
        fail("string");
      return;
    case "url":
      if (typeof candidate !== "string" || !/^https?:\/\//u.test(candidate))
        fail("absolute HTTP(S) URL");
      return;
    case "number":
      if (typeof candidate !== "number")
        fail("number");
      return;
    case "boolean":
      if (typeof candidate !== "boolean")
        fail("boolean");
      return;
    case "json":
      return;
    case "array": {
      if (!Array.isArray(candidate))
        fail("array");
      for (const child of candidate) {
        assertSchemaValue(makeSchema(wire.element), child, label);
      }
      return;
    }
    case "object": {
      if (!isRecord(candidate) || Array.isArray(candidate))
        fail("object");
      const object = candidate;
      for (const [name, child] of Object.entries(wire.fields)) {
        if (!(name in object))
          fail(`object with field ${name}`);
        assertSchemaValue(makeSchema(child), object[name], `${label}.${name}`);
      }
      return;
    }
  }
}
function isOutputHandle(candidate) {
  return isRecord(candidate) && candidate[outputHandleSymbol] === true;
}
function isBinding(candidate) {
  return isRecord(candidate) && bindingSymbol in candidate;
}
function sourceLabel(declaration) {
  return `${declaration.source.component}.${declaration.source.output}`;
}
function assertKind(kind) {
  if (!/^[a-z][a-z0-9-]*\/[a-z][a-z0-9-]*@[1-9][0-9]*$/u.test(kind))
    throw diagnostic("HENOSIS_RESOURCE_KIND", `Invalid resource kind ${quoted(kind)}.`, "Use a versioned kind such as cloudflare/worker@1.");
}
function assertName(name, label) {
  if (!/^[a-z][a-z0-9_-]{0,62}$/u.test(name))
    throw diagnostic("HENOSIS_LOGICAL_NAME", `Invalid ${label} ${quoted(name)}.`, "Use 1-63 lowercase letters, digits, underscores, or hyphens, beginning with a letter.");
}
function diagnostic(code, summary, help) {
  return new AuthoringError(code, summary, help);
}
function quoted(value2) {
  return JSON.stringify(value2);
}
function jsonKind(input2) {
  return input2 === null ? "null" : Array.isArray(input2) ? "array" : typeof input2;
}
function isRecord(input2) {
  return typeof input2 === "object" && input2 !== null;
}
function sorted(values) {
  return Object.freeze([...values].sort(compareCodeUnits));
}

// src/producer.ts
var producer_default = defineComponent({
  name: "producer",
  outputs: { value: output.static(value.string()) },
  build() {
    return { value: "ready" };
  }
});

// src/consumer.ts
var item = defineResource({
  kind: "test/item@1",
  outputs: { result: output.observed(value.string()) }
});
var consumer_default = defineComponent({
  name: "consumer",
  inputs: { source: input.required(producer_default.outputs.value) },
  outputs: { summary: output.static(value.string()), result: output.observed(value.string()) },
  build(context, inputs) {
    const value2 = inputs.source.value;
    const emitted = context.emit(item.create("main", { value: value2 }));
    return { summary: value2, result: emitted.outputs.result };
  }
});

// henosis-component.ts
var bundle = createBundle(consumer_default);
var protocolVersion = bundle.protocolVersion;
var component = bundle.component;
var evaluate = bundle.evaluate;
export {
  component,
  evaluate,
  protocolVersion
};
