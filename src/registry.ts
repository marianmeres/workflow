import { defKey, validateDefinition } from "./definition.ts";
import type { Handler, Matcher, WorkflowDefinition } from "./types.ts";

/**
 * In-memory registry of definitions, handlers, and matchers. Built once at
 * startup; immutable after construction (use `Workflow`'s constructor).
 *
 * Definitions are keyed by `<id>@<version>` — pin-and-freeze: each instance
 * runs on its birth-version forever. Keep old versions in code until the last
 * instance using them completes.
 */
export class WorkflowRegistry {
	#definitions: Map<string, WorkflowDefinition> = new Map();
	#handlers: Map<string, Handler> = new Map();
	#matchers: Map<string, Matcher> = new Map();

	/**
	 * Validates and stores definitions, handlers, and matchers. Throws on the
	 * first structural problem (unknown handler, unknown transition target,
	 * missing meta, etc.) or on duplicate `(id, version)` registration.
	 */
	constructor(input: {
		definitions: WorkflowDefinition[];
		handlers: Record<string, Handler>;
		matchers?: Record<string, Matcher>;
	}) {
		for (const [name, fn] of Object.entries(input.handlers)) {
			this.#handlers.set(name, fn);
		}
		for (const [name, fn] of Object.entries(input.matchers ?? {})) {
			this.#matchers.set(name, fn);
		}

		const handlerNames = new Set(this.#handlers.keys());
		const matcherNames = new Set(this.#matchers.keys());

		for (const def of input.definitions) {
			validateDefinition(def, {
				handlers: handlerNames,
				matchers: matcherNames,
			});
			const key = defKey(def.id, def.version);
			if (this.#definitions.has(key)) {
				throw new Error(`Workflow definition already registered: ${key}`);
			}
			this.#definitions.set(key, def);
		}
	}

	/** Looks up a definition by `(id, version)`. Returns `undefined` if not registered. */
	getDefinition(id: string, version: string): WorkflowDefinition | undefined {
		return this.#definitions.get(defKey(id, version));
	}

	/** Like {@link getDefinition} but throws if the definition is not registered. */
	requireDefinition(id: string, version: string): WorkflowDefinition {
		const def = this.getDefinition(id, version);
		if (!def) {
			throw new Error(`Unknown workflow definition: ${defKey(id, version)}`);
		}
		return def;
	}

	/** Looks up a registered handler by name. Returns `undefined` if missing. */
	getHandler(name: string): Handler | undefined {
		return this.#handlers.get(name);
	}

	/** Like {@link getHandler} but throws if the handler is not registered. */
	requireHandler(name: string): Handler {
		const h = this.#handlers.get(name);
		if (!h) throw new Error(`Unregistered handler: "${name}"`);
		return h;
	}

	/** Looks up a registered matcher by name. Returns `undefined` if missing. */
	getMatcher(name: string): Matcher | undefined {
		return this.#matchers.get(name);
	}

	/** Like {@link getMatcher} but throws if the matcher is not registered. */
	requireMatcher(name: string): Matcher {
		const m = this.#matchers.get(name);
		if (!m) throw new Error(`Unregistered matcher: "${name}"`);
		return m;
	}

	/** List of all registered handler names. Stable order not guaranteed. */
	handlerNames(): string[] {
		return [...this.#handlers.keys()];
	}
}
