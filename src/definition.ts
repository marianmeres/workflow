import type { NodeMeta, WorkflowDefinition } from "./types.ts";

/**
 * Validates a workflow definition against a set of registered handler/matcher
 * names. Throws on any structural problem.
 *
 * Checks:
 * - id and version are non-empty strings
 * - fsm.initial exists in fsm.states
 * - every state has a `meta` field with a known `kind`
 * - every `effectful` state names a handler that is in `availableHandlers`
 * - every `suspending` state's matcher (if present) is in `availableMatchers`
 * - every transition target string resolves to a defined state
 * - at least one terminal state exists
 */
export function validateDefinition(
	def: WorkflowDefinition,
	available: {
		handlers: ReadonlySet<string>;
		matchers: ReadonlySet<string>;
	},
): void {
	if (!def.id || typeof def.id !== "string") {
		throw new Error(`Workflow definition: id must be a non-empty string`);
	}
	if (!def.version || typeof def.version !== "string") {
		throw new Error(
			`Workflow definition "${def.id}": version must be a non-empty string`,
		);
	}
	if (!def.fsm || typeof def.fsm !== "object") {
		throw new Error(`Workflow definition "${def.id}@${def.version}": missing fsm`);
	}
	if (!def.fsm.initial || typeof def.fsm.initial !== "string") {
		throw new Error(
			`Workflow definition "${def.id}@${def.version}": fsm.initial must be a non-empty string`,
		);
	}
	if (!def.fsm.states || typeof def.fsm.states !== "object") {
		throw new Error(
			`Workflow definition "${def.id}@${def.version}": fsm.states must be an object`,
		);
	}
	if (!(def.fsm.initial in def.fsm.states)) {
		throw new Error(
			`Workflow definition "${def.id}@${def.version}": initial state "${def.fsm.initial}" not in states`,
		);
	}

	const stateNames = new Set(Object.keys(def.fsm.states));
	let terminalCount = 0;

	for (const [stateName, stateCfg] of Object.entries(def.fsm.states)) {
		const meta = stateCfg.meta as NodeMeta | undefined;
		if (!meta || typeof meta !== "object") {
			throw new Error(
				`Workflow definition "${def.id}@${def.version}": state "${stateName}" is missing meta`,
			);
		}
		const kind = (meta as { kind?: string }).kind;
		switch (kind) {
			case "pure":
				break;
			case "effectful": {
				const h = (meta as { handler?: string }).handler;
				if (!h || typeof h !== "string") {
					throw new Error(
						`Workflow definition "${def.id}@${def.version}": ` +
							`effectful state "${stateName}" is missing a handler`,
					);
				}
				if (!available.handlers.has(h)) {
					throw new Error(
						`Workflow definition "${def.id}@${def.version}": ` +
							`state "${stateName}" references unregistered handler "${h}"`,
					);
				}
				break;
			}
			case "suspending": {
				const m = (meta as { matcher?: string }).matcher;
				if (m !== undefined) {
					if (typeof m !== "string") {
						throw new Error(
							`Workflow definition "${def.id}@${def.version}": ` +
								`state "${stateName}" matcher must be a string`,
						);
					}
					if (!available.matchers.has(m)) {
						throw new Error(
							`Workflow definition "${def.id}@${def.version}": ` +
								`state "${stateName}" references unregistered matcher "${m}"`,
						);
					}
				}
				break;
			}
			case "terminal":
				terminalCount++;
				break;
			default:
				throw new Error(
					`Workflow definition "${def.id}@${def.version}": ` +
						`state "${stateName}" has unknown meta.kind "${String(kind)}"`,
				);
		}

		// transition target validation
		const on = stateCfg.on as
			| Record<string, string | { target?: string } | Array<{ target?: string }>>
			| undefined;
		for (const [eventName, transition] of Object.entries(on ?? {})) {
			const targets: Array<string | undefined> = [];
			if (typeof transition === "string") {
				targets.push(transition);
			} else if (Array.isArray(transition)) {
				for (const t of transition) targets.push(t.target);
			} else if (transition && typeof transition === "object") {
				targets.push(transition.target);
			}
			for (const t of targets) {
				if (t === undefined) continue; // internal transition (action-only)
				if (!stateNames.has(t)) {
					throw new Error(
						`Workflow definition "${def.id}@${def.version}": ` +
							`state "${stateName}" on event "${eventName}" targets unknown state "${t}"`,
					);
				}
			}
		}
	}

	if (terminalCount < 1) {
		throw new Error(
			`Workflow definition "${def.id}@${def.version}": ` +
				`needs at least one terminal state`,
		);
	}
}

/** Composite key used in the definition registry. */
export function defKey(id: string, version: string): string {
	return `${id}@${version}`;
}
