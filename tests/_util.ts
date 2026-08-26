/** Polls `predicate` every `intervalMs` until truthy or `timeoutMs` elapses. */
export async function waitUntil<T>(
	predicate: () => Promise<T | null | undefined | false>,
	{ timeoutMs = 10_000, intervalMs = 50 } = {},
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const v = await predicate();
		if (v) return v as T;
		await new Promise((r) => setTimeout(r, intervalMs));
	}
	throw new Error(`waitUntil: timed out after ${timeoutMs}ms`);
}
