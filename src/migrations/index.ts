import { Migrate } from "@marianmeres/migrate";
import type pg from "pg";
import { down as down_1_0_0, up as up_1_0_0 } from "./1_0_0.ts";
import { down as down_1_1_0, up as up_1_1_0 } from "./1_1_0.ts";
import { down as down_1_2_0, up as up_1_2_0 } from "./1_2_0.ts";

const VERSION_TABLE = "__workflow_migrations";

/**
 * Builds a {@link Migrate} instance pre-loaded with the package's schema
 * versions. The caller drives it (`.up('latest')`, `.down(...)`, etc.).
 *
 * The active version is stored in `__workflow_migrations` (one row per change,
 * latest row wins).
 *
 * @example
 * ```ts
 * import pg from "pg";
 * import { createMigrate } from "@marianmeres/workflow";
 *
 * const pool = new pg.Pool({ ... });
 * const migrate = createMigrate(pool);
 * await migrate.up("latest");
 * ```
 */
export function createMigrate(
	pool: pg.Pool,
	options: { logger?: (...args: unknown[]) => void } = {},
): Migrate {
	const migrate = new Migrate(
		{
			getActiveVersion: getActiveVersion,
			setActiveVersion: setActiveVersion,
			logger: options.logger,
		},
		{ pool },
	);

	migrate.addVersion("1.0.0", up_1_0_0, down_1_0_0);
	migrate.addVersion("1.1.0", up_1_1_0, down_1_1_0);
	migrate.addVersion("1.2.0", up_1_2_0, down_1_2_0);

	return migrate;
}

async function ensureVersionTable(client: pg.PoolClient): Promise<void> {
	await client.query(
		`CREATE TABLE IF NOT EXISTS ${VERSION_TABLE} (
			id          serial      PRIMARY KEY,
			version     text        NULL,
			created_at  timestamptz NOT NULL DEFAULT now()
		)`,
	);
}

async function getActiveVersion(
	ctx: Record<string, unknown>,
): Promise<string | undefined> {
	const { pool } = ctx as unknown as { pool: pg.Pool };
	const client = await pool.connect();
	try {
		await ensureVersionTable(client);
		const r = await client.query<{ version: string | null }>(
			`SELECT version FROM ${VERSION_TABLE} ORDER BY id DESC LIMIT 1`,
		);
		return r.rows[0]?.version ?? undefined;
	} finally {
		client.release();
	}
}

async function setActiveVersion(
	version: string | undefined,
	ctx: Record<string, unknown>,
): Promise<string | undefined> {
	const { pool } = ctx as unknown as { pool: pg.Pool };
	const client = await pool.connect();
	try {
		await ensureVersionTable(client);
		await client.query(
			`INSERT INTO ${VERSION_TABLE} (version) VALUES ($1)`,
			[version ?? null],
		);
		return version;
	} finally {
		client.release();
	}
}
