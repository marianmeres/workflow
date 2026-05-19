import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

await npmBuild({
	name: denoJson.name,
	version: denoJson.version,
	repository: denoJson.name.replace(/^@/, ""),
	dependencies: versionizeDeps(
		[
			"@marianmeres/clog",
			"@marianmeres/cron",
			"@marianmeres/fsm",
			"@marianmeres/migrate",
			"@marianmeres/steve",
			"pg",
			"@types/pg",
		],
		denoJson,
	),
});
