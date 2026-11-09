import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dashboardDirectory = path.dirname(fileURLToPath(import.meta.url));
const sourceDirectory = path.join(dashboardDirectory, 'sources', 'oss_pulse');
const target = (process.env.DBT_TARGET ?? 'dev').toLowerCase();

if (target !== 'dev' && target !== 'prod') {
	throw new Error(`Unsupported DBT_TARGET "${target}"; use "dev" or "prod".`);
}

let connection;
if (target === 'prod') {
	connection = {
		type: 'motherduck',
		name: 'oss_pulse',
		options: {
			token: '',
			database: ''
		}
	};
} else {
	const repositoryRoot = path.resolve(dashboardDirectory, '..');
	const databasePath = path.resolve(
		repositoryRoot,
		process.env.DUCKDB_PATH ?? 'data/oss_pulse.duckdb'
	);
	connection = {
		type: 'duckdb',
		name: 'oss_pulse',
		options: {
			filename: path.relative(sourceDirectory, databasePath)
		}
	};
}

await mkdir(sourceDirectory, { recursive: true });
await writeFile(
	path.join(sourceDirectory, 'connection.yaml'),
	`${JSON.stringify(connection, null, 2)}\n`,
	'utf8'
);
