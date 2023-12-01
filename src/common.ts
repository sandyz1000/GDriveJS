// ######################################
// ######### Setup File Config ##########
// ######################################

import { existsSync, readJSONSync, ensureDirSync, outputJson } from 'fs-extra';
import { join } from 'path';
import { transports as _transports, Logger } from 'winston';
import { Database } from 'sqlite3';
// setup oauth client
import google, { drive_v3 as drive, oauth2_v2 as _auth } from 'googleapis';

class Config {
	public cacheLocation: string;
	public advancedChunks: number;
	public chunkSize: number;
	public maxCacheSize: number;
	public refreshDelay: number;

	constructor(
		cacheLocation: string | null,
		advancedChunks: number | 0,
		chunkSize: number | 0, maxCacheSize: number | 0, refreshDelay: number | 0
	) {
		this.cacheLocation = cacheLocation || "/tmp/cache";
		this.advancedChunks = advancedChunks || 5;
		this.chunkSize = chunkSize || 1024 * 1024 * 16;
		this.maxCacheSize = maxCacheSize * 1024 * 1024 || 10737418240;
		this.refreshDelay = refreshDelay || 5000;
	}

	readFromFile = () => {
		let config = readJSONSync("config.json");
		for (let key in config) {
			this[key] = config[key];
		}
	}
}

let cfg = new Config(null, 0, 0, 0, 0);
const maxCache = ((config: Config) => {
	if (config.maxCacheSize) {
		return config.maxCacheSize;
	} else {
		logger.info("max cache size was not set. you should exit and manually set it");
		logger.info("defaulting to a 10 GB cache");
		return;
	}
})(cfg);


export const GDrive = drive({ version: 'v2' });
const OAuth2Client = _auth.OAuth2;
export const oauth2Client = new OAuth2Client(config.clientId || "520595891712-6n4r5q6runjds8m5t39rbeb6bpa3bf6h.apps.googleusercontent.com", config.clientSecret || "cNy6nr-immKnVIzlUsvKgSW8", config.redirectUrl || "urn:ietf:wg:oauth:2.0:oob");
oauth2Client.setCredentials(config.accessToken);
options({ auth: oauth2Client });

// ensure directory exist for upload, download and data folders
export const uploadLocation = join(config.cacheLocation, 'upload');
ensureDirSync(uploadLocation);
export const downloadLocation = join(config.cacheLocation, 'download');
ensureDirSync(downloadLocation);
export const dataLocation = join(config.cacheLocation, 'data');
ensureDirSync(dataLocation);

const printDate = () => {
	const d = new Date();
	return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}T${d.getHours()}:${d.getMinutes()}::${d.getSeconds()}`;
}
// setup winston logger

const transports = [new (_transports.File)({
	filename: '/tmp/GDriveF4JS.log',
	level: 'debug',
	maxsize: 10485760, //10mb
	maxFiles: 3
})];
if (config.debug)
	transports.push(new (_transports.Console)({ level: 'debug', timestamp: printDate, colorize: true }));
else
	transports.push(new (_transports.Console)({ level: 'info', timestamp: printDate, colorize: true }));

export const logger = new (Logger)({
	transports: transports
});

var lockRefresh = false;
export const refreshAccessToken = (cb) => {
	//if not locked, refresh access token
	if (lockRefresh) {
		cb();
		return;
	}

	lockRefresh = true
	oauth2Client.refreshAccessToken((err, tokens) => {
		if (err) {
			// logger.debug "There was an error with refreshing access token"
			// logger.debug err
			refreshAccessToken(cb);
			return;
		}

		config.accessToken = tokens;
		oauth2Client.setCredentials(tokens);
		outputJson('config.json', config, (err) => {
				if (err) {
					logger.debug("failed to save config");
				}
				lockRefresh = false;
				cb();
			});
	});
}

export const db = new Database(join(dataLocation, 'sqlite.db'));
export const currentLargestInode = 1;