
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="5004a521-91ad-5734-bd37-381e32d92085")}catch(e){}}();
import * as Sentry from '@sentry/node';
import logger from './logger.js';
import { callFranken } from './8sleep/frankenServer.js';
import './jobs/jobScheduler.js';
import settingsDB from './db/settings.js';
export async function setupSentryTags() {
    logger.debug('Setting up sentry tags');
    const deviceStatus = await callFranken((franken) => franken.getDeviceStatus());
    Sentry.setUser({ id: settingsDB.data.id });
    Sentry.setTag('hub_version', deviceStatus.hubVersion);
    Sentry.setTag('cover_version', deviceStatus.coverVersion);
    logger.debug('Set up sentry tags');
}
//# sourceMappingURL=setupSentryTags.js.map
//# debugId=5004a521-91ad-5734-bd37-381e32d92085
