
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="6bbda9fe-4ede-51c0-8cae-2f5623cce23a")}catch(e){}}();
import _ from 'lodash';
import { callFranken } from './frankenServer.js';
import logger from '../logger.js';
export const frankenCommands = {
    HELLO: '0',
    SET_TEMP: '1',
    SET_ALARM: '2',
    // RESET: '3',
    // FORCE_RESET: '4',
    ALARM_LEFT: '5',
    ALARM_RIGHT: '6',
    // FORMAT: '7',
    SET_SETTINGS: '8',
    LEFT_TEMP_DURATION: '9',
    RIGHT_TEMP_DURATION: '10',
    TEMP_LEVEL_LEFT: '11',
    TEMP_LEVEL_RIGHT: '12',
    PRIME: '13',
    DEVICE_STATUS: '14',
    ALARM_CLEAR: '16',
    // ALARM_SOLO: "17",
    // STOP_PRIME: "18",
};
export const invertedFrankenCommands = _.invert(frankenCommands);
export async function executeFunction(command, arg = 'empty') {
    logger.debug(`Executing command | command: ${command} | arg: ${arg}`);
    // Retry only when this command was not written. A timeout means the write may
    // already have reached the device, and a resent command would have no reply id.
    const response = await callFranken((franken) => franken.callFunction(command, arg));
    logger.debug(response);
    return response;
}
//# sourceMappingURL=deviceApi.js.map
//# debugId=6bbda9fe-4ede-51c0-8cae-2f5623cce23a
