import moment from 'moment-timezone';
import logger from '../logger.js';
import settingsDB from '../db/settings.js';
import { callFranken, Franken } from './frankenServer.js';
import { wait } from './promises.js';
import { DeviceStatus, Version } from '../routes/deviceStatus/deviceStatusSchema.js';
import { Side } from '../db/schedulesSchema.js';
import { Gesture, GestureSchema } from '../db/settingsSchema.js';
import { updateDeviceStatus } from '../routes/deviceStatus/updateDeviceStatus.js';
import { DeepPartial } from 'ts-essentials';
import serverStatus from '../serverStatus.js';



export class FrankenMonitor {
  private isRunning: boolean;
  private deviceStatus?: DeviceStatus;

  constructor() {
    this.isRunning = false;
    this.deviceStatus = undefined;
  }

  public async start() {
    if (this.isRunning) {
      logger.warn('FrankenMonitor is already running');
      return;
    }
    this.isRunning = true;
    this.frankenLoop().catch(error => {
      logger.error(error);
      serverStatus.status.frankenMonitor.status = 'failed';
      serverStatus.status.frankenMonitor.message = String(error);
      serverStatus.status.frankenMonitor.timestamp = moment.tz().format();
    });
  }

  public stop() {
    if (!this.isRunning) return;
    logger.debug('Stopping FrankenMonitor loop');
    this.isRunning = false;
  }

  private async processGesture(side: Side, gesture: Gesture) {
    const behavior = settingsDB.data[side].taps[gesture];
    if (behavior.type === 'temperature') {
      const currentTemperatureTarget = this.deviceStatus![side].targetTemperatureF;
      let newTemperatureTargetF;
      const change = behavior.amount;
      if (behavior.change === 'increment') {
        newTemperatureTargetF = currentTemperatureTarget + change;
      } else {
        newTemperatureTargetF = currentTemperatureTarget + (-1 * change);
      }
      logger.debug(`Processing gesture temperature change for ${side}. ${currentTemperatureTarget} -> ${newTemperatureTargetF}`);
      return await updateDeviceStatus({ [side]: { targetTemperatureF: newTemperatureTargetF } } as DeepPartial<DeviceStatus>);
    } else if (behavior.type) {
      // TODO: Add alarm handling
      logger.warn('Skipping gesture...');
    }
  }

  private processGesturesForSide(nextDeviceStatus: DeviceStatus, side: Side) {
    try {
      for (const gesture of GestureSchema.options) {
        if (nextDeviceStatus[side].taps?.[gesture] !== this?.deviceStatus?.[side].taps?.[gesture]) {
          this.processGesture(side, gesture);
        }
      }
    } catch (error) {
      logger.error(error);
    }
  }

  private async processGestures(nextDeviceStatus: DeviceStatus) {
    if (!this.deviceStatus) {
      logger.warn('Missing current deviceStatus, exiting...');
      return;
    }

    this.processGesturesForSide(nextDeviceStatus, 'left');
    this.processGesturesForSide(nextDeviceStatus, 'right');
  }


  private async frankenLoop() {
    // A failed first poll used to reject this loop forever. Keep scheduling the
    // same Pod 3 / gesture reads and let the next pass reconnect.
    while (this.isRunning) {
      let waitTime = 60_000;
      try {
        if (!this.deviceStatus) {
          const initialStatus = await callFranken((session: Franken) => session.getDeviceStatus(false));
          // Pod 3 is the identified cover without side taps, so it uses the slow poll.
          // Any other string, including "Version not found", keeps gesture reads.
          // This hub's beta cover has taps and must not be relabeled or slowed as Pod 3.
          const gesturesSupported = initialStatus.coverVersion !== Version.Pod3;
          waitTime = gesturesSupported ? 2_000 : 60_000;
          if (gesturesSupported) {
            const gesturedStatus = await callFranken((session: Franken) => session.getDeviceStatus(true));
            this.deviceStatus = gesturedStatus;
            logger.debug(`Gestures supported for ${gesturedStatus.coverVersion}`);
          } else {
            this.deviceStatus = initialStatus;
            logger.debug(`Gestures not supported for ${this.deviceStatus.coverVersion}`);
          }
        }

        while (this.isRunning && this.deviceStatus) {
          const gesturesSupported: boolean = this.deviceStatus.coverVersion !== Version.Pod3;
          waitTime = gesturesSupported ? 2_000 : 60_000;
          await wait(waitTime);
          if (!this.isRunning || !this.deviceStatus) break;
          const nextDeviceStatus: DeviceStatus = await callFranken(
            (session: Franken) => session.getDeviceStatus(gesturesSupported),
          );
          await settingsDB.read();
          if (gesturesSupported) {
            this.processGestures(nextDeviceStatus);
          }
          this.deviceStatus = nextDeviceStatus;
          serverStatus.status.frankenMonitor.status = 'healthy';
          serverStatus.status.frankenMonitor.message = '';
          serverStatus.status.frankenMonitor.timestamp = moment.tz().format();
        }
      } catch (error) {
        serverStatus.status.frankenMonitor.status = 'failed';
        serverStatus.status.frankenMonitor.message = String(error);
        serverStatus.status.frankenMonitor.timestamp = moment.tz().format();
        logger.error(error instanceof Error ? error.message : String(error), 'franken disconnected');
        await wait(waitTime);
      }
    }
    logger.debug('FrankenMonitor loop exited');
  }
}

