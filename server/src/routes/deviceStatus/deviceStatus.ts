import express, { Request, Response } from 'express';
import {
  callFranken,
  connectFranken,
  FRANKEN_REQUEST_CONNECT_BUDGET_MS,
  FrankenRequestConnectBudgetError,
} from '../../8sleep/frankenServer.js';
import { DeviceStatus, DeviceStatusSchema } from './deviceStatusSchema.js';
import logger from '../../logger.js';
import { updateDeviceStatus } from './updateDeviceStatus.js';
import { DeepPartial } from 'ts-essentials';

const router = express.Router();

router.get('/deviceStatus', async (req: Request, res: Response) => {
  try {
    const resp = await callFranken(
      (franken) => franken.getDeviceStatus(),
      { connectBudgetMs: FRANKEN_REQUEST_CONNECT_BUDGET_MS },
    );
    res.json(resp);
  } catch (error) {
    if (error instanceof FrankenRequestConnectBudgetError) {
      res.status(503).json({ error: error.message });
      return;
    }
    throw error;
  }
});


router.post('/deviceStatus', async (req: Request, res: Response) => {
  const { body } = req;
  const validationResult = DeviceStatusSchema.deepPartial().safeParse(body);
  if (!validationResult.success) {
    logger.error('Invalid device status update:', validationResult.error);
    res.status(400).json({
      error: 'Invalid request data',
      details: validationResult?.error?.errors,
    });
    return;
  }

  let franken;
  try {
    franken = await connectFranken({ connectBudgetMs: FRANKEN_REQUEST_CONNECT_BUDGET_MS });
  } catch (error) {
    if (error instanceof FrankenRequestConnectBudgetError) {
      res.status(503).json({ error: error.message });
      return;
    }
    throw error;
  }
  // The budget can lose to a connect that closes before this line. Do not send
  // the update on that session, and do not leave this handler queued behind it.
  if (franken.isClosed) {
    res.status(503).json({ error: 'Franken disconnected before the update was sent' });
    return;
  }
  await updateDeviceStatus(body as DeepPartial<DeviceStatus>);
  res.status(204).end();
});


export default router;
