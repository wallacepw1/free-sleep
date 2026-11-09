
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof globalThis?globalThis:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="72e35219-3295-59fa-8afe-2a4a806c95ea")}catch(e){}}();
import express from 'express';
import { callFranken, connectFranken, FRANKEN_REQUEST_CONNECT_BUDGET_MS, FrankenRequestConnectBudgetError, } from '../../8sleep/frankenServer.js';
import { DeviceStatusSchema } from './deviceStatusSchema.js';
import logger from '../../logger.js';
import { updateDeviceStatus } from './updateDeviceStatus.js';
const router = express.Router();
router.get('/deviceStatus', async (req, res) => {
    try {
        const resp = await callFranken((franken) => franken.getDeviceStatus(), { connectBudgetMs: FRANKEN_REQUEST_CONNECT_BUDGET_MS });
        res.json(resp);
    }
    catch (error) {
        if (error instanceof FrankenRequestConnectBudgetError) {
            res.status(503).json({ error: error.message });
            return;
        }
        throw error;
    }
});
router.post('/deviceStatus', async (req, res) => {
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
    }
    catch (error) {
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
    await updateDeviceStatus(body);
    res.status(204).end();
});
export default router;
//# sourceMappingURL=deviceStatus.js.map
//# debugId=72e35219-3295-59fa-8afe-2a4a806c95ea
