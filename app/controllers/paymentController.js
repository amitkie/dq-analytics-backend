const { doPayment } = require('../services/paymentService');
const { createErrorResponse } = require('../utils/errorResponse');
const { createSuccessResponse } = require('../utils/successResponse');

async function createPayment(req, res) {
    try {
        const paymentData = await doPayment(req.body);
        
        const successResponse = createSuccessResponse(201, 'Payment created successfully', paymentData);
        return res.status(201).json(successResponse);
    } catch (error) {
        console.error('Error in createPayment:', error);

        if (error.message === 'User not found') {
            const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
            return res.status(400).json(errorResponse);
        }

        const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
        return res.status(500).json(errorResponse);
    }
}

module.exports = {
    createPayment
};
