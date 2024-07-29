const userService = require("../services/userService")
const { ValidationError } = require("../handlers/errorHandler");
const { createErrorResponse } = require("../utils/errorResponse");
const { createSuccessResponse } = require("../utils/successResponse");


const userActivityTracker = async (req, res) => {
    try {
        const response = await userService.userActivityTracker(req.body);
      
        const successResponse = createSuccessResponse(200, 'User activity tracked successfully', response)
        return res.status(200).json(successResponse);
    } catch (error) {
        console.error('Error in userActivityTracker:', error);
        if (error.message.includes('count has reached its limit')) {
            const errorResponse = createErrorResponse(400, 'LIMIT_REACHED', error.message);
            return res.status(400).json(errorResponse);
        }
        if (error.message === 'User not found' || error.message === 'User activity not found' || error.message === 'Invalid tab name') {
            const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
            return res.status(400).json(errorResponse);
        }
        const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
        return res.status(500).json(errorResponse);
    }
};

const updateDBCreationStatus = async (req, res) => {
    try {
        const response = await userService.updateDBCreationStatus(req.body);
      
        const successResponse = createSuccessResponse(200, 'Database creation status updated successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        console.error('Error in userActivityTracker:', error);
        if (error.message.includes('count has reached its limit')) {
            const errorResponse = createErrorResponse(400, 'LIMIT_REACHED', error.message);
            return res.status(400).json(errorResponse);
        }
        if (error.message === 'User not found' || error.message === 'User activity not found' || error.message === 'Invalid tab name') {
            const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
            return res.status(400).json(errorResponse);
        }
        const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
        return res.status(500).json(errorResponse);
    }
};

const updateSchemaTableCreationStatus = async (req, res) => {
    try {
        const response = await userService.updateDBCreationStatus(req.body);
      
        const successResponse = createSuccessResponse(200, 'Schema and table creation status updated successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        console.error('Error in userActivityTracker:', error);
        if (error.message.includes('count has reached its limit')) {
            const errorResponse = createErrorResponse(400, 'LIMIT_REACHED', error.message);
            return res.status(400).json(errorResponse);
        }
        if (error.message === 'User not found' || error.message === 'User activity not found' || error.message === 'Invalid tab name') {
            const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
            return res.status(400).json(errorResponse);
        }
        const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
        return res.status(500).json(errorResponse);
    }
};
const getDBAndSchemaTableCreationStatus = async (req, res) => {
    try {
        const response = await userService.getDBAndSchemaTableCreationStatusService(req.params.id);
      
        const successResponse = createSuccessResponse(200, 'Database and schema/table creation status fetched successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        console.error('Error in getDBAndSchemaTableCreationStatus:', error);
        const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
        return res.status(500).json(errorResponse);
    }
};



module.exports = { userActivityTracker,updateDBCreationStatus , updateSchemaTableCreationStatus, getDBAndSchemaTableCreationStatus };
