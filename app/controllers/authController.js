const authService = require("../services/authService");
const { ValidationError } = require("../handlers/errorHandler");
const { createErrorResponse } = require("../utils/errorResponse");
const { createSuccessResponse } = require("../utils/successResponse");

const createUser = async (req, res) => {
    try {
      const response = await authService.registerUser(req.body);
   
      const successResponse = createSuccessResponse(200, 'User registered successfully', response )
      return res.status(200).json(successResponse);
    } catch (error) {
      if (error instanceof ValidationError) {
        const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message, error.errors);
        return res.status(400).json(errorResponse);
      }
      const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error', error.message);
      return res.status(500).json(errorResponse);
    }
  };

  const loginUser = async (req, res) => {
    try {
      const response = await authService.authenticateUser(req.body);
  
      if (response.error) {
        if (response.error === 'User not found') {
          const errorResponse = createErrorResponse(404, 'USER_NOT_FOUND', response.error);
          return res.status(404).json(errorResponse);
        } else if (response.error === 'Credentials are incorrect') {
          const errorResponse = createErrorResponse(401, 'INVALID_CREDENTIALS', response.error);
          return res.status(401).json(errorResponse);
        }
      }
  
      const successResponse = createSuccessResponse(200, "User logged in successfully", response);
      return res.status(200).json(successResponse);
  
    } catch (error) {
  
      if (error instanceof ValidationError) {
        const errorResponse = createErrorResponse(400, error.code, error.message);
        return res.status(400).json(errorResponse);
      }
  
      const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
      return res.status(500).json(errorResponse);
    }
  };
  

const getUserAndPaymentInfo = async (req,res) => {
    try {
        const response = await authService.getUserAndPaymentInfo(req.body);
        const successResponse = createSuccessResponse(200, "User data found successfully", response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}

const getUserInfo = async (req,res) => {
    try {
        const response = await authService.getUserInfo(req.body);
        const successResponse = createSuccessResponse(200, "User data found successfully", response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}

module.exports = {createUser, loginUser, getUserAndPaymentInfo, getUserInfo};