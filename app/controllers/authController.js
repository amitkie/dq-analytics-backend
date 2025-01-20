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

const sendFeedback = async (req, res) => {
    try {
        const { firstName, lastName, email, requestType, comments } = req.body;

        // Validate input
        if (!firstName || !lastName || !email || !requestType || !comments) {
            return res.status(400).json({ message: "All fields are required." });
        }

        // Call the service
        await authService.sendFeedbackEmail({ firstName, lastName, email, requestType, comments });

        res.status(200).json({ message: "Feedback sent successfully!" });
    } catch (error) {
        console.error("Error in sendFeedback:", error);
        res.status(500).json({ message: "Failed to send feedback." });
    }
};

const demoScheduler = async (req, res) => {
    try {
        const { email } = req.body;

        // Validate input
        if ( !email ) {
            return res.status(400).json({ message: "Email field is required." });
        }

        // Call the service
        await authService.demoScheduler({  email});

        res.status(200).json({ message: "Feedback sent successfully!" });
    } catch (error) {
        console.error("Error in sendFeedback:", error);
        res.status(500).json({ message: "Failed to send feedback." });
    }
};

module.exports = {createUser, loginUser, getUserAndPaymentInfo, getUserInfo, sendFeedback, demoScheduler};