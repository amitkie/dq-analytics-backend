const createSuccessResponse = (statusCode, message, data = {}) => {
    return {
      status: 'success',
      statusCode,
      message,
      data,
      timestamp: new Date().toISOString()
    };
  };
  
  module.exports = { createSuccessResponse };
  