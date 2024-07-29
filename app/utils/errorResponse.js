const createErrorResponse = (statusCode, errorCode, message, details = {}) => {
  return {
    status: 'error',
    statusCode,
    errorCode,
    message,
    details,
    timestamp: new Date().toISOString()
  };
};

module.exports = { createErrorResponse };