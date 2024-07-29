class AppError extends Error {
    constructor(code, message, statusCode, details = {}) {
      super(message);
      this.code = code;
      this.statusCode = statusCode;
      this.details = details;
      this.isOperational = true;
      Error.captureStackTrace(this, this.constructor);
    }
  }
  
  class ValidationError extends AppError {
    constructor(code, message, details = {}) {
      super(code, message, 400, details);
    }
  }
  
  class AuthenticationError extends AppError {
    constructor(code, message, details = {}) {
      super(code, message, 401, details);
    }
  }
  
  class NotFoundError extends AppError {
    constructor(code, message, details = {}) {
      super(code, message, 404, details);
    }
  }
  
  module.exports = { AppError, ValidationError, AuthenticationError, NotFoundError };
  