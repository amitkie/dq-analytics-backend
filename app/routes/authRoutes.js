const express = require('express');
const router = express.Router();
const authController =require('../controllers/authController')
const authService = require('../services/authService');

// POST /api/auth/register - Register a new user
router.post('/register', authController.createUser)

// POST /api/auth/login - Login an existing user (authentication)
router.post('/login', authController.loginUser);

router.post('/get-user-info', authController.getUserAndPaymentInfo);

router.post('/get-user-data', authController.getUserInfo);


module.exports = router;
