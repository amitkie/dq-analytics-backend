const express = require('express');
const { createProject } = require('../controllers/projectController');
const router = express.Router();


router.post('/create-project', createProject );


module.exports = router;
