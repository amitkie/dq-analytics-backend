const express = require('express');
const { createProject, getProjectByIdController} = require('../controllers/projectController');
const router = express.Router();


router.post('/create-project', createProject );
router.get('/get-projects', getProjectByIdController);



module.exports = router;
