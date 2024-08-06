const express = require('express');
const { createProject, getProjectByIdController, getProjectByUserIdController} = require('../controllers/projectController');
const router = express.Router();


router.post('/create-project', createProject );
router.get('/get-project', getProjectByIdController);
router.get('/get-project-by-user', getProjectByUserIdController);



module.exports = router;
