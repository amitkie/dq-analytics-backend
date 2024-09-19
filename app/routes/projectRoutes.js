const express = require('express');
const { createProject, getProjectByIdController, getProjectByUserIdController, createUrl, getUrl, saveMetrics, checkProjectName} = require('../controllers/projectController');
const router = express.Router();


router.post('/create-project', createProject );
router.get('/check-project-name', checkProjectName);
router.get('/get-project', getProjectByIdController);
router.get('/get-project-by-user', getProjectByUserIdController);
router.post('/save-metrics', saveMetrics)
router.post('/create-url', createUrl);
router.get('/get-url', getUrl);




module.exports = router;
