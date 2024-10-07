const express = require('express');
const { createProject,updateProject,deleteProject, getProjectByIdController, createUserProjectDQScore, getProjectByUserIdController, createUrl, getUrl, saveMetrics, checkProjectName, getProject, removeMetricFromProject} = require('../controllers/projectController');
const router = express.Router();


router.post('/create-project', createProject );
router.patch('/projects/:projectId', updateProject);
router.delete('/projects/:projectId', deleteProject);
router.get('/check-project-name', checkProjectName);
router.get('/get-project', getProjectByIdController);
router.get('/get-project-by-user', getProjectByUserIdController);
router.post('/save-metrics', saveMetrics);
router.delete('/remove-metric/:projectId/metrics/:metricId', removeMetricFromProject);
router.post('/create-url', createUrl);
router.post('/create-user-project-dq-score', createUserProjectDQScore);
router.get('/get-url', getUrl);
router.get('/projects-by-id', getProject);




module.exports = router;
