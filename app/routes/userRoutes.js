const express = require('express');
const { userActivityTracker, updateDBCreationStatus, updateSchemaTableCreationStatus, getDBAndSchemaTableCreationStatus } = require('../controllers/userController');
const router = express.Router();


router.post('/get-user-activity', userActivityTracker );
router.put('/update-db-status/:id', updateDBCreationStatus);
router.put('/update-schema-table-status/:id', updateSchemaTableCreationStatus);
router.get('/get-db-schema-status/:id', getDBAndSchemaTableCreationStatus);


module.exports = router;
