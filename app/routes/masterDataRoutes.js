const express = require('express');
const { getAllCategoriesController, getAllBrandsController, getAllBenchmarkController, getAllPlatformController, getAllMetricsController, getAllFrequencyController, getBrandsByCategoryIdController, getAllSectionController, getPlatformsBySectionIdController, getMetricsByPlatformIdController } = require('../controllers/masterDataController');

const router = express.Router();

router.get('/get-all-brands', getAllBrandsController);
router.get('/get-all-categories', getAllCategoriesController);
router.get('/get-all-categories-by-brand-id/:category_id', getBrandsByCategoryIdController);
router.get('/get-all-sections', getAllSectionController);
router.get('/get-all-platforms', getAllPlatformController);
router.get('/get-all-platforms-by-section-id/:section_id', getPlatformsBySectionIdController);
router.get('/get-all-metrics', getAllMetricsController);
router.get('/get-all-metrics-by-platform-id/:platform_id', getMetricsByPlatformIdController);
router.get('/get-all-benchmarks', getAllBenchmarkController);
router.get('/get-all-frequencies', getAllFrequencyController);

module.exports = router;