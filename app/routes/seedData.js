const express = require('express');
const router = express.Router();

const Category = require('../../models/category');
const Benchmark = require('../../models/benchmark');
const Brand = require('../../models/brand');
const Frequency = require('../../models/frequency');
const Metrics = require('../../models/metrics');
const Platform = require('../../models/platform');

const {selectCategory, selectBenchmarks, selectBrands, selectFrequency, selectMetrics, selectPlatform} = require('../utils/masterData');

router.post('/', async (req,res) => {
    try {
        // Seed Categories
        await Category.insertMany(selectCategory);
    
        // Seed Brands
        await Brand.insertMany(selectBrands);
    
        // Seed Platforms
        await Platform.insertMany(selectPlatform);
    
        // Seed Metrics
        await Metrics.insertMany(selectMetrics);
    
        // Seed Frequency
        await Frequency.insertMany(selectFrequency);
    
        // Seed Benchmarks
        await Benchmark.insertMany(selectBenchmarks);
    
        res.status(200).json({ message: 'Data seeded successfully' });
      } catch (error) {
        res.status(500).json({ message: 'Failed to seed data', error: error.message });
      }    
})

module.exports = router;
