const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const db = require("./app/models"); 
const authRoutes = require('./app/routes/authRoutes'); 
const userRoutes = require('./app/routes/userRoutes'); 
const paymentRoutes = require('./app/routes/paymentRoutes'); 
const masterRoutes = require('./app/routes/masterDataRoutes'); 

const app = express();

var corsOptions = {
  origin: '*'
};

app.use(cors(corsOptions));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/api/v1', authRoutes);
app.use('/api/v1/users', userRoutes);
app.use('/api/v1/payments', paymentRoutes);
app.use('/api/v1/master', masterRoutes);

// Sync database
db.sequelize.sync()
  .then(() => {
    console.log("Database synced.");
  })
  .catch((err) => {
    console.error("Failed to sync database:", err.message);
  });

// Define routes
app.get("/", (req, res) => {
  res.json({ message: "Welcome to your application." });
});

// Routes for authentication

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}.`);
});
