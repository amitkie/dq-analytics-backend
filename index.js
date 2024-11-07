const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const db = require("./app/models"); 
const authRoutes = require('./app/routes/authRoutes'); 
const userRoutes = require('./app/routes/userRoutes'); 
const paymentRoutes = require('./app/routes/paymentRoutes'); 
const masterRoutes = require('./app/routes/masterDataRoutes'); 
const projectRoutes = require('./app/routes/projectRoutes'); 

const app = express();

// var corsOptions = {
//   origin: '*'
// };

const corsOptions = {
  origin: ['https://m594bmgj-3000.inc1.devtunnels.ms/', 'http://localhost:3000', 'http://localhost:3000/', 'https://m594bmgj-8080.inc1.devtunnels.ms/', 'https://m594bmgj-8080.inc1.devtunnels.ms/api/v1/login'],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: ['*'],
  allowCredentials:true,
};
// const allowedOrigins = ['https://m594bmgj-3000.inc1.devtunnels.ms/', 'http://localhost:3000'];

// app.use(cors({
//   origin: (origin,callback) => {
//     if(allowedOrigins.includes(origin) || !origin){
//       callback(null, true);
//     }else{
//       callback(new Error('Not allowed by CORS'));
//     }
//   },
//   methods: ['POST', 'GET', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
//   credentials:true
// }))



// app.use(cors(corsOptions));

// app.options('*', cors(corsOptions));

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', 'https://m594bmgj-3000.inc1.devtunnels.ms');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') {
return res.sendStatus(200);
  }
  next();
});
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/api/v1', authRoutes);
app.use('/api/v1/users', userRoutes);
app.use('/api/v1/payments', paymentRoutes);
app.use('/api/v1/master', masterRoutes);
app.use('/api/v1/project', projectRoutes);

// Sync database
db.sequelize.sync()
  .then(() => {
    console.log("Database synced.");
  })
  .catch((err) => {
    // console.error("Failed to sync database:", err.message);
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
