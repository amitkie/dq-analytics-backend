module.exports = {
  HOST: "detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com",
  USER: "frontend",
  PASSWORD: "frontendkie123",
  DB: "KIESQUAREDE",
  PORT: 5434, // Ensure this is the correct port
  dialect: "postgres",
  pool: {
    max: 5,
    min: 0,
    acquire: 30000,
    idle: 10000
  },
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false // For development; set to true for production
    }
  }
};
