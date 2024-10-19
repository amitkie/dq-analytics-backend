const getMonthNumberFromString = (monthStr) => {
    // Define a mapping from month names to their numeric values
    const monthMap = {
      Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
      Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
    };
  
    // Return the corresponding numeric value (0-indexed) or -1 if not found
    return monthMap[monthStr] !== undefined ? monthMap[monthStr] : -1;
  };

module.exports = {
    getMonthNumberFromString
}  