use employeeattendancetracker;

// Insert employee notes
db.employee_notes.insertMany([
  {
    employee_id: 1,
    department: "IT",
    note: "Delivered the login module ahead of time. Well documented.",
    timestamp: new Date("2025-06-01T17:45:00Z")
  },
  {
    employee_id: 2,
    department: "HR",
    note: "Positive engagement with candidates, but needs to improve punctuality.",
    timestamp: new Date("2025-06-01T18:15:00Z")
  },
  {
    employee_id: 3,
    department: "Finance",
    note: "Accurate reporting with a minor delay due to dependency on external data.",
    timestamp: new Date("2025-06-01T17:05:00Z")
  }
]);


// Index Creation

db.employee_notes.createIndex({ employee_id: 1 });
db.employee_notes.createIndex({ department: 1 });
