# SQLite Web API

A simple JSON HTTP API for querying SQLite databases with read-only access.

## API Endpoints

### POST /query

Execute a SQL query and return results as JSON.

**Request:**

```json
{
  "sql": "SELECT * FROM users WHERE age > 30"
}
```

**Response:**

```json
{
  "success": true,
  "result": {
    "columns": ["id", "name", "email", "age"],
    "rows": [
      [2, "Bob Smith", "bob@example.com", 35],
      [3, "Charlie Brown", "charlie@example.com", 42]
    ],
    "query_time_ms": 15
  },
  "error": null
}
```

### GET /health

Check server health status.

**Response:**

```json
{
  "status": "healthy",
  "max_query_time_ms": 5000
}
```

## License

MIT
