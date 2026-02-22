# Postman Testing Guide - JA Assure RAG API

## ✅ API Status: RUNNING
- **URL**: http://localhost:8000
- **Status**: Ready to accept requests

## Quick Setup in Postman

### Step 1: Test Health Endpoint

1. Open Postman
2. Click "New" → "HTTP Request"
3. Set method to **GET**
4. Enter URL: `http://localhost:8000/health`
5. Click **Send**

**Expected Response (200 OK):**
```json
{
  "status": "ok"
}
```

---

### Step 2: Test Query Endpoint

1. Create a new request or change the existing one
2. Set method to **POST**
3. Enter URL: `http://localhost:8000/query`
4. Click the **Body** tab
5. Select **raw**
6. Select **JSON** from the dropdown (to the right)
7. Paste this JSON:

```json
{
  "question": "What is the business name of MYJADEQT001?"
}
```

8. Click **Send**

**Expected Response (200 OK):**
```json
{
  "question": "What is the business name of MYJADEQT001?",
  "answer": "Business Name for MYJADEQT001: Ja Assure IN"
}
```

---

## 8 Sample Queries to Test

Copy these JSON payloads into Postman's Body tab (raw JSON):

### 1. Business Name Lookup
```json
{
  "question": "What is the business name of MYJADEQT001?"
}
```

### 2. Count Query (CCTV Maintenance)
```json
{
  "question": "How many proposals have CCTV maintenance contracts?"
}
```
Expected: `8 proposal(s) match the criteria.`

### 3. Entity Lookup (Person Name)
```json
{
  "question": "What type of business does Suresh Kumar run?"
}
```

### 4. Door Access Type
```json
{
  "question": "What is the door access type used by Global Money Exchange?"
}
```

### 5. Armed Guards Check
```json
{
  "question": "Does Rapid FX Money Exchange use armed guards during transit?"
}
```

### 6. Background Checks
```json
{
  "question": "What background checks does LuxGold Jewellers do?"
}
```

### 7. Claim History
```json
{
  "question": "What is the claim history of Heritage Gold and Jewels?"
}
```

### 8. CCTV Backup Type
```json
{
  "question": "What type of CCTV backup does Secure Pawn Brokers use?"
}
```

---

## Testing Conversation History (Follow-up Queries)

Run these in sequence to test conversation context:

**Query 1:**
```json
{
  "question": "How many proposals have CCTV maintenance contracts?"
}
```

**Query 2 (follow-up):**
```json
{
  "question": "Give me their names"
}
```

The system should return the business names of proposals WITH CCTV maintenance.

---

## Advanced Test Cases

### Location Query
```json
{
  "question": "How many proposals are located in Penang?"
}
```

### Count with Feature
```json
{
  "question": "How many proposals have display windows?"
}
```

### Multiple Features
```json
{
  "question": "How many proposals have armed guards?"
}
```

### Stock Records
```json
{
  "question": "Does Royal Gems and Jewels keep detailed records of stock movements?"
}
```

---

## Error Testing

### Empty Question (should return 400)
```json
{
  "question": ""
}
```

### Invalid JSON (should return 422)
```
not valid json
```

---

## Expected Response Times

- **Health check**: < 100ms
- **First query**: 5-15 seconds (LLM processing)
- **Subsequent queries**: 5-15 seconds
- **Deterministic count queries**: 1-3 seconds (bypass LLM)

---

## Postman Collection Tips

### Save as Collection
1. Click "Save" on your request
2. Name it "JA Assure RAG API Tests"
3. Create folders for different query types:
   - Health Check
   - Lookups
   - Counts
   - Entity Queries
   - Follow-ups

### Use Environment Variables
1. Create environment: "JA Assure Local"
2. Add variable: `base_url` = `http://localhost:8000`
3. Use in requests: `{{base_url}}/query`

### Save Test Scripts
In Postman, add this test script (Tests tab):

```javascript
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Response has question and answer", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData).to.have.property('question');
    pm.expect(jsonData).to.have.property('answer');
});

pm.test("Answer is not empty", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData.answer.length).to.be.above(0);
});
```

---

## Troubleshooting

### API Not Responding
Check if server is running:
```bash
curl http://localhost:8000/health
```

If not running, restart:
```bash
cd /Users/admin22/Documents/Coding/ja-assure_rag
source venv/bin/activate
uvicorn api:app --reload
```

### Check Server Logs
```bash
tail -f api_server.log
```

### Stop the Server
Find and kill the process:
```bash
ps aux | grep uvicorn
kill <PID>
```

Or use:
```bash
pkill -f "uvicorn api:app"
```

---

## Interactive API Documentation

FastAPI provides automatic documentation at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

You can test queries directly in the browser using these interfaces!

---

## Current Status

✅ API server is running (PID: 19691)  
✅ Health endpoint working  
✅ Query endpoint working  
✅ All components initialized  
✅ Ready for Postman testing  

**Start testing now!** 🚀
