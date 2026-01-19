# Multithreading and Connection Optimization for Orchestrator

## Problem Statement

The orchestrator (`orquestrador.py`) was designed to run multiple processes but was executing them sequentially, not in parallel. Additionally, database connections were being held open for extended periods, potentially causing Oracle session limit issues.

## Key Questions Addressed

1. **Can the orchestrator run multiple processes in parallel?**
   - Answer: No, not in the original implementation. Processes ran sequentially despite having logic to calculate how many to start.

2. **How can we implement multithreading?**
   - Solution: Use Python's `ThreadPoolExecutor` to run processes in parallel threads.

3. **How can we optimize database connection usage?**
   - Solution: Modify `DBDataManager` to open/close connections per operation instead of keeping them open.

## Architecture Analysis

### Original Sequential Implementation

```python
# Sequential execution - one after another
for i in range(processes_to_start):
    # Process 1 starts → waits → finishes
    # Process 2 starts → waits → finishes
    # Process 3 starts → waits → finishes
    run_batch_process(...)  # Blocks until complete
```

**Connection Usage:**
- 1 main orchestrator connection (always open)
- 1 connection per thread (kept open for entire thread lifetime)
- 1 SQLAlchemy connection per data_manager (kept open during batch process)
- Total: 7+ connections held for minutes at a time

### Multithreaded Implementation

```python
# Parallel execution - all at once
with ThreadPoolExecutor(max_workers=LOCAL_MAX_CONCURRENT_PROCESSES) as executor:
    futures = []
    for process_data in process_list:
        future = executor.submit(run_single_process_worker, ...)
        futures.append(future)
    
    # Wait for all to complete
    for future in as_completed(futures):
        future.result()
```

**Key Components:**

1. **ThreadSafeCounter** - Thread-safe counter for tracking active processes
   ```python
   class ThreadSafeCounter:
       def __init__(self):
           self._value = 0
           self._lock = Lock()
       
       def increment(self):
           with self._lock:
               self._value += 1
       
       def decrement(self):
           with self._lock:
               self._value -= 1
   ```

2. **Worker Function** - Each thread executes independently
   - Creates its own database connection (Oracle connections are NOT thread-safe)
   - Runs batch process
   - Updates status
   - Closes connection when done

3. **Connection Helpers** - Open/close connections on demand
   ```python
   def get_thread_connection():
       """Create a new database connection for a thread."""
       connection_object = ConnectionHandler()
       connection_object.connect_to_database()
       return connection_object.get_connection()
   
   def with_connection(func):
       """Context manager: opens connection, executes function, closes connection."""
       connection = None
       try:
           connection = get_thread_connection()
           return func(connection)
       finally:
           if connection:
               connection.close()
   ```

## Database Connection Optimization

### Modified DBDataManager Behavior

**File Modified:** `base_data_project/data_manager/managers/managers.py`

**Before (Persistent Connection):**
```python
def connect(self):
    self.engine = create_engine(db_url)
    Session = sessionmaker(bind=self.engine)
    self.session = Session()  # Created once, kept open

def load_data(self, entity, **kwargs):
    # Uses persistent self.session
    result = self.session.execute(text(query))
    return result

def disconnect(self):
    self.session.close()  # Closed only at end
    self.engine.dispose()
```

**After (Per-Operation Connection):**
```python
def connect(self):
    self.engine = create_engine(db_url)
    self._Session = sessionmaker(bind=self.engine)  # Factory, not session

def _get_session(self):
    """Create a new session for an operation."""
    return self._Session()

def _close_session(self, session):
    """Close session after operation."""
    session.close()

def load_data(self, entity, **kwargs):
    session = self._get_session()  # Open
    try:
        result = session.execute(text(query))
        return result
    finally:
        self._close_session(session)  # Close immediately
```

**Benefits:**
- Connections only open during actual database operations
- Connections close immediately after each query/insert
- Reduced concurrent connection count
- Lower risk of hitting Oracle `SESSIONS_PER_USER` limit

## Implementation Details

### Thread Safety Considerations

1. **Database Connections:**
   - Oracle connections (cx_Oracle) are NOT thread-safe
   - Each thread MUST have its own connection
   - Never share connection objects across threads

2. **Shared State:**
   - Use locks for shared variables (e.g., `local_processes` counter)
   - Database operations are generally safe (Oracle handles concurrent queries)
   - Connection objects themselves cannot be shared

3. **Error Handling:**
   - Errors in one thread should not crash others
   - Each thread handles its own exceptions
   - Use try/except/finally in worker function

### Connection Lifecycle Per Thread

```
Thread Start:
1. Open connection for setup queries → Close
2. Open connection for batch process → Keep open during execution → Close immediately after
3. Open connection for final status updates → Close

Total connection time: Only during actual operations (seconds), not entire thread lifetime (minutes)
```

### Performance Comparison

**Sequential (Before):**
- Process 1: 10 min
- Process 2: 5 min  
- Process 3: 8 min
- **Total: 23 minutes**

**Parallel (After):**
- Process 1: 10 min ┐
- Process 2: 5 min  ├─ All run simultaneously
- Process 3: 8 min  ┘
- **Total: 10 minutes (saves 13 minutes!)**

## Files Created/Modified

1. **`orquestrador_multithreaded_example.py`** - Complete multithreaded implementation
   - Thread-safe counter
   - Worker function with per-operation connections
   - ThreadPoolExecutor integration
   - Complete orchestrator logic

2. **`base_data_project/data_manager/managers/managers.py`** - Modified DBDataManager
   - Changed from persistent sessions to per-operation sessions
   - Added `_get_session()` and `_close_session()` methods
   - All database operations now open/close connections per query

## Usage

### Running the Multithreaded Orchestrator

```bash
python orquestrador_multithreaded_example.py [api_proc_id] [api_user]
```

Or with defaults:
```bash
python orquestrador_multithreaded_example.py
```

### Configuration Parameters

- `GLOBAL_MAX_CONCURRENT_PROCESSES` - Maximum processes across all orchestrators
- `LOCAL_MAX_CONCURRENT_PROCESSES` - Maximum threads in this orchestrator
- `MAX_RETRIES` - Number of retry attempts
- `RETRY_WAIT_TIME` - Wait time between retries

## Critical Requirements

1. **Separate Database Connections Per Thread**
   - Oracle connections are not thread-safe
   - Each thread creates its own connection
   - Connections are closed immediately after use

2. **Thread-Safe Shared State**
   - Use locks for counters and shared variables
   - Database queries are safe (Oracle handles concurrency)
   - Connection objects cannot be shared

3. **Error Isolation**
   - One thread's error shouldn't crash others
   - Each thread handles its own exceptions
   - Proper cleanup in finally blocks

4. **Wait for Completion**
   - Don't exit main loop until all threads complete
   - Use `ThreadPoolExecutor` context manager or `as_completed()`
   - Track thread completion with thread-safe counter

## Connection Count Optimization

### Before Optimization
- 1 main orchestrator connection (always open)
- 3 thread connections (open for entire thread lifetime, ~minutes each)
- 3 SQLAlchemy connections (from data_manager, open during batch process)
- **Total: 7 connections, many held for long periods**

### After Optimization
- 1 main orchestrator connection (always open)
- Thread connections: opened/closed per operation (setup, batch, final)
- SQLAlchemy connections: opened/closed per query/insert
- **Total: Typically 4-5 connections at peak, connections closed quickly**

## Key Takeaways

1. **Multithreading enables true parallel execution** - Processes run simultaneously instead of sequentially
2. **Per-operation connections reduce resource usage** - Connections only open when needed
3. **Thread safety is critical** - Each thread needs its own database connection
4. **Connection lifecycle matters** - Close connections immediately after use, not at thread end
5. **Performance improvement** - Parallel execution can reduce total time from sum of all processes to max of longest process

## Testing Recommendations

1. Test with `LOCAL_MAX_CONCURRENT_PROCESSES = 1` first (sequential behavior)
2. Gradually increase to 2, 3, etc.
3. Monitor Oracle session count during execution
4. Verify all processes complete successfully
5. Check that connections are properly closed (no leaks)

## Future Enhancements

1. Connection pooling for better performance
2. Retry logic for failed threads
3. Better error reporting and aggregation
4. Metrics collection (execution time, connection usage, etc.)
5. Dynamic thread pool sizing based on system load
