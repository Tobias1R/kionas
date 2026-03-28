# Redis Session Storage & Management - Analysis Report

**Date**: March 27, 2026  
**Scope**: Session storage patterns, serialization, TTL management, and cleanup mechanisms

---

## 1. Key Pattern & Naming Convention

### Prefix Pattern: `kionas:session`
```
kionas:session:{session_id}
```

**Example**: `kionas:session:e11fa5c1-1e30-a685-b8df-c2833a2abeeb`

**Defined in**: [server/src/session.rs](server/src/session.rs) line 9
```rust
const SESSION_KEY_PREFIX: &str = "kionas:session";
```

**Key Functions**:
- `session_key(id)` → `kionas:session:{id}`
- `session_key_pattern()` → `kionas:session:*` (for pattern matching)

---

## 2. Session Data Serialization

### Format: JSON
Sessions are serialized to JSON using `serde_json`.

**Struct Definition** ([server/src/session.rs](server/src/session.rs) lines 34-48):
```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    auth_token: String,
    is_authenticated: bool,
    role: String,
    warehouse_name: String,          // aliased as "warehouse"
    pool_members: Vec<String>,       // worker pool snapshot
    remote_addr: String,
    last_active: u64,
    use_database: String,
}
```

**Serialization/Deserialization**:
- **Serialize**: `serde_json::to_string(&session)` ([server/src/session.rs](server/src/session.rs) line 208)
- **Deserialize**: `serde_json::from_str::<Session>(&value)` ([server/src/session.rs](server/src/session.rs) line 216)

---

## 3. Session Metadata Stored with Each Session

| Field | Type | Purpose | Notes |
|-------|------|---------|-------|
| `id` | String | Session unique identifier | UUID format |
| `auth_token` | String | Authentication token | Used for `get_token_session()` lookup |
| `is_authenticated` | bool | Authentication state | Default: `false` |
| `role` | String | User role/permissions | Set during session creation |
| `warehouse_name` | String | Assigned warehouse/pool | Can be updated via `set_warehouse()` |
| `pool_members` | Vec<String> | Worker snapshot | Stable during session lifetime |
| `remote_addr` | String | Client IP/source address | Captured at session creation |
| `last_active` | u64 | Last activity timestamp (Unix) | Updated via `update_last_active()` |
| `use_database` | String | Current database context | Default: `"default"` |

---

## 4. Session TTL & Cleanup Mechanisms

### Current TTL Configuration
**Default TTL**: 7 days (604,800 seconds)

**Configuration** ([kionas/src/constants.rs](kionas/src/constants.rs) lines 37-38):
```rust
pub const REDIS_SESSION_TTL_SECONDS: u64 = 604_800;        // 7 days
pub const REDIS_SESSION_TTL_ENV: &str = "KIONAS_SESSION_TTL_SECONDS";
```

**Environment Override**: `KIONAS_SESSION_TTL_SECONDS` (see [server/src/session.rs](server/src/session.rs) lines 23-27)

### TTL Implementation Strategy: SETEX (Atomic)
Sessions use Redis **SETEX** command (set + expire in one atomic operation):

**Implementation** ([server/src/session.rs](server/src/session.rs)):
- **Add session**: Line 206-211
  ```rust
  pub async fn add_session(&self, session: Session) {
      let key = Self::session_key(&session.get_id());
      let value = serde_json::to_string(&session).unwrap();
      let ttl = get_session_ttl();
      self.provider.set_ex(&key, &value, ttl).await.unwrap();
  }
  ```

- **Update session**: Line 235-239
  ```rust
  pub async fn update_session(&self, id: String, session: &mut Session) {
      let key = Self::session_key(&id);
      let value = serde_json::to_string(&session).unwrap();
      let ttl = get_session_ttl();
      self.provider.set_ex(&key, &value, ttl).await.unwrap();
  }
  ```

**SessionProvider Methods** ([kionas/src/session/mod.rs](kionas/src/session/mod.rs) lines 55-66):
```rust
pub async fn set_ex(&self, key: &str, value: &str, seconds: u64) -> redis::RedisResult<()> {
    let mut con = self.client.get_multiplexed_async_connection().await?;
    con.set_ex(key, value, seconds).await
}
```

### Automatic Expiry
✅ **Sessions automatically expire after TTL** — Redis handles expiration server-side, no explicit cleanup job needed for individual sessions.

### Manual Cleanup
Sessions can be explicitly removed via:
- `remove_session(id)` → Calls `del()` to delete key immediately
- Used for logout scenarios

---

## 5. Session Lifecycle: Create, Retrieve, Update, Delete

### Creation
**Primary Entry Point**: [server/src/services/warehouse_auth_service.rs](server/src/services/warehouse_auth_service.rs) lines 190-202
```rust
// Create new session from authenticated user
let mut session = self.new_session(
    token.clone(),
    user.username.clone(),
    "worker1".to_string(),
    remote_addr.clone(),
).await;

session.authenticate(token.clone());
let session_id = session.get_id();
let _ = self.session_manager.add_session(session).await;  // ← Persist to Redis with TTL
```

**Session Constructor** ([server/src/session.rs](server/src/session.rs) lines 47-57):
```rust
pub fn new(
    id: String,
    auth_token: String,
    role: String,
    warehouse_name: String,
    pool_members: Vec<String>,
    remote_addr: String,
) -> Self { ... }
```

### Retrieval
**By Session ID** ([server/src/session.rs](server/src/session.rs) lines 214-217):
```rust
pub async fn get_session(&self, id: String) -> Option<Session> {
    let key = Self::session_key(&id);
    let value = self.provider.get(&key).await.unwrap();
    serde_json::from_str::<Session>(&value).ok()
}
```

**Retrieval Points in Code**:
- [server/src/warehouse/state.rs](server/src/warehouse/state.rs) line 207
- [server/src/statement_handler/utility/use_warehouse.rs](server/src/statement_handler/utility/use_warehouse.rs) line 31
- [server/src/statement_handler/dml/insert.rs](server/src/statement_handler/dml/insert.rs) line 339
- [server/src/statement_handler/utility/rbac.rs](server/src/statement_handler/utility/rbac.rs) line 122
- [server/src/services/warehouse_service_server.rs](server/src/services/warehouse_service_server.rs) line 161
- [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs) line 392
- [server/src/services/request_context.rs](server/src/services/request_context.rs) line 40

**By Auth Token** ([server/src/session.rs](server/src/session.rs) lines 243-268):
```rust
pub async fn get_token_session(&self, token: String) -> Option<Session> {
    let key_pattern = Self::session_key_pattern();  // kionas:session:*
    let keys = self.provider.keys(&key_pattern).await?;
    
    for key in keys {
        let value = self.provider.get(&key).await?;
        if let Ok(session) = serde_json::from_str::<Session>(&value) {
            if session.get_auth_token() == token {
                return Some(session);
            }
        }
    }
    None
}
```

### Update
**Session Updates** ([server/src/session.rs](server/src/session.rs) lines 235-239):
```rust
pub async fn update_session(&self, id: String, session: &mut Session) {
    let key = Self::session_key(&id);
    let value = serde_json::to_string(&session).unwrap();
    let ttl = get_session_ttl();
    self.provider.set_ex(&key, &value, ttl).await.unwrap();
}
```

**Usage Example**: Warehouse update ([server/src/statement_handler/utility/use_warehouse.rs](server/src/statement_handler/utility/use_warehouse.rs) line 56)
```rust
.update_session(session_id.to_string(), &mut s)
```

### Deletion
**Manual Removal** ([server/src/session.rs](server/src/session.rs) lines 221-224):
```rust
#[allow(dead_code)]
pub async fn remove_session(&self, id: String) {
    let key = Self::session_key(&id);
    self.provider.del(&key).await.unwrap();
}
```

**Currently marked `#[allow(dead_code)]` — not actively used; relies on TTL expiry instead**

---

## 6. List All Sessions
**Method** ([server/src/session.rs](server/src/session.rs) lines 270-290):
```rust
pub async fn list_sessions(&self) -> Vec<Session> {
    let key_pattern = Self::session_key_pattern();  // kionas:session:*
    let keys = self.provider.keys(&key_pattern).await?;
    
    for key in keys {
        let value = self.provider.get(&key).await?;
        if let Ok(session) = serde_json::from_str::<Session>(&value) {
            sessions.push(session);
        }
    }
    sessions
}
```

**Used by**: [server/src/janitor/mod.rs](server/src/janitor/mod.rs) for publishing dashboard snapshots

---

## 7. Error Handling & Logging

**Connection Failures**: Logged with context
```
[WARN][2026-03-28 01:11:26][server::session] 
  failed to read session key 'kionas:session:e11fa5c1-1e30-a685-b8df-c2833a2abeeb' 
  from redis: Connection refused (os error 111)
```

**Graceful Degradation**:
- `list_sessions()` returns empty vec on Redis error
- `get_token_session()` returns `None` and logs warning
- Deserialization errors logged per key, processing continues

---

## 8. Storage Cleanup Roadmap Status

**Current State**: ✅ Sessions have TTL set via SETEX, no manual cleanup needed  
**Issue**: No dashboard cache expiry before Phase 4D sprint changes

**Roadmap**: [roadmaps/storage_cleanup.md](roadmaps/storage_cleanup.md)
- **Phase 1** (Complete): ✅ Session TTL via SETEX (implemented)
- **Phase 2** (Planned): Query artifact cleanup (MinIO objects)
- **Phase 3** (Planned): S3 lifecycle policies

---

## 9. Summary Table

| Aspect | Details |
|--------|---------|
| **Key Prefix** | `kionas:session` |
| **Full Key Format** | `kionas:session:{uuid}` |
| **Serialization** | JSON (serde_json) |
| **Default TTL** | 7 days (604,800 sec) |
| **TTL Method** | Redis SETEX (atomic) |
| **TTL Override** | `KIONAS_SESSION_TTL_SECONDS` env var |
| **Metadata Stored** | 9 fields: id, auth_token, is_authenticated, role, warehouse_name, pool_members, remote_addr, last_active, use_database |
| **Creation Flow** | Auth → `new_session()` → `add_session()` → Redis SETEX |
| **Lookup Methods** | By ID, by auth_token, list all (via pattern `kionas:session:*`) |
| **Update Pattern** | Fetch → Modify in-memory → `update_session()` → Redis SETEX (refreshes TTL) |
| **Deletion** | Manual: `remove_session()` (unused); Auto: Redis TTL expiry |
| **Cleanup Job** | None needed; Redis handles expiry automatically |

---

## Files Reference

- **Session struct & core methods**: [server/src/session.rs](server/src/session.rs)
- **SessionProvider (Redis client wrapper)**: [kionas/src/session/mod.rs](kionas/src/session/mod.rs)
- **Constants (TTL defaults)**: [kionas/src/constants.rs](kionas/src/constants.rs)
- **Auth service (creation)**: [server/src/services/warehouse_auth_service.rs](server/src/services/warehouse_auth_service.rs)
- **Dashboard publishing**: [server/src/janitor/mod.rs](server/src/janitor/mod.rs)
- **Cleanup roadmap**: [roadmaps/storage_cleanup.md](roadmaps/storage_cleanup.md)
