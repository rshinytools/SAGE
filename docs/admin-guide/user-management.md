# User Management

Manage users, roles, and permissions in SAGE.

---

## Overview

SAGE uses role-based access control (RBAC) to manage who can access the system and what they can do.

---

## User Roles

| Role | Permissions |
|------|-------------|
| **viewer** | Query data, view results |
| **analyst** | Query data, export data |
| **admin** | Full access, user management |

---

## Managing Users

### View Users

Access the Admin Dashboard at `http://localhost/admin`

```
┌──────────────────────────────────────────────┐
│ User Management                              │
├──────────────────────────────────────────────┤
│ Username    │ Role    │ Status  │ Actions   │
├─────────────┼─────────┼─────────┼───────────┤
│ admin       │ admin   │ Active  │ Edit      │
│ analyst1    │ analyst │ Active  │ Edit/Del  │
│ viewer1     │ viewer  │ Active  │ Edit/Del  │
└──────────────────────────────────────────────┘
```

### Create User

```bash
# Via API
curl -X POST http://localhost:8002/api/v1/users \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "new_user",
    "email": "user@example.com",
    "role": "analyst",
    "password": "secure_password"
  }'
```

### Update User

```bash
curl -X PUT http://localhost:8002/api/v1/users/user_id \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "role": "admin"
  }'
```

### Deactivate User

```bash
curl -X DELETE http://localhost:8002/api/v1/users/user_id \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

## Authentication

### JWT Tokens

SAGE uses JWT tokens for authentication:

```bash
# Login
curl -X POST http://localhost:8002/api/v1/auth/login \
  -d "username=admin&password=secret"

# Response
{
  "access_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 86400
}
```

### Token Expiration

| Setting | Default |
|---------|---------|
| Access token expiry | 24 hours |
| Refresh window | Last 4 hours |

---

## Password Policy

Configure password requirements:

```env
PASSWORD_MIN_LENGTH=12
PASSWORD_REQUIRE_UPPERCASE=true
PASSWORD_REQUIRE_LOWERCASE=true
PASSWORD_REQUIRE_NUMBERS=true
PASSWORD_REQUIRE_SPECIAL=true
```

---

## Session Management

### Active Sessions

View active user sessions:

```bash
curl http://localhost:8002/api/v1/sessions \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Terminate Session

```bash
curl -X DELETE http://localhost:8002/api/v1/sessions/session_id \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

## Audit

All user management actions are logged:

- User creation
- Role changes
- Password resets
- Account deactivation

See [Audit Trail](../compliance/audit-trail.md) for details.

---

## Best Practices

1. **Principle of Least Privilege**: Assign minimum required roles
2. **Regular Review**: Audit user access monthly
3. **Strong Passwords**: Enforce password policy
4. **Prompt Deactivation**: Remove access when no longer needed

---

## Next Steps

- [Data Loading](data-loading.md)
- [Access Controls](../compliance/access-controls.md)
