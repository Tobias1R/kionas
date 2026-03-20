
-- RBAC scaffold tables
CREATE TABLE IF NOT EXISTS users_rbac (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    can_login BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS groups_rbac (
    id BIGSERIAL PRIMARY KEY,
    group_name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS roles_rbac (
    id BIGSERIAL PRIMARY KEY,
    role_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS group_memberships_rbac (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    group_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, group_id),
    FOREIGN KEY (user_id) REFERENCES users_rbac(id) ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES groups_rbac(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS role_bindings_rbac (
    id BIGSERIAL PRIMARY KEY,
    principal_type VARCHAR(32) NOT NULL,
    principal_id BIGINT NOT NULL,
    role_id BIGINT NOT NULL,
    granted_by VARCHAR(255) NOT NULL DEFAULT 'kionas',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (principal_type, principal_id, role_id),
    FOREIGN KEY (role_id) REFERENCES roles_rbac(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_users_rbac_username ON users_rbac(username);
CREATE INDEX IF NOT EXISTS idx_groups_rbac_name ON groups_rbac(group_name);
CREATE INDEX IF NOT EXISTS idx_roles_rbac_name ON roles_rbac(role_name);
CREATE INDEX IF NOT EXISTS idx_role_bindings_rbac_principal ON role_bindings_rbac(principal_type, principal_id);
CREATE INDEX IF NOT EXISTS idx_role_bindings_rbac_role_id ON role_bindings_rbac(role_id);

-- Bootstrap reserved kionas principal and ADMIN role.
INSERT INTO users_rbac (username, can_login)
VALUES ('kionas', TRUE)
ON CONFLICT (username) DO NOTHING;

INSERT INTO roles_rbac (role_name, description)
VALUES ('ADMIN', 'Bootstrap administrator role')
ON CONFLICT (role_name) DO NOTHING;

INSERT INTO role_bindings_rbac (principal_type, principal_id, role_id, granted_by)
SELECT 'user', u.id, r.id, 'bootstrap'
FROM users_rbac u
CROSS JOIN roles_rbac r
WHERE u.username = 'kionas' AND r.role_name = 'ADMIN'
ON CONFLICT (principal_type, principal_id, role_id) DO NOTHING;
