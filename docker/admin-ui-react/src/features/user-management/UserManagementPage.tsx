import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Edit, Trash2, Key, UserCheck, UserX, Users } from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { usersApi } from "@/api/users";
import { formatDateTime } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { UserAccount, CreateUserRequest, UpdateUserRequest } from "@/types/api";

export function UserManagementPage() {
  const [isAddModalOpen, setIsAddModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<UserAccount | null>(null);
  const queryClient = useQueryClient();

  const { data: users, isLoading } = useQuery({
    queryKey: ["users"],
    queryFn: () => usersApi.getUsers(),
  });

  const { data: permissions } = useQuery({
    queryKey: ["permissions"],
    queryFn: usersApi.getPermissions,
  });

  const createMutation = useMutation({
    mutationFn: usersApi.createUser,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      setIsAddModalOpen(false);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateUserRequest }) =>
      usersApi.updateUser(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      setEditingUser(null);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: usersApi.deleteUser,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
    },
  });

  const toggleStatusMutation = useMutation({
    mutationFn: ({ id, isActive }: { id: string; isActive: boolean }) =>
      usersApi.toggleUserStatus(id, isActive),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
    },
  });

  const resetPasswordMutation = useMutation({
    mutationFn: usersApi.resetPassword,
    onSuccess: (data) => {
      alert(`Temporary password: ${data.temporary_password}`);
    },
  });

  const columns: ColumnDef<UserAccount>[] = [
    {
      accessorKey: "username",
      header: "Username",
      cell: ({ row }) => (
        <span className="font-medium">{row.original.username}</span>
      ),
    },
    {
      accessorKey: "email",
      header: "Email",
    },
    {
      accessorKey: "role",
      header: "Role",
      cell: ({ row }) => (
        <StatusBadge
          variant={
            row.original.role === "admin"
              ? "destructive"
              : row.original.role === "user"
              ? "primary"
              : "default"
          }
        >
          {row.original.role}
        </StatusBadge>
      ),
    },
    {
      accessorKey: "is_active",
      header: "Status",
      cell: ({ row }) => (
        <StatusBadge variant={row.original.is_active ? "success" : "default"}>
          {row.original.is_active ? "Active" : "Inactive"}
        </StatusBadge>
      ),
    },
    {
      accessorKey: "last_login",
      header: "Last Login",
      cell: ({ row }) =>
        row.original.last_login
          ? formatDateTime(row.original.last_login)
          : "Never",
    },
    {
      accessorKey: "created_at",
      header: "Created",
      cell: ({ row }) => formatDateTime(row.original.created_at),
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <div className="flex items-center gap-1">
          <button
            className="p-1 hover:text-[var(--primary)]"
            onClick={() => setEditingUser(row.original)}
            title="Edit"
          >
            <Edit className="w-4 h-4" />
          </button>
          <button
            className="p-1 hover:text-[var(--warning)]"
            onClick={() => {
              if (confirm(`Reset password for "${row.original.username}"?`)) {
                resetPasswordMutation.mutate(row.original.id);
              }
            }}
            title="Reset Password"
          >
            <Key className="w-4 h-4" />
          </button>
          <button
            className="p-1 hover:text-[var(--primary)]"
            onClick={() =>
              toggleStatusMutation.mutate({
                id: row.original.id,
                isActive: !row.original.is_active,
              })
            }
            title={row.original.is_active ? "Deactivate" : "Activate"}
          >
            {row.original.is_active ? (
              <UserX className="w-4 h-4" />
            ) : (
              <UserCheck className="w-4 h-4" />
            )}
          </button>
          <button
            className="p-1 hover:text-[var(--destructive)]"
            onClick={() => {
              if (confirm(`Delete user "${row.original.username}"?`)) {
                deleteMutation.mutate(row.original.id);
              }
            }}
            title="Delete"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            User Management
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Manage user accounts and permissions
          </p>
        </div>
        <button
          className="btn btn-primary btn-md"
          onClick={() => setIsAddModalOpen(true)}
        >
          <Plus className="w-4 h-4" />
          Add User
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="wp-box p-4 flex items-center gap-3">
          <Users className="w-8 h-8 text-[var(--primary)]" />
          <div>
            <div className="text-2xl font-bold">{users?.items?.length || 0}</div>
            <div className="text-sm text-[var(--muted)]">Total Users</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <UserCheck className="w-8 h-8 text-[var(--success)]" />
          <div>
            <div className="text-2xl font-bold">
              {users?.items?.filter((u) => u.is_active).length || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Active Users</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <UserX className="w-8 h-8 text-[var(--muted)]" />
          <div>
            <div className="text-2xl font-bold">
              {users?.items?.filter((u) => !u.is_active).length || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Inactive Users</div>
          </div>
        </div>
      </div>

      {/* Users Table */}
      <WPBox title="Users">
        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
          </div>
        ) : (
          <DataTable
            columns={columns}
            data={users?.items || []}
            searchColumn="username"
            searchPlaceholder="Search users..."
          />
        )}
      </WPBox>

      {/* Add/Edit Modal */}
      {(isAddModalOpen || editingUser) && (
        <UserModal
          user={editingUser}
          permissions={permissions || []}
          onSave={(data) => {
            if (editingUser) {
              updateMutation.mutate({ id: editingUser.id, data });
            } else {
              createMutation.mutate(data as CreateUserRequest);
            }
          }}
          onClose={() => {
            setIsAddModalOpen(false);
            setEditingUser(null);
          }}
          isLoading={createMutation.isPending || updateMutation.isPending}
        />
      )}
    </div>
  );
}

interface UserModalProps {
  user: UserAccount | null;
  permissions: string[];
  onSave: (data: CreateUserRequest | UpdateUserRequest) => void;
  onClose: () => void;
  isLoading: boolean;
}

function UserModal({
  user,
  permissions,
  onSave,
  onClose,
  isLoading,
}: UserModalProps) {
  const [formData, setFormData] = useState({
    username: user?.username || "",
    email: user?.email || "",
    password: "",
    role: user?.role || "user",
    permissions: user?.permissions || [],
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (user) {
      onSave({
        email: formData.email,
        role: formData.role as "admin" | "user" | "viewer",
        permissions: formData.permissions,
      });
    } else {
      onSave({
        username: formData.username,
        email: formData.email,
        password: formData.password,
        role: formData.role as "admin" | "user" | "viewer",
        permissions: formData.permissions,
      });
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="wp-box w-full max-w-lg">
        <div className="wp-box-header flex items-center justify-between">
          <h3>{user ? "Edit User" : "Add New User"}</h3>
          <button onClick={onClose} className="text-[var(--muted)] hover:text-[var(--foreground)]">
            &times;
          </button>
        </div>
        <div className="wp-box-body">
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1">Username</label>
              <input
                type="text"
                value={formData.username}
                onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                required
                disabled={!!user}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Email</label>
              <input
                type="email"
                value={formData.email}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                required
              />
            </div>
            {!user && (
              <div>
                <label className="block text-sm font-medium mb-1">Password</label>
                <input
                  type="password"
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  required
                  minLength={8}
                />
              </div>
            )}
            <div>
              <label className="block text-sm font-medium mb-1">Role</label>
              <select
                value={formData.role}
                onChange={(e) => setFormData({ ...formData, role: e.target.value as "admin" | "user" | "viewer" })}
              >
                <option value="viewer">Viewer</option>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Permissions</label>
              <div className="space-y-2 max-h-40 overflow-y-auto border border-[var(--border)] rounded p-2">
                {permissions.map((perm) => (
                  <label key={perm} className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={formData.permissions.includes(perm)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setFormData({
                            ...formData,
                            permissions: [...formData.permissions, perm],
                          });
                        } else {
                          setFormData({
                            ...formData,
                            permissions: formData.permissions.filter((p) => p !== perm),
                          });
                        }
                      }}
                    />
                    <span className="text-sm">{perm}</span>
                  </label>
                ))}
              </div>
            </div>
            <div className="flex justify-end gap-3 pt-4">
              <button type="button" className="btn btn-secondary btn-md" onClick={onClose}>
                Cancel
              </button>
              <button type="submit" className="btn btn-primary btn-md" disabled={isLoading}>
                {isLoading ? "Saving..." : "Save"}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
