import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Edit, Trash2, Key, UserCheck, UserX, Users, X, UserPlus } from "lucide-react";
import * as Dialog from "@radix-ui/react-dialog";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { ConfirmDialog } from "@/components/common/ConfirmDialog";
import { useToast } from "@/components/common/Toast";
import { usersApi } from "@/api/users";
import { formatDateTime } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { UserAccount, CreateUserRequest, UpdateUserRequest } from "@/types/api";

export function UserManagementPage() {
  const [isAddModalOpen, setIsAddModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<UserAccount | null>(null);
  const [userToDelete, setUserToDelete] = useState<UserAccount | null>(null);
  const [userToResetPassword, setUserToResetPassword] = useState<UserAccount | null>(null);
  const queryClient = useQueryClient();
  const toast = useToast();

  const { data: users, isLoading } = useQuery({
    queryKey: ["users"],
    queryFn: () => usersApi.getUsers(),
  });

  const createMutation = useMutation({
    mutationFn: usersApi.createUser,
    onSuccess: (user) => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      setIsAddModalOpen(false);
      toast.success("User Created", `User "${user.username}" has been created successfully.`);
    },
    onError: (error: Error) => {
      toast.error("Failed to Create User", error.message);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateUserRequest }) =>
      usersApi.updateUser(id, data),
    onSuccess: (user) => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      setEditingUser(null);
      toast.success("User Updated", `User "${user.username}" has been updated successfully.`);
    },
    onError: (error: Error) => {
      toast.error("Failed to Update User", error.message);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: ({ id }: { id: string; username: string }) => usersApi.deleteUser(id),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      toast.success("User Deleted", `User "${variables.username}" has been deleted.`);
      setUserToDelete(null);
    },
    onError: (error: Error) => {
      toast.error("Failed to Delete User", error.message);
      setUserToDelete(null);
    },
  });

  const toggleStatusMutation = useMutation({
    mutationFn: ({ id, isActive }: { id: string; isActive: boolean }) =>
      usersApi.toggleUserStatus(id, isActive),
    onSuccess: (user) => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      toast.success(
        user.is_active ? "User Activated" : "User Deactivated",
        `User "${user.username}" has been ${user.is_active ? "activated" : "deactivated"}.`
      );
    },
    onError: (error: Error) => {
      toast.error("Failed to Update User Status", error.message);
    },
  });

  const resetPasswordMutation = useMutation({
    mutationFn: ({ id }: { id: string; username: string }) => usersApi.resetPassword(id),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["users"] });
      toast.success(
        "Password Reset",
        `Temporary password for "${variables.username}": ${data.temporary_password}`
      );
      setUserToResetPassword(null);
    },
    onError: (error: Error) => {
      toast.error("Failed to Reset Password", error.message);
      setUserToResetPassword(null);
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
      accessorKey: "permissions",
      header: "Access Level",
      cell: ({ row }) => {
        const perms = row.original.permissions || [];
        const isFullAdmin = perms.includes("*");
        const isUserAdmin = perms.includes("user_admin");

        return (
          <StatusBadge
            variant={isFullAdmin ? "destructive" : isUserAdmin ? "warning" : "default"}
          >
            {isFullAdmin ? "Full Admin" : isUserAdmin ? "User Admin" : "Chat Only"}
          </StatusBadge>
        );
      },
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
            onClick={() => setUserToResetPassword(row.original)}
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
            onClick={() => setUserToDelete(row.original)}
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
      <UserModal
        open={isAddModalOpen || !!editingUser}
        user={editingUser}
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

      {/* Delete Confirmation Dialog */}
      <ConfirmDialog
        open={!!userToDelete}
        onOpenChange={(open) => !open && setUserToDelete(null)}
        title="Delete User"
        description={`Are you sure you want to delete the user "${userToDelete?.username}"? This action cannot be undone.`}
        confirmLabel="Delete"
        cancelLabel="Cancel"
        variant="danger"
        icon={<Trash2 className="w-5 h-5" />}
        onConfirm={() => {
          if (userToDelete) {
            deleteMutation.mutate({
              id: userToDelete.id,
              username: userToDelete.username,
            });
          }
        }}
        onCancel={() => setUserToDelete(null)}
      />

      {/* Reset Password Confirmation Dialog */}
      <ConfirmDialog
        open={!!userToResetPassword}
        onOpenChange={(open) => !open && setUserToResetPassword(null)}
        title="Reset Password"
        description={`Are you sure you want to reset the password for "${userToResetPassword?.username}"? A temporary password will be generated.`}
        confirmLabel="Reset Password"
        cancelLabel="Cancel"
        variant="warning"
        icon={<Key className="w-5 h-5" />}
        onConfirm={() => {
          if (userToResetPassword) {
            resetPasswordMutation.mutate({
              id: userToResetPassword.id,
              username: userToResetPassword.username,
            });
          }
        }}
        onCancel={() => setUserToResetPassword(null)}
      />
    </div>
  );
}

interface UserModalProps {
  user: UserAccount | null;
  onSave: (data: CreateUserRequest | UpdateUserRequest) => void;
  onClose: () => void;
  isLoading: boolean;
  open: boolean;
}

function UserModal({
  user,
  onSave,
  onClose,
  isLoading,
  open,
}: UserModalProps) {
  const [formData, setFormData] = useState({
    username: user?.username || "",
    email: user?.email || "",
    password: "",
    role: user?.role || "user",
    permissions: user?.permissions || [],
  });

  // Update form data when user prop changes (for edit mode)
  useEffect(() => {
    if (open) {
      setFormData({
        username: user?.username || "",
        email: user?.email || "",
        password: "",
        role: user?.role || "user",
        permissions: user?.permissions || [],
      });
    }
  }, [user, open]);

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

  // Input field styles
  const inputStyles = "w-full px-3 py-2 text-sm bg-gray-50 dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-100 dark:disabled:bg-gray-700 disabled:text-gray-500";
  const selectStyles = "w-full px-3 py-2 text-sm bg-gray-50 dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent";
  const labelStyles = "block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1.5";

  return (
    <Dialog.Root open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0" />
        <Dialog.Content className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95">
          {/* Header */}
          <div className="flex items-center gap-4 px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <div className="flex-shrink-0 w-10 h-10 rounded-full flex items-center justify-center bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400">
              {user ? <Edit className="w-5 h-5" /> : <UserPlus className="w-5 h-5" />}
            </div>
            <div className="flex-1">
              <Dialog.Title className="text-lg font-semibold text-gray-900 dark:text-white">
                {user ? "Edit User" : "Add New User"}
              </Dialog.Title>
              <Dialog.Description className="text-sm text-gray-500 dark:text-gray-400">
                {user ? "Update user details and permissions" : "Create a new user account"}
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button
                className="flex-shrink-0 p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
                aria-label="Close"
              >
                <X className="w-5 h-5" />
              </button>
            </Dialog.Close>
          </div>

          {/* Form */}
          <form id="user-form" onSubmit={handleSubmit} className="px-6 py-4 space-y-4 max-h-[60vh] overflow-y-auto">
            <div>
              <label className={labelStyles}>Username</label>
              <input
                type="text"
                value={formData.username}
                onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                required
                disabled={!!user}
                placeholder="Enter username"
                className={inputStyles}
              />
            </div>

            <div>
              <label className={labelStyles}>Email</label>
              <input
                type="email"
                value={formData.email}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                required
                placeholder="Enter email address"
                className={inputStyles}
              />
            </div>

            {!user && (
              <div>
                <label className={labelStyles}>Password</label>
                <input
                  type="password"
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  required
                  minLength={8}
                  placeholder="Minimum 8 characters"
                  className={inputStyles}
                />
              </div>
            )}

            <div>
              <label className={labelStyles}>Role</label>
              <select
                value={formData.role}
                onChange={(e) => setFormData({ ...formData, role: e.target.value as "admin" | "user" | "viewer" })}
                className={selectStyles}
              >
                <option value="viewer">Viewer</option>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </div>

            <div>
              <label className={labelStyles}>Access Level</label>
              <select
                value={
                  formData.permissions.includes("*") ? "admin" :
                  formData.permissions.includes("user_admin") ? "user_admin" :
                  "chat_only"
                }
                onChange={(e) => {
                  const value = e.target.value;
                  let newPermissions: string[] = [];
                  if (value === "admin") {
                    newPermissions = ["*"];
                  } else if (value === "user_admin") {
                    newPermissions = ["user_admin"];
                  }
                  // chat_only = empty permissions
                  setFormData({ ...formData, permissions: newPermissions });
                }}
                className={selectStyles}
              >
                <option value="chat_only">Chat Only - Access to AI Chat only</option>
                <option value="user_admin">User Admin - User Management + Audit + Chat</option>
                <option value="admin">Full Admin - Complete access to all features</option>
              </select>
              <p className="mt-1.5 text-xs text-gray-500 dark:text-gray-400">
                {formData.permissions.includes("*") && "Full access to all platform features"}
                {formData.permissions.includes("user_admin") && "Can manage users and view audit logs"}
                {formData.permissions.length === 0 && "Can only access the AI Chat feature"}
              </p>
            </div>

          </form>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 dark:border-gray-700">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 dark:focus:ring-offset-gray-900"
            >
              Cancel
            </button>
            <button
              type="submit"
              form="user-form"
              disabled={isLoading}
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-900 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? "Saving..." : user ? "Update User" : "Create User"}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
