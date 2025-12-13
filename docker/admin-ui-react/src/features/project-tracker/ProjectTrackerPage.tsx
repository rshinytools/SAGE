import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Plus,
  Edit,
  Trash2,
  CheckCircle,
  Clock,
  AlertTriangle,
  PlayCircle,
  ChevronDown,
  ChevronRight,
  GripVertical,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { StatusBadge } from "@/components/common/StatusBadge";
import { HealthCircle } from "@/components/common/HealthCircle";
import { trackerApi } from "@/api/tracker";
import type { TrackerPhase, TrackerTask, CreateTaskRequest, UpdateTaskRequest } from "@/types/api";

type Tab = "board" | "list" | "timeline";

const statusColors = {
  pending: "default",
  in_progress: "primary",
  completed: "success",
  blocked: "destructive",
} as const;

const priorityColors = {
  low: "default",
  medium: "primary",
  high: "warning",
  critical: "destructive",
} as const;

export function ProjectTrackerPage() {
  const [activeTab, setActiveTab] = useState<Tab>("board");
  const [expandedPhases, setExpandedPhases] = useState<Set<number>>(new Set());
  const [isAddTaskModalOpen, setIsAddTaskModalOpen] = useState(false);
  const [selectedPhase, setSelectedPhase] = useState<number | null>(null);
  const [editingTask, setEditingTask] = useState<TrackerTask | null>(null);
  const queryClient = useQueryClient();

  const { data: phases, isLoading } = useQuery({
    queryKey: ["trackerPhases"],
    queryFn: trackerApi.getPhases,
  });

  const { data: progress } = useQuery({
    queryKey: ["trackerProgress"],
    queryFn: trackerApi.getProgress,
  });

  const createTaskMutation = useMutation({
    mutationFn: trackerApi.createTask,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["trackerPhases"] });
      queryClient.invalidateQueries({ queryKey: ["trackerProgress"] });
      setIsAddTaskModalOpen(false);
    },
  });

  const updateTaskMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: UpdateTaskRequest }) =>
      trackerApi.updateTask(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["trackerPhases"] });
      queryClient.invalidateQueries({ queryKey: ["trackerProgress"] });
      setEditingTask(null);
    },
  });

  const deleteTaskMutation = useMutation({
    mutationFn: trackerApi.deleteTask,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["trackerPhases"] });
      queryClient.invalidateQueries({ queryKey: ["trackerProgress"] });
    },
  });

  const togglePhase = (phaseId: number) => {
    const newExpanded = new Set(expandedPhases);
    if (newExpanded.has(phaseId)) {
      newExpanded.delete(phaseId);
    } else {
      newExpanded.add(phaseId);
    }
    setExpandedPhases(newExpanded);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "completed":
        return <CheckCircle className="w-4 h-4 text-[var(--success)]" />;
      case "in_progress":
        return <PlayCircle className="w-4 h-4 text-[var(--primary)]" />;
      case "blocked":
        return <AlertTriangle className="w-4 h-4 text-[var(--destructive)]" />;
      default:
        return <Clock className="w-4 h-4 text-[var(--muted)]" />;
    }
  };

  const tabs = [
    { id: "board", label: "Board View" },
    { id: "list", label: "List View" },
    { id: "timeline", label: "Timeline" },
  ] as const;

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Project Tracker
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Track implementation progress and tasks
          </p>
        </div>
        <button
          className="btn btn-primary btn-md"
          onClick={() => setIsAddTaskModalOpen(true)}
        >
          <Plus className="w-4 h-4" />
          Add Task
        </button>
      </div>

      {/* Progress Overview */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-5">
        <WPBox className="lg:col-span-1">
          <div className="flex items-center justify-center py-4">
            <HealthCircle
              percentage={progress?.overall_progress || 0}
              size={120}
              label="Overall Progress"
            />
          </div>
        </WPBox>
        <div className="lg:col-span-3 grid grid-cols-4 gap-4">
          <div className="wp-box p-4 flex items-center gap-3">
            <Clock className="w-8 h-8 text-[var(--muted)]" />
            <div>
              <div className="text-2xl font-bold">
                {(progress?.total_tasks || 0) -
                  (progress?.completed_tasks || 0) -
                  (progress?.in_progress_tasks || 0) -
                  (progress?.blocked_tasks || 0)}
              </div>
              <div className="text-sm text-[var(--muted)]">Pending</div>
            </div>
          </div>
          <div className="wp-box p-4 flex items-center gap-3">
            <PlayCircle className="w-8 h-8 text-[var(--primary)]" />
            <div>
              <div className="text-2xl font-bold">
                {progress?.in_progress_tasks || 0}
              </div>
              <div className="text-sm text-[var(--muted)]">In Progress</div>
            </div>
          </div>
          <div className="wp-box p-4 flex items-center gap-3">
            <CheckCircle className="w-8 h-8 text-[var(--success)]" />
            <div>
              <div className="text-2xl font-bold">
                {progress?.completed_tasks || 0}
              </div>
              <div className="text-sm text-[var(--muted)]">Completed</div>
            </div>
          </div>
          <div className="wp-box p-4 flex items-center gap-3">
            <AlertTriangle className="w-8 h-8 text-[var(--destructive)]" />
            <div>
              <div className="text-2xl font-bold">
                {progress?.blocked_tasks || 0}
              </div>
              <div className="text-sm text-[var(--muted)]">Blocked</div>
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="tabs-list">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`tab-trigger ${activeTab === tab.id ? "active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      {isLoading ? (
        <div className="flex items-center justify-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
        </div>
      ) : activeTab === "list" ? (
        <div className="space-y-4">
          {phases?.map((phase) => (
            <WPBox key={phase.id}>
              <button
                className="w-full flex items-center justify-between p-3 hover:bg-[var(--background)]"
                onClick={() => togglePhase(phase.id)}
              >
                <div className="flex items-center gap-3">
                  {expandedPhases.has(phase.id) ? (
                    <ChevronDown className="w-5 h-5" />
                  ) : (
                    <ChevronRight className="w-5 h-5" />
                  )}
                  <span className="font-medium">{phase.name}</span>
                  <span className="text-sm text-[var(--muted)]">
                    ({phase.tasks?.length || 0} tasks)
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <div className="w-32 h-2 bg-[var(--border)] rounded-full overflow-hidden">
                    <div
                      className="h-full bg-[var(--primary)] rounded-full"
                      style={{ width: `${phase.progress}%` }}
                    />
                  </div>
                  <span className="text-sm text-[var(--muted)] w-12">
                    {phase.progress}%
                  </span>
                </div>
              </button>
              {expandedPhases.has(phase.id) && (
                <div className="border-t border-[var(--border)]">
                  {phase.tasks?.map((task) => (
                    <div
                      key={task.id}
                      className="flex items-center gap-3 p-3 border-b border-[var(--border)] last:border-b-0 hover:bg-[var(--background)]"
                    >
                      <GripVertical className="w-4 h-4 text-[var(--muted)]" />
                      {getStatusIcon(task.status)}
                      <div className="flex-1">
                        <div className="font-medium">{task.name}</div>
                        {task.description && (
                          <div className="text-sm text-[var(--muted)] line-clamp-1">
                            {task.description}
                          </div>
                        )}
                      </div>
                      <StatusBadge variant={priorityColors[task.priority]}>
                        {task.priority}
                      </StatusBadge>
                      <StatusBadge variant={statusColors[task.status]}>
                        {task.status.replace("_", " ")}
                      </StatusBadge>
                      <div className="flex items-center gap-1">
                        <button
                          className="p-1 hover:text-[var(--primary)]"
                          onClick={() => setEditingTask(task)}
                        >
                          <Edit className="w-4 h-4" />
                        </button>
                        <button
                          className="p-1 hover:text-[var(--destructive)]"
                          onClick={() => {
                            if (confirm(`Delete task "${task.name}"?`)) {
                              deleteTaskMutation.mutate(task.id);
                            }
                          }}
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                  <button
                    className="w-full p-2 text-sm text-[var(--primary)] hover:bg-[var(--background)]"
                    onClick={() => {
                      setSelectedPhase(phase.id);
                      setIsAddTaskModalOpen(true);
                    }}
                  >
                    <Plus className="w-4 h-4 inline mr-1" />
                    Add Task
                  </button>
                </div>
              )}
            </WPBox>
          ))}
        </div>
      ) : activeTab === "board" ? (
        <div className="grid grid-cols-4 gap-4">
          {["pending", "in_progress", "completed", "blocked"].map((status) => (
            <div key={status} className="wp-box min-h-[400px]">
              <div className="wp-box-header flex items-center justify-between">
                <h3 className="capitalize">{status.replace("_", " ")}</h3>
                <span className="text-sm text-[var(--muted)]">
                  {phases?.reduce(
                    (acc, phase) =>
                      acc +
                      (phase.tasks?.filter((t) => t.status === status).length || 0),
                    0
                  )}
                </span>
              </div>
              <div className="wp-box-body space-y-2">
                {phases?.flatMap(
                  (phase) =>
                    phase.tasks
                      ?.filter((t) => t.status === status)
                      .map((task) => (
                        <div
                          key={task.id}
                          className="p-3 bg-[var(--background)] rounded border border-[var(--border)] cursor-pointer hover:border-[var(--primary)]"
                          onClick={() => setEditingTask(task)}
                        >
                          <div className="font-medium text-sm">{task.name}</div>
                          <div className="flex items-center justify-between mt-2">
                            <StatusBadge variant={priorityColors[task.priority]}>
                              {task.priority}
                            </StatusBadge>
                            <span className="text-xs text-[var(--muted)]">
                              {phases?.find((p) => p.id === task.phase_id)?.name}
                            </span>
                          </div>
                        </div>
                      )) || []
                )}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <WPBox title="Timeline View">
          <div className="text-center py-8 text-[var(--muted)]">
            Timeline view coming soon
          </div>
        </WPBox>
      )}

      {/* Add/Edit Task Modal */}
      {(isAddTaskModalOpen || editingTask) && (
        <TaskModal
          task={editingTask}
          phases={phases || []}
          selectedPhaseId={selectedPhase}
          onSave={(data) => {
            if (editingTask) {
              updateTaskMutation.mutate({ id: editingTask.id, data });
            } else {
              createTaskMutation.mutate(data as CreateTaskRequest);
            }
          }}
          onClose={() => {
            setIsAddTaskModalOpen(false);
            setEditingTask(null);
            setSelectedPhase(null);
          }}
          isLoading={createTaskMutation.isPending || updateTaskMutation.isPending}
        />
      )}
    </div>
  );
}

interface TaskModalProps {
  task: TrackerTask | null;
  phases: TrackerPhase[];
  selectedPhaseId: number | null;
  onSave: (data: CreateTaskRequest | UpdateTaskRequest) => void;
  onClose: () => void;
  isLoading: boolean;
}

function TaskModal({
  task,
  phases,
  selectedPhaseId,
  onSave,
  onClose,
  isLoading,
}: TaskModalProps) {
  const [formData, setFormData] = useState({
    phase_id: task?.phase_id || selectedPhaseId || phases[0]?.id || 0,
    name: task?.name || "",
    description: task?.description || "",
    status: task?.status || "pending",
    priority: task?.priority || "medium",
    assigned_to: task?.assigned_to || "",
    due_date: task?.due_date || "",
    notes: task?.notes || "",
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (task) {
      onSave({
        name: formData.name,
        description: formData.description || undefined,
        status: formData.status as TrackerTask["status"],
        priority: formData.priority as TrackerTask["priority"],
        assigned_to: formData.assigned_to || undefined,
        due_date: formData.due_date || undefined,
        notes: formData.notes || undefined,
      });
    } else {
      onSave({
        phase_id: formData.phase_id,
        name: formData.name,
        description: formData.description || undefined,
        priority: formData.priority as TrackerTask["priority"],
        assigned_to: formData.assigned_to || undefined,
        due_date: formData.due_date || undefined,
      });
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="wp-box w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="wp-box-header flex items-center justify-between">
          <h3>{task ? "Edit Task" : "Add New Task"}</h3>
          <button onClick={onClose} className="text-[var(--muted)] hover:text-[var(--foreground)]">
            &times;
          </button>
        </div>
        <div className="wp-box-body">
          <form onSubmit={handleSubmit} className="space-y-4">
            {!task && (
              <div>
                <label className="block text-sm font-medium mb-1">Phase</label>
                <select
                  value={formData.phase_id}
                  onChange={(e) =>
                    setFormData({ ...formData, phase_id: Number(e.target.value) })
                  }
                >
                  {phases.map((phase) => (
                    <option key={phase.id} value={phase.id}>
                      {phase.name}
                    </option>
                  ))}
                </select>
              </div>
            )}
            <div>
              <label className="block text-sm font-medium mb-1">Task Name</label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Description</label>
              <textarea
                value={formData.description}
                onChange={(e) =>
                  setFormData({ ...formData, description: e.target.value })
                }
                rows={2}
              />
            </div>
            {task && (
              <div>
                <label className="block text-sm font-medium mb-1">Status</label>
                <select
                  value={formData.status}
                  onChange={(e) =>
                    setFormData({ ...formData, status: e.target.value as TrackerTask["status"] })
                  }
                >
                  <option value="pending">Pending</option>
                  <option value="in_progress">In Progress</option>
                  <option value="completed">Completed</option>
                  <option value="blocked">Blocked</option>
                </select>
              </div>
            )}
            <div>
              <label className="block text-sm font-medium mb-1">Priority</label>
              <select
                value={formData.priority}
                onChange={(e) =>
                  setFormData({ ...formData, priority: e.target.value as TrackerTask["priority"] })
                }
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="critical">Critical</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Assigned To</label>
              <input
                type="text"
                value={formData.assigned_to}
                onChange={(e) =>
                  setFormData({ ...formData, assigned_to: e.target.value })
                }
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Due Date</label>
              <input
                type="date"
                value={formData.due_date}
                onChange={(e) =>
                  setFormData({ ...formData, due_date: e.target.value })
                }
              />
            </div>
            {task && (
              <div>
                <label className="block text-sm font-medium mb-1">Notes</label>
                <textarea
                  value={formData.notes}
                  onChange={(e) =>
                    setFormData({ ...formData, notes: e.target.value })
                  }
                  rows={2}
                />
              </div>
            )}
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
