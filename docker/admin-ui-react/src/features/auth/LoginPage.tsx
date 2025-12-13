import { useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useAuth } from "@/hooks/useAuth";
import { useTheme } from "@/hooks/useTheme";
import { Eye, EyeOff, LogIn, Sun, Moon, AlertCircle } from "lucide-react";
import { cn } from "@/lib/utils";

export function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, isLoading, error } = useAuth();
  const { effectiveTheme, toggleTheme } = useTheme();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [shake, setShake] = useState(false);

  // Trigger shake animation when error occurs
  useEffect(() => {
    if (error) {
      setShake(true);
      const timer = setTimeout(() => setShake(false), 500);
      return () => clearTimeout(timer);
    }
  }, [error]);

  const from =
    (location.state as { from?: { pathname: string } })?.from?.pathname ||
    "/dashboard";

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    login(
      { username, password },
      {
        onSuccess: () => {
          navigate(from, { replace: true });
        },
      }
    );
  };

  return (
    <div className="min-h-screen flex flex-col bg-gray-50 dark:bg-gray-900">
      {/* Theme toggle in corner */}
      <div className="absolute top-4 right-4">
        <button
          onClick={toggleTheme}
          className="flex items-center justify-center w-10 h-10 text-gray-500 transition-colors bg-white border border-gray-200 rounded-full hover:text-gray-900 hover:bg-gray-100 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white"
        >
          {effectiveTheme === "dark" ? (
            <Sun className="w-5 h-5" />
          ) : (
            <Moon className="w-5 h-5" />
          )}
        </button>
      </div>

      <div className="flex-1 flex items-center justify-center p-4">
        <div className="w-full max-w-md">
          {/* Logo/Header */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-[var(--color-brand-500)] to-[var(--color-brand-600)] mb-4">
              <span className="text-white font-bold text-2xl">S</span>
            </div>
            <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">
              Welcome to SAGE
            </h1>
            <p className="text-gray-500 dark:text-gray-400 mt-2">
              Study Analytics Generative Engine
            </p>
          </div>

          {/* Login Card */}
          <div className={cn("card", shake && "animate-shake")}>
            <div className="card-header">
              <h3>Sign in to your account</h3>
              <p>Enter your credentials to access the admin panel</p>
            </div>
            <div className="card-body">
              <form onSubmit={handleSubmit} className="space-y-5">
                {/* Error Message */}
                {error && (
                  <div className="flex items-center gap-3 p-4 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800" role="alert">
                    <AlertCircle className="flex-shrink-0 w-5 h-5 text-red-500 dark:text-red-400" />
                    <div className="flex-1">
                      <p className="text-sm font-medium text-red-800 dark:text-red-300">
                        Login Failed
                      </p>
                      <p className="text-sm text-red-700 dark:text-red-400 mt-0.5">
                        {(error as Error).message || "Invalid username or password. Please try again."}
                      </p>
                    </div>
                  </div>
                )}

                {/* Username */}
                <div>
                  <label
                    htmlFor="username"
                    className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2"
                  >
                    Username
                  </label>
                  <input
                    id="username"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="Enter your username"
                    required
                    autoComplete="username"
                    disabled={isLoading}
                    className="input"
                  />
                </div>

                {/* Password */}
                <div>
                  <label
                    htmlFor="password"
                    className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2"
                  >
                    Password
                  </label>
                  <div className="relative">
                    <input
                      id="password"
                      type={showPassword ? "text" : "password"}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      placeholder="Enter your password"
                      required
                      autoComplete="current-password"
                      disabled={isLoading}
                      className="input pr-10"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                    >
                      {showPassword ? (
                        <EyeOff className="w-5 h-5" />
                      ) : (
                        <Eye className="w-5 h-5" />
                      )}
                    </button>
                  </div>
                </div>

                {/* Submit Button */}
                <button
                  type="submit"
                  className={cn(
                    "btn btn-primary btn-md w-full",
                    isLoading && "opacity-70 cursor-not-allowed"
                  )}
                  disabled={isLoading || !username || !password}
                >
                  {isLoading ? (
                    <span className="spinner spinner-sm" />
                  ) : (
                    <>
                      <LogIn className="w-5 h-5" />
                      <span>Sign In</span>
                    </>
                  )}
                </button>
              </form>
            </div>
          </div>

          {/* Footer */}
          <p className="text-center text-gray-400 dark:text-gray-500 text-sm mt-6">
            SAGE Admin Panel v1.0
          </p>
        </div>
      </div>
    </div>
  );
}
